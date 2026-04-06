package com.dremio.plugins.hudi.format;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.store.RecordReader;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.easy.EasyFormatPlugin;
import com.dremio.exec.store.dfs.easy.EasyWriter;
import com.dremio.exec.store.iceberg.SupportsFsCreation;
import com.dremio.io.file.FileSystem;
import com.dremio.plugins.hudi.HudiStoragePlugin;
import com.dremio.plugins.hudi.conf.HudiPluginConfig;
import com.dremio.plugins.hudi.read.HudiParquetRecordReader;
import com.dremio.plugins.hudi.write.HudiRecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Dremio FormatPlugin for Apache Hudi.
 *
 * Extends EasyFormatPlugin which provides default implementations for most
 * FormatPlugin interface methods. We override getRecordWriter() to supply
 * our HudiRecordWriter for the write path.
 *
 * Write Path
 * ----------
 * SQL:  INSERT INTO hudi_source.my_table SELECT ...
 *
 *   Dremio planner
 *       -> getRecordWriter(OperatorContext, EasyWriter)
 *           -> HudiRecordWriter (per-thread)
 *               -> converts Arrow RecordBatch to Avro GenericRecord
 *               -> buffers records and flushes to HoodieJavaWriteClient
 *           -> HudiRecordWriter.close()
 *               -> commits the Hudi timeline instant atomically
 *
 * Read Path (future)
 * ------------------
 * HudiSnapshotUtils and HudiMORRecordReader handle file listing and MOR
 * log merging. These require Hudi 0.15.0 API adaptation and are excluded
 * from this build. For now, Dremio's native Parquet scanner reads COW
 * base files if pointed at the correct paths.
 */
public class HudiFormatPlugin extends EasyFormatPlugin<HudiFormatPluginConfig> {

  private static final Logger logger = LoggerFactory.getLogger(HudiFormatPlugin.class);

  // Record key field from HudiPluginConfig; passed to HudiParquetRecordReader
  // so it can look up MOR merge-map entries by key during reads.
  private final String recordKeyField;

  // Read batch size from HudiPluginConfig; passed to HudiParquetRecordReader
  // so it is tunable without recompiling.
  private final int readBatchSize;

  // Directory-based format matcher: identifies Hudi tables by .hoodie/ presence.
  // Replaces EasyFormatPlugin's default extension-based BasicFormatMatcher so
  // that Dremio auto-discovers existing Hudi tables during metadata refresh.
  private final HudiFormatMatcher hudiMatcher;

  /**
   * Standard 4-arg constructor used by Dremio's reflection-based plugin instantiation.
   *
   * @param name            Format plugin name ("hudi")
   * @param context         Dremio plugin context
   * @param config          HudiFormatPluginConfig
   * @param storagePlugin   The owning storage plugin (FileSystemPlugin / HudiStoragePlugin)
   */
  public HudiFormatPlugin(
      String name,
      PluginSabotContext context,
      HudiFormatPluginConfig config,
      SupportsFsCreation storagePlugin) {
    super(
        name,
        context,
        config != null ? config : new HudiFormatPluginConfig(),
        /* blockSplittable */ false,
        /* compressible */    false,
        /* extensions */      Arrays.asList("parquet"),
        /* name */            "hudi");

    if (storagePlugin instanceof HudiStoragePlugin) {
      com.dremio.plugins.hudi.conf.HudiPluginConfig hudiConfig =
          ((HudiStoragePlugin) storagePlugin).getHudiConfig();
      this.recordKeyField = (hudiConfig != null && hudiConfig.defaultRecordKeyField != null)
          ? hudiConfig.defaultRecordKeyField : "id";
      this.readBatchSize  = (hudiConfig != null && hudiConfig.readBatchSize > 0)
          ? hudiConfig.readBatchSize : 4096;
    } else {
      this.recordKeyField = "id";
      this.readBatchSize  = 4096;
    }
    this.hudiMatcher = new HudiFormatMatcher(this);
  }

  // -----------------------------------------------------------------------
  // Format detection (auto-discovery)
  // -----------------------------------------------------------------------

  /**
   * Returns a directory-based FormatMatcher that identifies Hudi tables by
   * the presence of a {@code .hoodie/} subdirectory.
   *
   * Dremio calls getMatcher() during metadata refresh to get the matcher for
   * each registered format plugin. When the returned matcher's matches()
   * returns true for a directory, Dremio registers that directory as a Hudi
   * dataset automatically — without requiring manual promotion in the UI.
   *
   * This overrides EasyFormatPlugin's default BasicFormatMatcher (extension-based).
   */
  @Override
  public FormatMatcher getMatcher() {
    return hudiMatcher;
  }

  // -----------------------------------------------------------------------
  // EasyFormatPlugin abstract method implementations
  // -----------------------------------------------------------------------

  /**
   * Read support for Hudi data files is deferred (requires HudiSnapshotUtils
   * adaptation for Hudi 0.15.0 internal APIs). For now, Dremio's native
   * Parquet scanner handles COW base files.
   */
  @Override
  public RecordReader getRecordReader(
      OperatorContext context,
      FileSystem fs,
      EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns) throws ExecutionSetupException {
    String path = splitAttributes.getPath();
    logger.debug("HudiFormatPlugin.getRecordReader(): reading {}", path);
    return new HudiParquetRecordReader(context, path, columns, recordKeyField, readBatchSize);
  }

  @Override
  public int getReaderOperatorType() {
    return 29; // EASY_JSON_SUB_SCAN — placeholder
  }

  @Override
  public int getWriterOperatorType() {
    return 44; // EASY_JSON_WRITER — placeholder
  }

  // -----------------------------------------------------------------------
  // Write Path
  // -----------------------------------------------------------------------

  /**
   * Creates a HudiRecordWriter for the given write operation.
   * Called by Dremio once per executor thread during CTAS / INSERT INTO.
   */
  @Override
  public RecordWriter getRecordWriter(OperatorContext operatorContext, EasyWriter writer)
      throws IOException {

    String location = writer.getLocation();
    WriterOptions options = writer.getOptions();

    // Extract HudiPluginConfig from the owning storage plugin
    HudiPluginConfig hudiConfig = null;
    SupportsFsCreation fsCreator = writer.getFileSystemCreator();
    if (fsCreator instanceof HudiStoragePlugin) {
      hudiConfig = ((HudiStoragePlugin) fsCreator).getHudiConfig();
    } else if (fsCreator instanceof FileSystemPlugin) {
      logger.debug("Storage plugin is {} not HudiStoragePlugin; using default Hudi config",
          fsCreator.getClass().getSimpleName());
    }

    logger.info("Creating HudiRecordWriter for path: {}", location);
    return new HudiRecordWriter(location, options, hudiConfig, getContext());
  }
}
