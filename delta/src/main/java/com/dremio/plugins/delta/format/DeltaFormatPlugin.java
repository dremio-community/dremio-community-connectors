package com.dremio.plugins.delta.format;

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
import com.dremio.plugins.delta.DeltaStoragePlugin;
import com.dremio.plugins.delta.conf.DeltaPluginConfig;
import com.dremio.plugins.delta.read.DeltaParquetRecordReader;
import com.dremio.plugins.delta.read.DeltaSnapshotUtils;
import com.dremio.plugins.delta.write.DeltaRecordWriter;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.exec.store.easy.proto.EasyProtobuf;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Dremio FormatPlugin for Delta Lake.
 *
 * Extends EasyFormatPlugin which provides default implementations for most
 * FormatPlugin interface methods. We override getRecordWriter() to supply
 * our DeltaRecordWriter for the write path.
 *
 * Write Path
 * ----------
 * SQL:  INSERT INTO delta_source.orders SELECT * FROM raw.orders
 *
 *   Dremio planner
 *     -> getRecordWriter(OperatorContext, EasyWriter)
 *         -> DeltaRecordWriter (per executor thread)
 *             -> Arrow rows -> Avro -> Parquet file written to storage
 *         -> DeltaRecordWriter.close()
 *             -> DeltaLog.startTransaction().commit(AddFile)
 */
public class DeltaFormatPlugin extends EasyFormatPlugin<DeltaFormatPluginConfig> {

  private static final Logger logger = LoggerFactory.getLogger(DeltaFormatPlugin.class);

  // Directory-based format matcher: identifies Delta tables by _delta_log/ presence.
  // Replaces EasyFormatPlugin's default extension-based BasicFormatMatcher so
  // that Dremio auto-discovers Delta tables during metadata refresh without
  // requiring manual promotion in the UI.
  private final DeltaFormatMatcher deltaMatcher;

  // Extracted from DeltaPluginConfig at construction time and passed to each
  // DeltaParquetRecordReader so both values are configurable without recompiling.
  private final int  readBatchSize;
  private final long cacheTtlMs;

  /**
   * Standard 4-arg constructor used by Dremio's reflection-based plugin instantiation.
   *
   * @param name            Format plugin name ("delta")
   * @param context         Dremio plugin context
   * @param config          DeltaFormatPluginConfig
   * @param storagePlugin   The owning storage plugin (FileSystemPlugin / DeltaStoragePlugin)
   */
  public DeltaFormatPlugin(
      String name,
      PluginSabotContext context,
      DeltaFormatPluginConfig config,
      SupportsFsCreation storagePlugin) {
    super(
        name,
        context,
        config != null ? config : new DeltaFormatPluginConfig(),
        /* blockSplittable */ false,
        /* compressible */    false,
        /* extensions */      Arrays.asList("parquet"),
        /* name */            "delta_write");
    this.deltaMatcher = new DeltaFormatMatcher(this);

    if (storagePlugin instanceof DeltaStoragePlugin) {
      DeltaPluginConfig deltaConfig = ((DeltaStoragePlugin) storagePlugin).getDeltaConfig();
      this.readBatchSize = (deltaConfig != null && deltaConfig.readBatchSize > 0)
          ? deltaConfig.readBatchSize : 4096;
      this.cacheTtlMs    = (deltaConfig != null)
          ? (long) deltaConfig.metadataCacheTtlSeconds * 1000L : 60_000L;
    } else {
      this.readBatchSize = 4096;
      this.cacheTtlMs    = 60_000L;
    }
  }

  // -----------------------------------------------------------------------
  // Format detection (auto-discovery)
  // -----------------------------------------------------------------------

  /**
   * Returns a directory-based FormatMatcher that identifies Delta Lake tables
   * by the presence of a {@code _delta_log/} subdirectory.
   *
   * Dremio calls getMatcher() during metadata refresh to get the matcher for
   * each registered format plugin. When the returned matcher's matches()
   * returns true for a directory, Dremio registers that directory as a
   * delta_write dataset automatically — without requiring manual promotion.
   *
   * This overrides EasyFormatPlugin's default BasicFormatMatcher (extension-based).
   */
  @Override
  public FormatMatcher getMatcher() {
    return deltaMatcher;
  }

  // -----------------------------------------------------------------------
  // EasyFormatPlugin abstract method implementations
  // -----------------------------------------------------------------------

  /**
   * Returns a DeltaParquetRecordReader for the given Parquet file split.
   *
   * Called by Dremio once per file split during a SELECT on a delta_write dataset.
   * DeltaParquetRecordReader validates the file against the current Delta snapshot
   * before reading — stale (REMOVEd) files are skipped by returning 0 from next().
   *
   * This path is reached when DeltaFormatMatcher has auto-discovered the table
   * and Dremio's file scanner has created one split per .parquet file found in
   * the table directory tree.
   */
  @Override
  public RecordReader getRecordReader(
      OperatorContext context,
      FileSystem fs,
      EasyProtobuf.EasyDatasetSplitXAttr splitAttributes,
      List<SchemaPath> columns) throws ExecutionSetupException {
    String path = splitAttributes != null ? splitAttributes.getPath() : "";
    logger.debug("Creating DeltaParquetRecordReader for split: {}", path);
    return new DeltaParquetRecordReader(context, path, columns, readBatchSize, cacheTtlMs);
  }

  /**
   * Returns the Dremio CoreOperatorType int for the read operator.
   * 29 = EASY_JSON_SUB_SCAN (placeholder).
   */
  @Override
  public int getReaderOperatorType() {
    return 29;
  }

  /**
   * Returns the Dremio CoreOperatorType int for the write operator.
   * 44 = EASY_JSON_WRITER (placeholder).
   */
  @Override
  public int getWriterOperatorType() {
    return 44;
  }

  // -----------------------------------------------------------------------
  // Write Path
  // -----------------------------------------------------------------------

  /**
   * Creates a DeltaRecordWriter for the given write operation.
   *
   * Called by Dremio once per executor thread during CTAS / INSERT INTO.
   *
   * @param operatorContext  Operator-level context
   * @param writer           EasyWriter carrying location, WriterOptions, and the storage plugin
   */
  @Override
  public RecordWriter getRecordWriter(OperatorContext operatorContext, EasyWriter writer)
      throws IOException {

    String location = writer.getLocation();
    WriterOptions options = writer.getOptions();

    // Extract DeltaPluginConfig from the owning storage plugin
    DeltaPluginConfig deltaConfig = null;
    SupportsFsCreation fsCreator = writer.getFileSystemCreator();
    if (fsCreator instanceof DeltaStoragePlugin) {
      deltaConfig = ((DeltaStoragePlugin) fsCreator).getDeltaConfig();
    } else if (fsCreator instanceof FileSystemPlugin) {
      logger.debug("Storage plugin is {} not DeltaStoragePlugin; using default Delta config",
          fsCreator.getClass().getSimpleName());
    }

    logger.info("Creating DeltaRecordWriter for path: {}", location);
    return new DeltaRecordWriter(location, options, deltaConfig, getContext());
  }

  // -----------------------------------------------------------------------
  // Utility methods for read path
  // -----------------------------------------------------------------------

  /**
   * Returns all active Parquet file paths in the current Delta snapshot.
   */
  public List<String> getBaseFilePaths(String tablePath, Configuration conf) throws IOException {
    return DeltaSnapshotUtils.getLatestFilePaths(tablePath, conf);
  }

  /**
   * Returns a summary of the current snapshot (version, file count, total size).
   */
  public DeltaSnapshotUtils.SnapshotSummary getSnapshotSummary(
      String tablePath, Configuration conf) throws IOException {
    return DeltaSnapshotUtils.getSnapshotSummary(tablePath, conf);
  }
}
