package com.dremio.plugins.hudi;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.plugins.hudi.conf.HudiPluginConfig;
import com.dremio.plugins.hudi.format.HudiFormatPlugin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;
import java.io.IOException;
import java.net.URI;
import java.util.List;

/**
 * Dremio Storage Plugin for Apache Hudi.
 *
 * Architecture Overview
 * ---------------------
 * Dremio's plugin system is built around two layers:
 *
 *   1. StoragePlugin  - manages connection to a storage system (S3, HDFS, NAS etc.)
 *                       and handles table/schema discovery via the catalog.
 *
 *   2. FormatPlugin   - handles reading and WRITING a specific file format
 *                       (Parquet, JSON, Iceberg, or in our case Hudi) within
 *                       a storage system.
 *
 * We extend FileSystemPlugin which already handles:
 *   - Filesystem connections (S3, ADLS, HDFS, NAS)
 *   - Schema discovery (listing directories as tables)
 *   - SchemaMutability routing (which operations are allowed)
 *   - Metadata refresh cycles
 *
 * What we add:
 *   - Register HudiFormatPlugin so Dremio recognizes .hoodie directories
 *   - Override createNewTable() to initialize Hudi metadata on CTAS
 *   - Wire the write path through HudiRecordWriter instead of ParquetRecordWriter
 *
 * Write Path Flow (CTAS / INSERT INTO)
 * --------------------------------------
 * SQL:  INSERT INTO hudi_source.my_table SELECT ...
 *
 *   Dremio planner
 *       -> WriterCommitterPrel
 *           -> FileSystemPlugin.createNewTable() / getTable()
 *               -> HudiFormatPlugin.getWriter()
 *                   -> HudiRecordWriter (per-thread)
 *                       -> converts Arrow RecordBatch to Avro GenericRecord
 *                       -> buffers records in memory
 *                   -> HudiRecordWriter.close()
 *                       -> calls HoodieJavaWriteClient.insert() / upsert()
 *                       -> commits the Hudi timeline instant
 */
public class HudiStoragePlugin extends FileSystemPlugin<HudiPluginConfig> {

  private static final Logger logger = LoggerFactory.getLogger(HudiStoragePlugin.class);

  private final HudiPluginConfig config;

  public HudiStoragePlugin(
      HudiPluginConfig config,
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
    this.config = config;
    logger.info("HudiStoragePlugin initialized. Root: {}, DefaultTableType: {}",
        config.rootPath, config.defaultTableType);
  }

  /**
   * Returns the plugin config, used by HudiFormatPlugin to access Hudi settings.
   */
  public HudiPluginConfig getHudiConfig() {
    return config;
  }

  // -----------------------------------------------------------------------
  // FormatPlugin Registration
  // -----------------------------------------------------------------------
  // FileSystemPlugin discovers format plugins via the Dremio plugin registry.
  // HudiFormatPlugin must be registered in:
  //   src/main/resources/META-INF/services/com.dremio.exec.store.dfs.FormatPlugin
  // pointing to com.dremio.plugins.hudi.format.HudiFormatPlugin
  //
  // During metadata refresh, Dremio calls HudiFormatMatcher.matches() on each
  // directory it scans. If it returns true (.hoodie/ found), Dremio registers
  // that directory as a Hudi dataset automatically — no manual promotion needed.
  // -----------------------------------------------------------------------

  // -----------------------------------------------------------------------
  // Bulk table discovery
  // -----------------------------------------------------------------------

  /**
   * Walks the source's configured root path and returns the Hadoop paths of
   * all Hudi tables found (directories containing a {@code .hoodie/} subdirectory).
   *
   * Use this to bulk-register existing Hudi tables in Dremio's catalog when
   * first connecting to an environment that already has many tables. Pass the
   * returned paths to Dremio's catalog REST API to register each one:
   *
   * <pre>
   *   POST /api/v3/catalog
   *   {
   *     "entityType": "dataset",
   *     "path": ["hudi_source", "table_name"],
   *     "type": "PHYSICAL_DATASET",
   *     "format": {"type": "Parquet"}
   *   }
   * </pre>
   *
   * For ongoing discovery, Dremio's metadata refresh (via HudiFormatMatcher)
   * handles new tables automatically — this method is primarily useful for
   * one-time migrations of large existing environments.
   *
   * @return List of Hadoop Paths to Hudi table roots; empty if none found
   * @throws IOException if the root path cannot be accessed
   */
  public List<Path> discoverHudiTables() throws IOException {
    FileSystem hadoopFs = buildHadoopFileSystem();
    Path rootPath = new Path(config.rootPath);
    return HudiTableDiscovery.findHudiTables(hadoopFs, rootPath);
  }

  /**
   * Same as {@link #discoverHudiTables()} but limits recursion to {@code maxDepth}
   * directory levels. Use this for very large or deeply nested source layouts.
   *
   * @param maxDepth Maximum directory depth to recurse into (0 = root level only)
   */
  public List<Path> discoverHudiTables(int maxDepth) throws IOException {
    FileSystem hadoopFs = buildHadoopFileSystem();
    Path rootPath = new Path(config.rootPath);
    return HudiTableDiscovery.findHudiTables(hadoopFs, rootPath, maxDepth);
  }

  /**
   * Builds a Hadoop FileSystem from the plugin config's connection URI and
   * any additional connection properties (S3 keys, ADLS credentials, etc.).
   */
  private FileSystem buildHadoopFileSystem() throws IOException {
    Configuration hadoopConf = new Configuration();
    if (config.propertyList != null) {
      for (Property p : config.propertyList) {
        hadoopConf.set(p.name, p.value);
      }
    }
    try {
      URI fsUri = URI.create(config.getConnection());
      return FileSystem.get(fsUri, hadoopConf);
    } catch (Exception e) {
      throw new IOException(
          "HudiStoragePlugin: failed to build Hadoop FileSystem for connection '"
              + config.getConnection() + "': " + e.getMessage(), e);
    }
  }
}
