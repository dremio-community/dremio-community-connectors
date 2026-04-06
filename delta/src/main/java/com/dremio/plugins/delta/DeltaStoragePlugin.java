package com.dremio.plugins.delta;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.store.dfs.FileSystemPlugin;
import com.dremio.plugins.delta.conf.DeltaPluginConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Provider;

/**
 * Dremio Storage Plugin for Delta Lake.
 *
 * Architecture Overview
 * ---------------------
 * Dremio's plugin system has two layers:
 *
 *   StoragePlugin  — manages the connection to a storage system (S3, HDFS,
 *                    ADLS, NAS) and handles table/schema discovery.
 *
 *   FormatPlugin   — handles reading and writing a specific file format
 *                    (Parquet, Iceberg, or in our case Delta Lake) within
 *                    the storage system.
 *
 * We extend FileSystemPlugin which already provides:
 *   - Filesystem connections (S3, ADLS, HDFS, NAS)
 *   - Directory listing as tables
 *   - SchemaMutability routing (which SQL operations are allowed)
 *   - Metadata refresh cycles
 *
 * What we add:
 *   - Register DeltaFormatPlugin so Dremio recognizes _delta_log/ directories
 *   - Override createNewTable() to delegate to DeltaFormatPlugin's write path
 *   - Expose the DeltaPluginConfig to the format plugin for write settings
 *
 * Write Path (CTAS / INSERT INTO)
 * --------------------------------
 * SQL:  INSERT INTO delta_source.orders SELECT * FROM raw.orders
 *
 *   Dremio planner
 *     -> WriterCommitterPrel
 *         -> FileSystemPlugin.createNewTable() / getTable()
 *             -> DeltaFormatPlugin.getWriter()
 *                 -> DeltaRecordWriter (per executor thread)
 *                     -> Arrow -> Avro -> Parquet file on storage
 *                 -> DeltaRecordWriter.close()
 *                     -> DeltaLog.startTransaction() / commit(AddFile)
 */
public class DeltaStoragePlugin extends FileSystemPlugin<DeltaPluginConfig> {

  private static final Logger logger = LoggerFactory.getLogger(DeltaStoragePlugin.class);

  private final DeltaPluginConfig config;

  public DeltaStoragePlugin(
      DeltaPluginConfig config,
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> idProvider) {
    super(config, context, name, idProvider);
    this.config = config;
    logger.info("DeltaStoragePlugin initialized. Root: {}", config.rootPath);
  }

  /** Returns the plugin config, used by DeltaFormatPlugin to access write settings. */
  public DeltaPluginConfig getDeltaConfig() {
    return config;
  }

}
