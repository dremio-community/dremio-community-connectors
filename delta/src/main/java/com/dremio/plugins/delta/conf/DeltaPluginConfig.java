package com.dremio.plugins.delta.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.plugins.delta.DeltaStoragePlugin;
import io.protostuff.Tag;

import java.util.List;

/**
 * Configuration class for the Dremio Delta Lake Storage Plugin.
 *
 * Shown in the Dremio "Add Source" UI. Each @Tag field becomes a form element.
 * Serialized via Protostuff and stored in the Dremio catalog.
 *
 * The @SourceType annotation registers the plugin as "DELTA" in Dremio's
 * plugin registry so it appears in the Sources UI.
 */
@SourceType(value = "DELTA", label = "Delta Lake", uiConfig = "delta-layout.json")
public class DeltaPluginConfig extends FileSystemConf<DeltaPluginConfig, DeltaStoragePlugin> {

  // -----------------------------------------------------------------------
  // Storage location
  // -----------------------------------------------------------------------

  /**
   * Root path where Delta Lake tables live.
   * Example: "s3://my-bucket/delta-tables" or "/mnt/datalake/delta"
   * Each subdirectory containing a _delta_log/ folder is treated as a table.
   */
  @Tag(1)
  @DisplayMetadata(label = "Root Path")
  public String rootPath;

  // -----------------------------------------------------------------------
  // Write options
  // -----------------------------------------------------------------------

  /**
   * Default Parquet compression codec for new data files.
   * SNAPPY is the Delta Lake default and offers a good size/speed trade-off.
   */
  @Tag(2)
  @DisplayMetadata(label = "Compression Codec")
  public CompressionCodec compressionCodec = CompressionCodec.SNAPPY;

  /**
   * Default partition column for new tables created via CTAS.
   * Leave empty to create non-partitioned tables.
   * Individual tables can override this via table properties.
   */
  @Tag(3)
  @DisplayMetadata(label = "Default Partition Column (optional, for new tables)")
  public String defaultPartitionColumn = "";

  /**
   * Allow new columns to be added to existing Delta tables on write.
   * When true, INSERT INTO with extra columns triggers a schema evolution
   * commit (updates Metadata in the Delta log) before writing data.
   * When false, extra columns are dropped and a warning is logged.
   */
  @Tag(4)
  @DisplayMetadata(label = "Allow Schema Evolution on Write")
  @NotMetadataImpacting
  public boolean allowSchemaEvolution = false;

  // -----------------------------------------------------------------------
  // Performance / file sizing
  // -----------------------------------------------------------------------

  /**
   * Target Parquet row group size in bytes.
   * Each DeltaRecordWriter produces one Parquet file with row groups of this
   * size. Default 128MB matches the Delta Lake default and HDFS block size.
   */
  @Tag(5)
  @DisplayMetadata(label = "Target File Size (bytes)")
  @NotMetadataImpacting
  public long targetFileSizeBytes = 134_217_728L; // 128 MB

  /**
   * Maximum rows to accumulate per writer thread before writing them to the
   * Parquet file. Lower this for very wide rows to avoid OOM; raise it to
   * reduce Parquet write overhead on narrow tables.
   */
  @Tag(6)
  @DisplayMetadata(label = "Write Buffer Rows (per thread)")
  @NotMetadataImpacting
  public int writeBufferMaxRows = 100_000;

  /**
   * Number of Parquet rows read per Arrow batch during SELECT queries.
   *
   * Higher values reduce per-batch overhead (vector resets, loop iterations)
   * but increase heap pressure per executor thread. Lower values reduce
   * memory footprint at the cost of more batch boundaries.
   *
   * Default 4096 matches Dremio's standard batch target size.
   */
  @Tag(8)
  @DisplayMetadata(label = "Read Batch Size (rows per Arrow batch)")
  @NotMetadataImpacting
  public int readBatchSize = 4096;

  /**
   * How long (seconds) to cache the Delta snapshot (active file list) per table.
   *
   * Without caching, every file split triggers a full _delta_log replay —
   * O(N) log reads per query on an N-file table. The cache holds the active
   * AddFile list keyed by canonical table path. All splits for the same table
   * share a single cache entry after the first split pays the full read cost.
   *
   * The cache uses a two-layer freshness strategy:
   *   1. Version check: on each cache hit, cheaply tests whether a newer
   *      _delta_log commit file exists. If found, rebuilds immediately.
   *   2. TTL fallback: evicts the entry after this many seconds regardless,
   *      bounding retention and covering edge cases the version check misses.
   *
   * Writes through this connector always call invalidateCache() after commit,
   * so post-write reads are always fresh. The TTL mainly guards against
   * external writers (Spark, other engines) mutating the table.
   *
   * Set to 0 to disable caching entirely.
   */
  @Tag(9)
  @DisplayMetadata(label = "Snapshot Cache TTL (seconds, 0=disabled)")
  @NotMetadataImpacting
  public int metadataCacheTtlSeconds = 60;

  // -----------------------------------------------------------------------
  // Connection properties
  // -----------------------------------------------------------------------

  /**
   * Additional Hadoop / S3 / ADLS connection properties passed to the
   * underlying Hadoop FileSystem.
   * Example: fs.s3a.endpoint, fs.s3a.access.key
   */
  @Tag(7)
  @DisplayMetadata(label = "Connection Properties")
  public List<Property> propertyList;

  // -----------------------------------------------------------------------
  // FileSystemConf overrides
  // -----------------------------------------------------------------------

  @Override
  public DeltaStoragePlugin newPlugin(
      com.dremio.exec.catalog.PluginSabotContext context,
      String name,
      javax.inject.Provider<com.dremio.exec.catalog.StoragePluginId> pluginIdProvider) {
    return new DeltaStoragePlugin(this, context, name, pluginIdProvider);
  }

  @Override
  public boolean isImpersonationEnabled() {
    return false;
  }

  @Override
  public boolean isPartitionInferenceEnabled() {
    return false;
  }

  @Override
  public String getConnection() {
    if (rootPath == null || rootPath.isEmpty()) {
      return "file:///";
    }
    try {
      java.net.URI uri = java.net.URI.create(rootPath);
      if (uri.getScheme() != null) {
        String authority = uri.getAuthority() != null ? uri.getAuthority() : "";
        return uri.getScheme() + "://" + authority + "/";
      }
    } catch (Exception ignored) {}
    return "file:///";
  }

  @Override
  public com.dremio.io.file.Path getPath() {
    if (rootPath == null || rootPath.isEmpty()) {
      return com.dremio.io.file.Path.of("/");
    }
    try {
      java.net.URI uri = java.net.URI.create(rootPath);
      if (uri.getScheme() != null) {
        String path = uri.getPath();
        return com.dremio.io.file.Path.of(path != null && !path.isEmpty() ? path : "/");
      }
    } catch (Exception ignored) {}
    return com.dremio.io.file.Path.of(rootPath);
  }

  @Override
  public SchemaMutability getSchemaMutability() {
    // ALL_MUTABLE: enables CTAS, INSERT, UPDATE, DELETE, DROP TABLE via SQL
    return SchemaMutability.USER_TABLE;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  // -----------------------------------------------------------------------
  // Enums
  // -----------------------------------------------------------------------

  public enum CompressionCodec {
    SNAPPY,
    GZIP,
    ZSTD,
    UNCOMPRESSED
  }
}
