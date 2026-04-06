package com.dremio.plugins.hudi.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Property;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.dfs.FileSystemConf;
import com.dremio.exec.store.dfs.SchemaMutability;
import com.dremio.plugins.hudi.HudiStoragePlugin;
import io.protostuff.Tag;

import java.util.List;

/**
 * Configuration class for the Dremio Hudi Storage Plugin.
 *
 * This is the "source config" that appears in Dremio's Add Source UI.
 * Each field annotated with @Tag becomes a form field in the UI.
 * Dremio serializes this via Protostuff and stores it in the catalog.
 *
 * The @SourceType annotation registers the plugin under the name "HUDI"
 * in Dremio's plugin registry so it can be selected when adding a source.
 */
@SourceType(value = "HUDI", label = "Apache Hudi", uiConfig = "hudi-layout.json")
public class HudiPluginConfig extends FileSystemConf<HudiPluginConfig, HudiStoragePlugin> {

  // -----------------------------------------------------------------------
  // Storage location
  // -----------------------------------------------------------------------

  /**
   * Root path of the Hudi tables on object storage or HDFS.
   * Example: "s3://my-bucket/hudi-tables" or "/mnt/datalake/hudi"
   */
  @Tag(1)
  @DisplayMetadata(label = "Root Path")
  public String rootPath;

  // -----------------------------------------------------------------------
  // Hudi write options
  // -----------------------------------------------------------------------

  /**
   * Hudi table type to use when CREATING new tables.
   * COPY_ON_WRITE: simpler, better read performance, higher write amplification.
   * MERGE_ON_READ: faster writes, requires compaction for read performance.
   *
   * Existing tables retain whatever type they were created with.
   */
  @Tag(2)
  @DisplayMetadata(label = "Default Table Type (for new tables)")
  public HudiTableType defaultTableType = HudiTableType.COPY_ON_WRITE;

  /**
   * Record key field name in incoming data.
   * Hudi requires a unique record key per table.
   * This is the DEFAULT - individual tables can override via table properties.
   */
  @Tag(3)
  @DisplayMetadata(label = "Default Record Key Field")
  public String defaultRecordKeyField = "id";

  /**
   * Partition path field for Hudi partitioning.
   * Leave empty for non-partitioned tables.
   */
  @Tag(4)
  @DisplayMetadata(label = "Default Partition Path Field (optional)")
  public String defaultPartitionPathField = "";

  /**
   * Precombine field used for deduplication on upsert.
   * Hudi picks the record with the HIGHEST value in this field
   * when multiple records share the same record key.
   */
  @Tag(5)
  @DisplayMetadata(label = "Default Precombine Field")
  public String defaultPrecombineField = "ts";

  // -----------------------------------------------------------------------
  // Performance / parallelism
  // -----------------------------------------------------------------------

  /**
   * Number of parallel insert/upsert buckets.
   * Tune based on table size. Default 2 is conservative.
   */
  @Tag(6)
  @DisplayMetadata(label = "Write Parallelism (buckets)")
  @NotMetadataImpacting
  public int writeParallelism = 2;

  /**
   * Target size (bytes) for each Hudi base file.
   * Default 128MB matches common Parquet/Iceberg defaults.
   */
  @Tag(7)
  @DisplayMetadata(label = "Target File Size (bytes)")
  @NotMetadataImpacting
  public long targetFileSizeBytes = 134217728L; // 128 MB

  // -----------------------------------------------------------------------
  // Connection properties (forwarded to Hadoop FileSystem / S3 etc.)
  // -----------------------------------------------------------------------

  /**
   * Maximum number of Hudi records to buffer in memory per writer thread before
   * flushing to storage. Each flush calls HoodieJavaWriteClient.insert()/upsert()
   * without committing; the final commit happens in close().
   *
   * Tune down for memory-constrained environments or very wide rows.
   * Tune up for better Hudi file-sizing on small batches.
   */
  @Tag(8)
  @DisplayMetadata(label = "Max Write Buffer Rows (per thread)")
  @NotMetadataImpacting
  public int writeBufferMaxRows = 100_000;

  /**
   * How many commits trigger an automatic clustering pass (COW) or compaction pass (MOR).
   *
   * After a successful write commit, the writer counts completed commits on the active
   * timeline. If {@code completedCommits % clusteringCommitInterval == 0} it schedules
   * and runs clustering/compaction immediately:
   *
   *   COW tables → HoodieJavaWriteClient.scheduleClustering() + cluster()
   *                Bins many small Parquet files into fewer large files, improving
   *                scan performance and reducing metadata overhead.
   *
   *   MOR tables → HoodieJavaWriteClient.scheduleCompaction() + compact()
   *                Merges Avro/HFile delta logs back into the base Parquet file,
   *                restoring full COW read performance.
   *
   * Set to 0 to disable. Default 10 means every 10th commit triggers a rewrite.
   * Tune higher (e.g. 20–50) for tables with many small writes; tune lower (e.g. 5)
   * for tables where read performance is critical.
   *
   * This is best-effort: a failure during clustering/compaction is logged as a
   * warning but does NOT roll back or fail the preceding write commit.
   */
  @Tag(10)
  @DisplayMetadata(label = "Auto-Compaction/Clustering Interval (commits, 0=disabled)")
  @NotMetadataImpacting
  public int clusteringCommitInterval = 10;

  /**
   * Additional Hadoop/S3 connection properties.
   * Example: fs.s3a.endpoint, fs.s3a.access.key etc.
   */
  @Tag(9)
  @DisplayMetadata(label = "Connection Properties")
  public List<Property> propertyList;

  /**
   * Number of Parquet rows read per Arrow batch during SELECT queries.
   *
   * Higher values reduce per-batch overhead (vector resets, loop iterations)
   * but increase heap pressure per executor thread. Lower values reduce
   * memory footprint at the cost of more batch boundaries.
   *
   * Default 4096 matches Dremio's standard batch target size.
   */
  @Tag(11)
  @DisplayMetadata(label = "Read Batch Size (rows per Arrow batch)")
  @NotMetadataImpacting
  public int readBatchSize = 4096;

  // -----------------------------------------------------------------------
  // FileSystemConf required overrides
  // -----------------------------------------------------------------------

  @Override
  public HudiStoragePlugin newPlugin(
      com.dremio.exec.catalog.PluginSabotContext context,
      String name,
      javax.inject.Provider<com.dremio.exec.catalog.StoragePluginId> pluginIdProvider) {
    return new HudiStoragePlugin(this, context, name, pluginIdProvider);
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
    // ALL_MUTABLE: allows Dremio to create, alter, and drop tables in this source.
    // This is what enables CTAS, INSERT, UPDATE, DELETE, DROP TABLE via SQL.
    return SchemaMutability.USER_TABLE;
  }

  @Override
  public List<Property> getProperties() {
    return propertyList;
  }

  // -----------------------------------------------------------------------
  // Enum
  // -----------------------------------------------------------------------

  public enum HudiTableType {
    COPY_ON_WRITE,
    MERGE_ON_READ
  }
}
