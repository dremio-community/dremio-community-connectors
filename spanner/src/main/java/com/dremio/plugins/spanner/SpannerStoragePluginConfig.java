package com.dremio.plugins.spanner;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.catalog.conf.ConnectionConf;
import io.protostuff.Tag;
import javax.inject.Provider;

/**
 * Dremio source configuration for Google Cloud Spanner.
 *
 * Fields annotated with @Tag are persisted in Dremio's catalog and appear
 * as form fields in the Add Source UI (layout defined in spanner-layout.json).
 *
 * @SourceType registers this plugin as "GOOGLE_CLOUD_SPANNER" in Dremio's registry.
 */
@SourceType(value = "GOOGLE_CLOUD_SPANNER", label = "Google Cloud Spanner",
            uiConfig = "spanner-layout.json")
public class SpannerStoragePluginConfig
    extends ConnectionConf<SpannerStoragePluginConfig, SpannerStoragePlugin> {

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /** GCP project ID (e.g. "my-project"). */
  @Tag(1)
  @DisplayMetadata(label = "GCP Project ID")
  public String project = "";

  /** Spanner instance ID (e.g. "my-instance"). */
  @Tag(2)
  @DisplayMetadata(label = "Instance ID")
  public String instance = "";

  /** Spanner database name (e.g. "my-database"). */
  @Tag(3)
  @DisplayMetadata(label = "Database")
  public String database = "";

  // -----------------------------------------------------------------------
  // Authentication
  // -----------------------------------------------------------------------

  /**
   * Path to a service account key JSON file.
   * Leave blank to use Application Default Credentials (gcloud auth, Workload Identity, etc.).
   */
  @Tag(4)
  @DisplayMetadata(label = "Service Account Key File (optional)")
  public String credentialsFile = "";

  // -----------------------------------------------------------------------
  // Performance
  // -----------------------------------------------------------------------

  /**
   * Number of parallel partitions per table scan.
   *
   * Spanner's PartitionQuery API splits a full-table scan into independent
   * partitions that Dremio executor fragments read in parallel. Higher values
   * increase read throughput on large tables. Default: 4.
   */
  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Split Parallelism")
  public int splitParallelism = 4;

  /**
   * Query timeout in seconds. Applies to each Spanner SQL call. Default: 300.
   */
  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Query Timeout (seconds)")
  public int queryTimeoutSeconds = 300;

  /**
   * Schema metadata cache TTL in seconds.
   * Dremio caches the Spanner column schema for each table for this many seconds.
   * Set to 0 to disable caching (always re-query INFORMATION_SCHEMA). Default: 300.
   */
  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Schema Cache TTL (seconds)")
  public int metadataCacheTtlSeconds = 300;

  // -----------------------------------------------------------------------
  // ConnectionConf required override
  // -----------------------------------------------------------------------

  @Override
  public SpannerStoragePlugin newPlugin(
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new SpannerStoragePlugin(this, context, name);
  }
}
