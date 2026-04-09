package com.dremio.plugins.dynamodb;

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
 * Dremio source configuration for Amazon DynamoDB.
 *
 * Fields annotated with @Tag are persisted in Dremio's catalog and appear
 * as form fields in the Add Source UI (layout defined in dynamodb-layout.json).
 *
 * @SourceType registers this plugin as "AMAZON_DYNAMODB" in Dremio's registry.
 */
@SourceType(value = "AMAZON_DYNAMODB", label = "Amazon DynamoDB", uiConfig = "dynamodb-layout.json")
public class DynamoDBStoragePluginConfig
    extends ConnectionConf<DynamoDBStoragePluginConfig, DynamoDBStoragePlugin> {

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /** AWS region where the DynamoDB tables live (e.g. "us-east-1"). */
  @Tag(1)
  @DisplayMetadata(label = "AWS Region")
  public String region = "us-east-1";

  /**
   * Optional endpoint override for DynamoDB Local or VPC endpoints.
   * Example: "http://localhost:8000" for local testing.
   * Leave blank to connect to the real AWS DynamoDB endpoint.
   */
  @Tag(2)
  @DisplayMetadata(label = "Endpoint Override")
  public String endpointOverride = "";

  // -----------------------------------------------------------------------
  // Credentials
  // -----------------------------------------------------------------------

  /**
   * AWS Access Key ID.  Leave blank to use the default credential chain
   * (IAM role, instance profile, environment variables, ~/.aws/credentials).
   */
  @Tag(3)
  @DisplayMetadata(label = "Access Key ID")
  public String accessKeyId = "";

  /**
   * AWS Secret Access Key.  Leave blank to use the default credential chain.
   */
  @Tag(4)
  @Secret
  @DisplayMetadata(label = "Secret Access Key")
  public String secretAccessKey = "";

  // -----------------------------------------------------------------------
  // Schema inference
  // -----------------------------------------------------------------------

  /**
   * Number of items to scan per table for schema inference.
   *
   * DynamoDB is schemaless; Dremio infers a fixed Arrow schema by sampling
   * up to this many items per table. Larger values improve accuracy at the
   * cost of slower metadata refresh. Default: 100.
   */
  @Tag(5)
  @DisplayMetadata(label = "Schema Sample Size")
  public int sampleSize = 100;

  // -----------------------------------------------------------------------
  // Performance
  // -----------------------------------------------------------------------

  /**
   * Number of parallel scan segments per table.
   *
   * DynamoDB's Parallel Scan divides a table into TotalSegments and reads
   * each segment in a separate Dremio executor fragment. Higher values
   * increase throughput on large tables. Default: 4.
   */
  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Split Parallelism")
  public int splitParallelism = 4;

  /**
   * AWS SDK call timeout in seconds. Increase for slow networks or very
   * large DynamoDB pages. Default: 30 seconds.
   */
  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Read Timeout (seconds)")
  public int readTimeoutSeconds = 30;

  /**
   * Maximum number of items returned per DynamoDB Scan/Query page.
   * Controls memory pressure vs. round-trip count. Default: 1000.
   */
  @Tag(8)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Page Size")
  public int maxPageSize = 1_000;

  /**
   * Schema metadata cache TTL in seconds.
   *
   * Dremio caches the inferred Arrow schema for each DynamoDB table for this
   * many seconds before re-sampling. Set to 0 to disable caching (always re-sample).
   * Default: 60 seconds.
   */
  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Schema Cache TTL (seconds)")
  public int metadataCacheTtlSeconds = 60;

  // -----------------------------------------------------------------------
  // ConnectionConf required override
  // -----------------------------------------------------------------------

  @Override
  public DynamoDBStoragePlugin newPlugin(
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new DynamoDBStoragePlugin(this, context, name);
  }
}
