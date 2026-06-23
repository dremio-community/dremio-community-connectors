package com.dremio.plugins.pubsub;

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
 * Dremio source configuration for Google Cloud Pub/Sub.
 *
 * Subscriptions are exposed as tables. Each query pulls up to defaultMaxMessages
 * from the subscription and NACKs them immediately after, so messages stay available
 * for other consumers (pull-without-ack bounded snapshot semantics).
 *
 * IMPORTANT: Create a dedicated Dremio query subscription per topic. Do not share
 * the subscription used by your CDC pipeline or application consumers — queries NACK
 * messages so they remain in the subscription, but concurrent pulls may cause races.
 *
 * Auth precedence:
 *   1. emulatorHost — use anonymous credentials against local emulator
 *   2. credentialsFile — service-account JSON key file
 *   3. ADC — Application Default Credentials (gcloud auth, Workload Identity, etc.)
 *
 * Schema modes:
 *   RAW  — metadata columns + _value_raw VARCHAR (raw message bytes as UTF-8)
 *   JSON — metadata columns + _value_raw + inferred top-level JSON fields
 */
@SourceType(value = "GOOGLE_PUBSUB", label = "Google Pub/Sub", uiConfig = "pubsub-layout.json")
public class PubSubConf extends ConnectionConf<PubSubConf, PubSubStoragePlugin> {

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /** GCP project ID containing the subscriptions to query. */
  @Tag(1)
  @DisplayMetadata(label = "GCP Project ID")
  public String projectId = "";

  /**
   * Path to a service-account JSON credentials file.
   * Leave blank to use Application Default Credentials (ADC).
   */
  @Tag(2)
  @DisplayMetadata(label = "Service Account Credentials File (leave blank for ADC)")
  public String credentialsFile = "";

  /**
   * Pub/Sub emulator host:port (e.g. "localhost:8085").
   * When set, skips auth and routes all requests to the emulator.
   * Leave blank for production GCP.
   */
  @Tag(3)
  @DisplayMetadata(label = "Emulator Host (blank for production)")
  public String emulatorHost = "";

  // -----------------------------------------------------------------------
  // Schema
  // -----------------------------------------------------------------------

  /**
   * Schema mode for message payload deserialization.
   *
   * RAW  — expose only metadata columns and _value_raw (raw bytes as UTF-8 string).
   *         Fast, always works, no assumptions about message format.
   * JSON — additionally sample messages and infer top-level JSON field types.
   *         Inferred fields are exposed as named columns alongside metadata columns.
   *         Falls back gracefully to _value_raw when a record is not valid JSON.
   */
  @Tag(4)
  @DisplayMetadata(label = "Schema Mode")
  public String schemaMode = "JSON";

  /**
   * Number of messages to sample from the subscription when inferring a JSON schema.
   * Only used when schemaMode = JSON. These messages are NACKed (not consumed).
   */
  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Schema Sample Messages")
  public int sampleMessagesForSchema = 20;

  // -----------------------------------------------------------------------
  // Scan window
  // -----------------------------------------------------------------------

  /**
   * Maximum number of messages to return in a plain SELECT * scan.
   * Pub/Sub has no concept of offsets or "earliest" — this caps how many
   * currently-buffered messages to pull per query.
   * Set higher for topics with many pending messages.
   */
  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Default Max Messages Per Scan")
  public int defaultMaxMessages = 1000;

  /**
   * How long to wait for messages per pull batch (seconds).
   * Pub/Sub returns immediately if no messages are available; this controls
   * how long we keep retrying pull batches until we have enough messages or give up.
   */
  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Pull Timeout (seconds)")
  public int pullTimeoutSeconds = 10;

  // -----------------------------------------------------------------------
  // Subscription filtering
  // -----------------------------------------------------------------------

  /**
   * Optional Java regex pattern for subscriptions to include.
   * When non-empty, only subscriptions matching this pattern are shown as tables.
   * Leave blank to include all non-excluded subscriptions.
   */
  @Tag(8)
  @DisplayMetadata(label = "Subscription Include Pattern (regex)")
  public String subscriptionIncludePattern = "";

  /**
   * Java regex pattern for subscriptions to exclude from the Dremio catalog.
   * Default: "_dremio_" — hides any internal Dremio-managed subscriptions.
   */
  @Tag(9)
  @DisplayMetadata(label = "Subscription Exclude Pattern (regex)")
  public String subscriptionExcludePattern = "_dremio_";

  // -----------------------------------------------------------------------
  // Metadata cache
  // -----------------------------------------------------------------------

  /**
   * How long to cache subscription metadata (schema, row count estimate).
   * Default: 60 seconds. Set to 0 to disable caching (always fetch fresh).
   */
  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Metadata Cache TTL (seconds)")
  public int metadataCacheTtlSeconds = 60;

  // -----------------------------------------------------------------------
  // ConnectionConf required override
  // -----------------------------------------------------------------------

  @Override
  public PubSubStoragePlugin newPlugin(
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new PubSubStoragePlugin(this, context, name);
  }
}
