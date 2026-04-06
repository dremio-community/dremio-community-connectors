package com.dremio.plugins.kafka;

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
 * Dremio source configuration for Apache Kafka.
 *
 * Topics are exposed as tables. Each query snapshot-freezes partition offsets
 * at planning time, so scans are deterministic bounded reads — not infinite streams.
 *
 * Schema modes:
 *   RAW  — metadata columns + _value_raw VARCHAR (raw bytes as UTF-8 string)
 *   JSON — metadata columns + _value_raw VARCHAR + inferred top-level JSON fields
 *   AVRO — metadata columns + _schema_id INT + typed fields from Schema Registry schema
 */
@SourceType(value = "APACHE_KAFKA", label = "Apache Kafka", uiConfig = "kafka-layout.json")
public class KafkaConf extends ConnectionConf<KafkaConf, KafkaStoragePlugin> {

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /**
   * Comma-separated list of Kafka broker addresses (bootstrap servers).
   * Example: "broker1:9092,broker2:9092"
   */
  @Tag(1)
  @DisplayMetadata(label = "Bootstrap Servers")
  public String bootstrapServers = "localhost:9092";

  // -----------------------------------------------------------------------
  // Schema
  // -----------------------------------------------------------------------

  /**
   * Schema mode for topic payload deserialization.
   *
   * RAW  — expose only metadata columns and _value_raw (raw bytes as UTF-8 string).
   *         Fast, always works, no assumptions about message format.
   * JSON — additionally sample messages and infer top-level JSON field types.
   *         Inferred fields are exposed as named columns alongside metadata columns.
   *         Falls back gracefully to _value_raw when a record is not valid JSON.
   * AVRO — metadata columns + _schema_id INT + typed fields from Schema Registry schema.
   *         Requires schemaRegistryUrl to be set. Decodes Confluent wire-format Avro messages.
   */
  @Tag(2)
  @DisplayMetadata(label = "Schema Mode")
  public String schemaMode = "JSON";

  /**
   * Number of records to sample from the latest offsets per partition when
   * inferring a JSON schema. Only used when schemaMode = JSON.
   */
  @Tag(3)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Schema Sample Records")
  public int sampleRecordsForSchema = 100;

  // -----------------------------------------------------------------------
  // Scan window
  // -----------------------------------------------------------------------

  /**
   * Maximum number of records to return per partition in a plain
   * {@code SELECT * FROM kafka.topic} scan (no explicit offset bounds).
   *
   * The connector seeks to {@code max(earliestOffset, latestOffset - maxRecords)}
   * and reads forward to {@code latestOffset} (captured at planning time).
   *
   * This prevents accidental full-topic scans on high-volume topics.
   * Set to 0 to disable the cap (read from earliest offset).
   */
  @Tag(4)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Default Max Records Per Partition")
  public int defaultMaxRecordsPerPartition = 10_000;

  // -----------------------------------------------------------------------
  // Topic filtering
  // -----------------------------------------------------------------------

  /**
   * Java regex pattern for topics to exclude from the Dremio catalog.
   * Default: "^__" — hides all internal Kafka topics (starting with "__").
   * Example: "^(__|_confluent)" to also hide Confluent internal topics.
   */
  @Tag(5)
  @DisplayMetadata(label = "Topic Exclude Pattern (regex)")
  public String topicExcludePattern = "^__";

  /**
   * Optional Java regex pattern for topics to include.
   * When non-empty, only topics matching this pattern are shown.
   * Example: "^(orders|customers|products)" to show only specific topics.
   * Leave blank to include all non-excluded topics.
   */
  @Tag(6)
  @DisplayMetadata(label = "Topic Include Pattern (regex)")
  public String topicIncludePattern = "";

  // -----------------------------------------------------------------------
  // Metadata cache
  // -----------------------------------------------------------------------

  /**
   * How long to cache topic metadata (partition counts, offsets, inferred schemas).
   * Default: 60 seconds. Set to 0 to disable caching (always fetch fresh).
   */
  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Metadata Cache TTL (seconds)")
  public int metadataCacheTtlSeconds = 60;

  // -----------------------------------------------------------------------
  // Security
  // -----------------------------------------------------------------------

  /**
   * Kafka security protocol.
   * PLAINTEXT      — no authentication, no encryption (default)
   * SSL            — TLS encryption only, no authentication
   * SASL_PLAINTEXT — SASL authentication, no encryption
   * SASL_SSL       — SASL authentication + TLS encryption (recommended for production)
   */
  @Tag(8)
  @DisplayMetadata(label = "Security Protocol")
  public String securityProtocol = "PLAINTEXT";

  /**
   * SASL mechanism. Used when securityProtocol is SASL_PLAINTEXT or SASL_SSL.
   * Common values: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512
   */
  @Tag(9)
  @DisplayMetadata(label = "SASL Mechanism")
  public String saslMechanism = "";

  @Tag(10)
  @DisplayMetadata(label = "SASL Username")
  public String saslUsername = "";

  @Tag(11)
  @Secret
  @DisplayMetadata(label = "SASL Password")
  public String saslPassword = "";

  // -----------------------------------------------------------------------
  // SSL / TLS
  // -----------------------------------------------------------------------

  @Tag(12)
  @DisplayMetadata(label = "SSL Truststore Path")
  public String sslTruststorePath = "";

  @Tag(13)
  @Secret
  @DisplayMetadata(label = "SSL Truststore Password")
  public String sslTruststorePassword = "";

  @Tag(14)
  @DisplayMetadata(label = "SSL Truststore Type")
  public String sslTruststoreType = "JKS";

  // -----------------------------------------------------------------------
  // Performance
  // -----------------------------------------------------------------------

  /**
   * Maximum number of records per Kafka consumer poll() call.
   * Controls memory pressure vs. throughput per batch. Default: 500.
   */
  @Tag(15)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Max Poll Records")
  public int maxPollRecords = 500;

  /**
   * Kafka consumer request timeout in milliseconds.
   * Applies to metadata requests and poll() calls. Default: 30000.
   */
  @Tag(16)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Request Timeout (ms)")
  public int requestTimeoutMs = 30_000;

  /**
   * Confluent-compatible Schema Registry URL.
   * Required when schemaMode = AVRO.
   * Supports both http:// and https://.
   * Example: "https://psrc-abc123.us-east-1.aws.confluent.cloud"
   * Leave blank when not using Avro/Schema Registry.
   */
  @Tag(17)
  @DisplayMetadata(label = "Schema Registry URL")
  public String schemaRegistryUrl = "";

  /**
   * Schema Registry username (or API key for Confluent Cloud).
   * Used for HTTP Basic authentication against the Schema Registry.
   * Leave blank for unauthenticated registries.
   */
  @Tag(22)
  @DisplayMetadata(label = "Schema Registry Username")
  public String schemaRegistryUsername = "";

  /**
   * Schema Registry password (or API secret for Confluent Cloud).
   */
  @Tag(23)
  @Secret
  @DisplayMetadata(label = "Schema Registry Password")
  public String schemaRegistryPassword = "";

  /**
   * When true, disables TLS hostname verification for Schema Registry HTTPS connections.
   * Useful for self-signed certificates in internal environments.
   * Do NOT enable in production.
   */
  @Tag(24)
  @DisplayMetadata(label = "Disable Schema Registry SSL Hostname Verification")
  public boolean schemaRegistryDisableSslVerification = false;

  // -----------------------------------------------------------------------
  // SSL / TLS — client keystore (for mutual TLS / mTLS)
  // -----------------------------------------------------------------------

  /**
   * Path to a JKS or PKCS12 keystore containing the client's private key and certificate.
   * Required only when the Kafka broker demands mutual TLS (client authentication).
   * Leave blank for one-way TLS or non-SSL connections.
   */
  @Tag(18)
  @DisplayMetadata(label = "SSL Keystore Path")
  public String sslKeystorePath = "";

  @Tag(19)
  @Secret
  @DisplayMetadata(label = "SSL Keystore Password")
  public String sslKeystorePassword = "";

  /**
   * Keystore file format: JKS or PKCS12. Default: JKS.
   */
  @Tag(20)
  @DisplayMetadata(label = "SSL Keystore Type")
  public String sslKeystoreType = "JKS";

  /**
   * When true, disables TLS hostname verification (ssl.endpoint.identification.algorithm="").
   * Useful for self-signed certificates or internal brokers where the CN/SAN does not
   * match the hostname. Do NOT enable in production environments.
   */
  @Tag(21)
  @DisplayMetadata(label = "Disable SSL Hostname Verification")
  public boolean sslDisableHostnameVerification = false;

  // -----------------------------------------------------------------------
  // ConnectionConf required override
  // -----------------------------------------------------------------------

  @Override
  public KafkaStoragePlugin newPlugin(
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new KafkaStoragePlugin(this, context, name);
  }
}
