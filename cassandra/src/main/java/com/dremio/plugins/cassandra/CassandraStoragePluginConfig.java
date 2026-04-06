package com.dremio.plugins.cassandra;

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
 * Dremio source configuration for Apache Cassandra.
 *
 * Fields annotated with @Tag are serialized by Protostuff and persisted
 * in Dremio's catalog. They also appear as form fields in the Add Source UI
 * (layout defined in cassandra-layout.json).
 *
 * @SourceType registers this plugin under the name "APACHE_CASSANDRA" in
 * Dremio's plugin registry. The value avoids the "CASSANDRA" identifier that
 * Dremio 26.x reserves as a disabled placeholder in its frontend source picker.
 */
@SourceType(value = "APACHE_CASSANDRA", label = "Apache Cassandra", uiConfig = "cassandra-layout.json")
public class CassandraStoragePluginConfig
    extends ConnectionConf<CassandraStoragePluginConfig, CassandraStoragePlugin> {

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /**
   * Comma-separated list of Cassandra contact point hostnames or IPs.
   * Example: "cassandra-node-1,cassandra-node-2"
   */
  @Tag(1)
  @DisplayMetadata(label = "Contact Points")
  public String host = "localhost";

  /**
   * Cassandra native transport port (default 9042).
   */
  @Tag(2)
  @DisplayMetadata(label = "Port")
  public int port = 9042;

  /**
   * Name of the local datacenter for DataStax driver load-balancing.
   * Leave blank to auto-detect from {@code system.local} at connect time.
   * Example: "datacenter1" (the Cassandra single-node default).
   */
  @Tag(3)
  @DisplayMetadata(label = "Local Datacenter")
  public String datacenter = "";

  // -----------------------------------------------------------------------
  // Credentials (optional — many Cassandra clusters have auth disabled)
  // -----------------------------------------------------------------------

  @Tag(4)
  @DisplayMetadata(label = "Username")
  public String username;

  @Tag(5)
  @Secret
  @DisplayMetadata(label = "Password")
  public String password;

  // -----------------------------------------------------------------------
  // Performance / behaviour
  // -----------------------------------------------------------------------

  /**
   * CQL request timeout in milliseconds. Default 30 seconds.
   * Increase for large full-table scans on slow networks.
   */
  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Read Timeout (ms)")
  public int readTimeoutMs = 30_000;

  /**
   * Number of rows returned per CQL page (fetch size).
   * Controls memory pressure vs. round-trip count.
   */
  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Fetch Size (rows per page)")
  public int fetchSize = 1_000;

  /**
   * Comma-separated list of keyspaces to exclude from Dremio.
   * System keyspaces (system, system_auth, system_schema, system_distributed,
   * system_traces) are always excluded regardless of this setting.
   */
  @Tag(8)
  @DisplayMetadata(label = "Excluded Keyspaces")
  public String excludedKeyspaces = "";

  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Consistency Level")
  public String consistencyLevel = "LOCAL_ONE";

  @Tag(10)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable SSL/TLS")
  public boolean sslEnabled = false;

  @Tag(11)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Enable Speculative Execution")
  public boolean speculativeExecutionEnabled = false;

  @Tag(12)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Speculative Execution Delay (ms)")
  public int speculativeExecutionDelayMs = 500;

  @Tag(13)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Split Parallelism")
  public int splitParallelism = 8;

  @Tag(14)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Metadata Cache TTL (seconds)")
  public int metadataCacheTtlSeconds = 60;

  /**
   * Comma-separated list of fallback datacenters.
   *
   * When non-empty the connector automatically relaxes LOCAL_ consistency levels
   * (LOCAL_ONE → ONE, LOCAL_QUORUM → QUORUM) so the DataStax driver can route
   * requests to remote-DC nodes if local-DC nodes are unavailable.
   * The driver's DefaultLoadBalancingPolicy already prefers the local DC and
   * falls back to remote DCs — this setting simply unlocks that fallback path.
   *
   * Example: "dc-west,dc-eu"
   */
  @Tag(15)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Fallback Datacenters")
  public String fallbackDatacenters = "";

  // -----------------------------------------------------------------------
  // SSL / TLS
  // -----------------------------------------------------------------------

  /**
   * Path to a JKS or PKCS12 truststore file used to verify the Cassandra
   * server's certificate.  Leave blank to use the JVM default trust store
   * (suitable when the server cert is signed by a well-known CA already in
   * the JVM cacerts).
   *
   * Example: "/etc/dremio/certs/cassandra-truststore.jks"
   */
  @Tag(16)
  @DisplayMetadata(label = "TLS Truststore Path")
  public String sslTruststorePath = "";

  /**
   * Password for the truststore file.  Leave blank if the truststore has
   * no password.
   */
  @Tag(17)
  @Secret
  @DisplayMetadata(label = "TLS Truststore Password")
  public String sslTruststorePassword = "";

  /**
   * Truststore file format.  Must be {@code JKS} or {@code PKCS12}.
   * Default: {@code JKS}.
   */
  @Tag(18)
  @DisplayMetadata(label = "TLS Truststore Type")
  public String sslTruststoreType = "JKS";

  /**
   * Path to a JKS or PKCS12 keystore file that holds the client certificate
   * and private key for mutual TLS (mTLS) authentication.
   * Leave blank for one-way TLS (no client certificate).
   *
   * Example: "/etc/dremio/certs/cassandra-keystore.jks"
   */
  @Tag(19)
  @DisplayMetadata(label = "mTLS Keystore Path")
  public String sslKeystorePath = "";

  /**
   * Password for the keystore file (also used as the key-entry password).
   */
  @Tag(20)
  @Secret
  @DisplayMetadata(label = "mTLS Keystore Password")
  public String sslKeystorePassword = "";

  /**
   * Keystore file format.  Must be {@code JKS} or {@code PKCS12}.
   * Default: {@code JKS}.
   */
  @Tag(21)
  @DisplayMetadata(label = "mTLS Keystore Type")
  public String sslKeystoreType = "JKS";

  /**
   * When {@code true} (default), the DataStax driver verifies that the
   * Cassandra node's hostname matches the certificate CN / SAN.
   * Set to {@code false} only for self-signed certs in dev/test environments.
   */
  @Tag(22)
  @NotMetadataImpacting
  @DisplayMetadata(label = "TLS Hostname Verification")
  public boolean sslHostnameVerification = true;

  // -----------------------------------------------------------------------
  // Protocol compression
  // -----------------------------------------------------------------------

  /**
   * CQL wire-protocol compression algorithm.
   *
   * Accepted values: {@code NONE} (default), {@code LZ4}, {@code SNAPPY}.
   *
   * <ul>
   *   <li>{@code LZ4} — fast compression with good ratios; requires the
   *       {@code lz4-java} library (already on the DataStax driver classpath).</li>
   *   <li>{@code SNAPPY} — alternative algorithm; requires the
   *       {@code snappy-java} library on the classpath.</li>
   *   <li>{@code NONE} — no compression (default); best for low-latency local
   *       networks where bandwidth is not the bottleneck.</li>
   * </ul>
   *
   * Recommendation: enable {@code LZ4} for WAN or cross-AZ deployments where
   * network bandwidth is limited. Leave {@code NONE} on fast local networks to
   * avoid unnecessary CPU overhead.
   */
  @Tag(23)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Protocol Compression")
  public String compressionAlgorithm = "NONE";

  // -----------------------------------------------------------------------
  // Async page prefetch
  // -----------------------------------------------------------------------

  /**
   * Enable async page prefetch for large table scans.
   *
   * <p>When {@code true} (default), the connector uses the DataStax driver's
   * asynchronous execution API ({@code session.executeAsync}) to overlap network
   * I/O with Arrow vector writes:
   * <ol>
   *   <li>While Dremio writes rows from page <i>N</i> into Arrow vectors,
   *       the driver is simultaneously fetching page <i>N+1</i> over the
   *       network.</li>
   *   <li>When {@code next()} is called again, page <i>N+1</i> is already
   *       buffered (or close to it), eliminating the per-page round-trip
   *       stall.</li>
   * </ol>
   *
   * <p>This yields measurable improvements on large full-table scans where many
   * CQL pages are needed.  For small queries (partition key lookup, small LIMIT)
   * the gain is negligible because only one or two pages are ever fetched.
   *
   * <p>Set to {@code false} to revert to synchronous page fetching (simpler
   * stack trace, easier debugging of paging issues).
   */
  @Tag(24)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Async Page Prefetch")
  public boolean asyncPagePrefetch = true;

  // -----------------------------------------------------------------------
  // ConnectionConf required override
  // -----------------------------------------------------------------------

  @Override
  public CassandraStoragePlugin newPlugin(
      PluginSabotContext context,
      String name,
      Provider<StoragePluginId> pluginIdProvider) {
    return new CassandraStoragePlugin(this, context, name);
  }
}
