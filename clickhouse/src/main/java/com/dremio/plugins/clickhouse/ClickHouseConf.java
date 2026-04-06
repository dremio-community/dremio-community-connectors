package com.dremio.plugins.clickhouse;

import java.sql.SQLException;
import java.util.Properties;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.conf.AbstractArpConf;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import io.protostuff.Tag;

/**
 * Source configuration for the Dremio ClickHouse Connector.
 *
 * <p>Uses Dremio's ARP (Advanced Relational Pushdown) framework over the official
 * ClickHouse JDBC driver ({@code com.clickhouse:clickhouse-jdbc}). The ARP YAML dialect
 * file ({@code clickhouse-arp.yaml}) drives all SQL translation, predicate pushdown,
 * aggregation pushdown, and function mapping.</p>
 *
 * <p>Connection: HTTP interface (port 8123) or HTTPS (port 8443).</p>
 */
@SourceType(value = "CLICKHOUSE", label = "ClickHouse", uiConfig = "clickhouse-layout.json")
public class ClickHouseConf extends AbstractArpConf<ClickHouseConf> {

  private static final String ARP_FILENAME = "arp/implementation/clickhouse-arp.yaml";
  private static final String DRIVER = "com.clickhouse.jdbc.ClickHouseDriver";

  // ARP dialect — loaded once from the YAML file at class-load time.
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, ClickHouseDialect::new);

  // Pre-load the ClickHouse JDBC driver on the main startup thread.
  // DBCP2 runs driver loading on Dremio worker threads which may have a
  // different execution context that causes ClickHouseDriver.<clinit> to fail.
  // Loading here caches the class so worker-thread calls just return the
  // already-initialized class without re-running the static initializer.
  static {
    try {
      Class.forName(DRIVER, true, ClickHouseConf.class.getClassLoader());
    } catch (Throwable t) {
      throw new RuntimeException(
          "Failed to load ClickHouse JDBC driver (" + DRIVER + "). "
              + "Ensure clickhouse-jdbc-*-all.jar is in $DREMIO_HOME/jars/3rdparty/",
          t);
    }
  }

  // -----------------------------------------------------------------------
  // Connection fields
  // -----------------------------------------------------------------------

  @Tag(1)
  @DisplayMetadata(label = "Host")
  @NotBlank
  public String host = "localhost";

  @Tag(2)
  @DisplayMetadata(label = "Port")
  @Min(1) @Max(65535)
  public int port = 8123;

  @Tag(3)
  @DisplayMetadata(label = "Database")
  public String database = "default";

  @Tag(4)
  @DisplayMetadata(label = "Username")
  @NotMetadataImpacting
  public String username = "default";

  @Tag(5)
  @DisplayMetadata(label = "Password")
  @Secret
  @NotMetadataImpacting
  public String password = "";

  // -----------------------------------------------------------------------
  // SSL / TLS
  // -----------------------------------------------------------------------

  @Tag(6)
  @DisplayMetadata(label = "Enable SSL / TLS")
  public boolean useSsl = false;

  @Tag(7)
  @DisplayMetadata(label = "SSL Trust Store Path")
  public String sslTrustStorePath = "";

  @Tag(8)
  @DisplayMetadata(label = "SSL Trust Store Password")
  @Secret
  @NotMetadataImpacting
  public String sslTrustStorePassword = "";

  // -----------------------------------------------------------------------
  // Connection pool / timeouts
  // -----------------------------------------------------------------------

  @Tag(9)
  @DisplayMetadata(label = "Max Idle Connections")
  @NotMetadataImpacting
  @Min(1) @Max(100)
  public int maxIdleConnections = 8;

  @Tag(10)
  @DisplayMetadata(label = "Connection Timeout (seconds)")
  @NotMetadataImpacting
  @Min(1) @Max(600)
  public int connectionTimeoutSeconds = 30;

  @Tag(11)
  @DisplayMetadata(label = "Socket Timeout (seconds)")
  @NotMetadataImpacting
  @Min(1) @Max(3600)
  public int socketTimeoutSeconds = 300;

  // -----------------------------------------------------------------------
  // Performance tuning
  // -----------------------------------------------------------------------

  /**
   * Enable HTTP-level LZ4 compression on query results.
   * Reduces network traffic by 3-10x for typical analytical result sets.
   * Disable only if the ClickHouse server is co-located and bandwidth is not a concern.
   */
  @Tag(12)
  @DisplayMetadata(label = "Enable HTTP Result Compression")
  @NotMetadataImpacting
  public boolean enableCompression = true;

  /**
   * Number of rows per block returned by ClickHouse in each HTTP response chunk.
   * Higher values improve throughput for large scans; lower values reduce
   * memory pressure for small/interactive queries.
   * ClickHouse server default is 65536.
   */
  @Tag(13)
  @DisplayMetadata(label = "Fetch Block Size (rows)")
  @NotMetadataImpacting
  @Min(1024) @Max(1000000)
  public int fetchBlockSize = 65536;

  // -----------------------------------------------------------------------
  // Catalog filtering
  // -----------------------------------------------------------------------

  /**
   * Comma-separated list of ClickHouse database names to exclude from the Dremio
   * catalog browser. These databases are hidden in addition to the always-hidden
   * system databases (system, information_schema, _temporary_and_external_tables).
   * Example: "staging,raw_ingest,temp_db"
   */
  @Tag(14)
  @DisplayMetadata(label = "Excluded Databases")
  @NotMetadataImpacting
  public String excludedDatabases = "";

  // -----------------------------------------------------------------------
  // ClickHouse Cloud
  // -----------------------------------------------------------------------

  /**
   * Enable ClickHouse Cloud mode. When on, the connector always uses port 8443
   * and enforces SSL/TLS — the port field and SSL toggle are overridden.
   * Use this when connecting to ClickHouse Cloud (cloud.clickhouse.com).
   */
  @Tag(15)
  @DisplayMetadata(label = "ClickHouse Cloud")
  @NotMetadataImpacting
  public boolean clickHouseCloud = false;

  // -----------------------------------------------------------------------
  // Additional JDBC properties (power-user escape hatch)
  // -----------------------------------------------------------------------

  /**
   * Additional JDBC driver properties to pass to the ClickHouse JDBC driver.
   * One {@code key=value} pair per line (or semicolon-separated).
   * These are merged after all built-in properties, so they can override defaults.
   *
   * <p>Examples:
   * <pre>
   *   session_timezone=UTC
   *   max_memory_usage=10000000000
   *   max_threads=4
   * </pre>
   *
   * <p>See the ClickHouse JDBC driver configuration reference for all available keys:
   * https://github.com/ClickHouse/clickhouse-java/tree/main/clickhouse-jdbc#configuration
   */
  @Tag(16)
  @DisplayMetadata(label = "Additional JDBC Properties")
  @NotMetadataImpacting
  public String additionalJdbcProperties = "";

  // -----------------------------------------------------------------------
  // AbstractArpConf implementation
  // -----------------------------------------------------------------------

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  @Override
  public JdbcPluginConfig buildPluginConfig(
      JdbcPluginConfig.Builder configBuilder,
      CredentialsService credentialsService,
      OptionManager optionManager) {

    JdbcPluginConfig.Builder builder = configBuilder
        .withDialect(getDialect())
        .withDatasourceFactory(this::newDataSource)
        .clearHiddenSchemas()
        // Hide ClickHouse internal system databases from the catalog.
        // Hiding these prevents Dremio from scanning them during metadata refresh,
        // which can be slow when system.* tables are large.
        .addHiddenSchema("system")
        .addHiddenSchema("information_schema")
        .addHiddenSchema("INFORMATION_SCHEMA")
        // _temporary_and_external_tables is a ClickHouse-internal virtual database
        // used for temporary tables; it should never appear in the user catalog.
        .addHiddenSchema("_temporary_and_external_tables");

    // User-defined excluded databases (comma-separated list from UI)
    if (excludedDatabases != null && !excludedDatabases.trim().isEmpty()) {
      for (String db : excludedDatabases.split(",")) {
        String trimmed = db.trim();
        if (!trimmed.isEmpty()) {
          builder = builder.addHiddenSchema(trimmed);
        }
      }
    }

    return builder.build();
  }

  /**
   * Factory method implementing {@link CloseableDataSource.Factory}.
   * Must declare {@code throws SQLException} to match the interface.
   */
  private CloseableDataSource newDataSource() throws SQLException {
    return DataSources.newGenericConnectionPoolDataSource(
        DRIVER,
        toJdbcConnectionString(),
        username,
        password,
        buildDriverProperties(),
        DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE,
        maxIdleConnections,
        // DataSources expects connection timeout as milliseconds (long)
        (long) connectionTimeoutSeconds * 1000L);
  }

  private Properties buildDriverProperties() {
    Properties props = new Properties();

    // --- Timeouts ---
    // Socket timeout (milliseconds) — how long the client waits for a response chunk
    props.setProperty("socket_timeout", String.valueOf((long) socketTimeoutSeconds * 1000L));
    // Server-side execution timeout: kill the query on the server if it runs too long.
    // Set 10 s shorter than socket_timeout so the server cancels cleanly before the
    // client times out and the socket is left in a bad state. Minimum 10 s.
    int serverTimeoutSec = Math.max(10, socketTimeoutSeconds - 10);
    props.setProperty("max_execution_time", String.valueOf(serverTimeoutSec));
    // Keep HTTP connections alive between requests (60 s)
    props.setProperty("keep_alive_timeout", "60000");

    // --- Performance ---
    // LZ4 HTTP result compression.  Reduces typical analytical payloads by 3-10x.
    props.setProperty("compress", enableCompression ? "1" : "0");
    // Number of rows per response block (server default: 65536).
    props.setProperty("max_block_size", String.valueOf(fetchBlockSize));

    // --- SSL / TLS ---
    // ClickHouse Cloud always requires SSL; the clickHouseCloud flag implies useSsl.
    if (useSsl || clickHouseCloud) {
      props.setProperty("ssl", "true");
      props.setProperty("sslmode", "strict");
      if (sslTrustStorePath != null && !sslTrustStorePath.isEmpty()) {
        props.setProperty("sslrootcert", sslTrustStorePath);
      }
    }

    // --- Additional JDBC properties (power-user override, applied last) ---
    // Parsed as key=value pairs separated by newlines or semicolons.
    // Applied after all built-in properties so they can override any default.
    if (additionalJdbcProperties != null && !additionalJdbcProperties.trim().isEmpty()) {
      for (String pair : additionalJdbcProperties.split("[;\n]")) {
        String trimmed = pair.trim();
        int eq = trimmed.indexOf('=');
        if (eq > 0) {
          String key = trimmed.substring(0, eq).trim();
          String val = trimmed.substring(eq + 1).trim();
          if (!key.isEmpty()) {
            props.setProperty(key, val);
          }
        }
      }
    }

    return props;
  }

  /**
   * Builds the JDBC connection URL for the ClickHouse HTTP interface.
   *
   * <p>Format: {@code jdbc:clickhouse://host:port/database}
   *
   * <p>Auth is passed separately via the datasource factory args — not embedded
   * in the URL — so credentials are not captured in logs.
   */
  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String h = Preconditions.checkNotNull(host, "Host is required").trim();
    final String db = (database != null && !database.trim().isEmpty())
        ? database.trim() : "default";
    // ClickHouse Cloud always runs on port 8443 (HTTPS). Override the port field
    // when Cloud mode is enabled so users don't have to remember to change it.
    final int effectivePort = clickHouseCloud ? 8443 : port;
    return String.format("jdbc:clickhouse://%s:%d/%s", h, effectivePort, db);
  }
}
