package com.dremio.plugins.singlestore;

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
 * Source configuration for the Dremio SingleStore Connector.
 *
 * <p>Uses Dremio's ARP (Advanced Relational Pushdown) framework over the official
 * SingleStore JDBC driver ({@code com.singlestore:singlestore-jdbc-client}).
 * The ARP YAML dialect file ({@code singlestore-arp.yaml}) drives all SQL translation,
 * predicate pushdown, aggregation pushdown, and function mapping.</p>
 *
 * <p>Connects to SingleStore on the standard port 3306 (TCP).</p>
 */
@SourceType(value = "SINGLESTORE", label = "SingleStore", uiConfig = "singlestore-layout.json")
public class SingleStoreConf extends AbstractArpConf<SingleStoreConf> {

  private static final String ARP_FILENAME = "arp/implementation/singlestore-arp.yaml";
  private static final String DRIVER = "com.singlestore.jdbc.Driver";

  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, SingleStoreDialect::new);

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
  public int port = 3306;

  @Tag(3)
  @DisplayMetadata(label = "Database")
  public String database = "";

  @Tag(4)
  @DisplayMetadata(label = "Username")
  @NotMetadataImpacting
  public String username = "root";

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
  // Catalog filtering
  // -----------------------------------------------------------------------

  @Tag(12)
  @DisplayMetadata(label = "Excluded Databases")
  @NotMetadataImpacting
  public String excludedDatabases = "";

  // -----------------------------------------------------------------------
  // Additional JDBC properties
  // -----------------------------------------------------------------------

  @Tag(13)
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
        // Hide SingleStore system databases from the catalog.
        .addHiddenSchema("information_schema")
        .addHiddenSchema("memsql")
        .addHiddenSchema("cluster");

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

  private CloseableDataSource newDataSource() throws SQLException {
    return DataSources.newGenericConnectionPoolDataSource(
        DRIVER,
        toJdbcConnectionString(),
        username,
        password,
        buildDriverProperties(),
        DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE,
        maxIdleConnections,
        (long) connectionTimeoutSeconds * 1000L);
  }

  private Properties buildDriverProperties() {
    Properties props = new Properties();

    props.setProperty("socketTimeout", String.valueOf((long) socketTimeoutSeconds * 1000L));
    props.setProperty("sessionVariables", "time_zone='+00:00'");

    if (useSsl) {
      props.setProperty("sslMode", "VERIFY_CA");
      if (sslTrustStorePath != null && !sslTrustStorePath.isEmpty()) {
        props.setProperty("trustStore", sslTrustStorePath);
        if (sslTrustStorePassword != null && !sslTrustStorePassword.isEmpty()) {
          props.setProperty("trustStorePassword", sslTrustStorePassword);
        }
      }
    }

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

  @VisibleForTesting
  public String toJdbcConnectionString() {
    final String h = Preconditions.checkNotNull(host, "Host is required").trim();
    final String db = (database != null && !database.trim().isEmpty())
        ? "/" + database.trim() : "";
    return String.format("jdbc:singlestore://%s:%d%s", h, port, db);
  }
}
