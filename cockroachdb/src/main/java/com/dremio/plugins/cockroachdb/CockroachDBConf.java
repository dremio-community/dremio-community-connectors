package com.dremio.plugins.cockroachdb;

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

@SourceType(value = "COCKROACHDB", label = "CockroachDB", uiConfig = "cockroachdb-layout.json")
public class CockroachDBConf extends AbstractArpConf<CockroachDBConf> {

  private static final String ARP_FILENAME = "arp/implementation/cockroachdb-arp.yaml";
  private static final String DRIVER = "org.postgresql.Driver";

  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, CockroachDBDialect::new);

  @Tag(1)
  @DisplayMetadata(label = "Host")
  @NotBlank
  public String host = "localhost";

  @Tag(2)
  @DisplayMetadata(label = "Port")
  @Min(1) @Max(65535)
  public int port = 26257;

  @Tag(3)
  @DisplayMetadata(label = "Database")
  public String database = "defaultdb";

  @Tag(4)
  @DisplayMetadata(label = "Username")
  @NotMetadataImpacting
  public String username = "root";

  @Tag(5)
  @DisplayMetadata(label = "Password")
  @Secret
  @NotMetadataImpacting
  public String password = "";

  @Tag(6)
  @DisplayMetadata(label = "Enable SSL / TLS")
  public boolean useSsl = false;

  @Tag(7)
  @DisplayMetadata(label = "SSL Mode")
  @NotMetadataImpacting
  public String sslMode = "require";

  @Tag(8)
  @DisplayMetadata(label = "SSL Root Cert Path")
  public String sslRootCertPath = "";

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

  @Tag(12)
  @DisplayMetadata(label = "Excluded Schemas")
  @NotMetadataImpacting
  public String excludedSchemas = "";

  @Tag(13)
  @DisplayMetadata(label = "Additional JDBC Properties")
  @NotMetadataImpacting
  public String additionalJdbcProperties = "";

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
        // Hide CockroachDB/PostgreSQL system schemas from the catalog.
        .addHiddenSchema("crdb_internal")
        .addHiddenSchema("pg_catalog")
        .addHiddenSchema("pg_extension")
        .addHiddenSchema("information_schema");

    if (excludedSchemas != null && !excludedSchemas.trim().isEmpty()) {
      for (String schema : excludedSchemas.split(",")) {
        String trimmed = schema.trim();
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

    props.setProperty("socketTimeout", String.valueOf(socketTimeoutSeconds));
    props.setProperty("connectTimeout", String.valueOf(connectionTimeoutSeconds));
    props.setProperty("ApplicationName", "dremio");

    if (useSsl) {
      props.setProperty("sslmode", sslMode != null && !sslMode.isEmpty() ? sslMode : "require");
      if (sslRootCertPath != null && !sslRootCertPath.isEmpty()) {
        props.setProperty("sslrootcert", sslRootCertPath);
      }
    } else {
      props.setProperty("sslmode", "disable");
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
        ? "/" + database.trim() : "/defaultdb";
    return String.format("jdbc:postgresql://%s:%d%s", h, port, db);
  }
}
