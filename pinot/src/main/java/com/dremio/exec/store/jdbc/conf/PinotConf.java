/*
 * Copyright (C) 2024 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.exec.store.jdbc.conf;

import com.dremio.exec.catalog.conf.DisplayMetadata;
import com.dremio.exec.catalog.conf.NotMetadataImpacting;
import com.dremio.exec.catalog.conf.Secret;
import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.store.jdbc.CloseableDataSource;
import com.dremio.exec.store.jdbc.DataSources;
import com.dremio.exec.store.jdbc.JdbcPluginConfig;
import com.dremio.exec.store.jdbc.dialect.arp.ArpDialect;
import com.dremio.options.OptionManager;
import com.dremio.services.credentials.CredentialsService;
import com.google.common.annotations.VisibleForTesting;
import io.protostuff.Tag;

/**
 * Dremio source configuration for Apache Pinot.
 *
 * Exposes Pinot tables via the ARP (Advanced Relational Pushdown) framework.
 * Connects to the Pinot Controller REST API using the official Pinot JDBC driver.
 *
 * Pushdown: aggregations, projections, filter predicates, ORDER BY, LIMIT.
 * JOINs and subqueries are handled by Dremio's query planner.
 */
@SourceType(value = "PINOT", label = "Apache Pinot", uiConfig = "pinot-layout.json", externalQuerySupported = true)
public class PinotConf extends AbstractArpConf<PinotConf> {

  private static final String ARP_FILENAME = "arp/implementation/pinot-arp.yaml";
  private static final ArpDialect ARP_DIALECT =
      AbstractArpConf.loadArpFile(ARP_FILENAME, (ArpDialect::new));
  private static final String DRIVER = "org.apache.pinot.client.PinotDriver";

  // -----------------------------------------------------------------------
  // Connection
  // -----------------------------------------------------------------------

  /** Pinot Controller hostname or IP. */
  @Tag(1)
  @DisplayMetadata(label = "Controller Host")
  public String host = "localhost";

  /**
   * Pinot Controller REST API port.
   * Default: 9000 (HTTP). Use 443 or 9443 for TLS deployments.
   */
  @Tag(2)
  @DisplayMetadata(label = "Controller Port")
  public int port = 9000;

  // -----------------------------------------------------------------------
  // Authentication
  // -----------------------------------------------------------------------

  /**
   * Pinot username (for basic authentication).
   * Leave blank for unauthenticated clusters.
   */
  @Tag(3)
  @DisplayMetadata(label = "Username (optional)")
  public String username = "";

  /**
   * Pinot password (for basic authentication).
   * Leave blank for unauthenticated clusters.
   */
  @Tag(4)
  @Secret
  @DisplayMetadata(label = "Password (optional)")
  public String password = "";

  // -----------------------------------------------------------------------
  // Connection options
  // -----------------------------------------------------------------------

  /**
   * Enable TLS for the JDBC connection to the Pinot controller.
   * When true, uses the jdbc:pinot+ssl:// URL scheme.
   * Ensure your Pinot controller is configured for HTTPS.
   */
  @Tag(5)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Use TLS / HTTPS")
  public boolean useTls = false;

  /**
   * Optional comma-separated list of Pinot broker addresses (host:port).
   * When set, Dremio routes queries directly to the specified brokers rather
   * than relying on the controller to discover them.
   * Example: broker1:8099,broker2:8099
   */
  @Tag(6)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Broker List (optional, host:port,...)")
  public String brokerList = "";

  // -----------------------------------------------------------------------
  // Performance
  // -----------------------------------------------------------------------

  /** Number of rows to fetch per page when reading query results. */
  @Tag(7)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Record Fetch Size")
  public int fetchSize = 500;

  /** Maximum number of idle JDBC connections to keep in the pool. */
  @Tag(8)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Maximum Idle Connections")
  public int maxIdleConns = 8;

  /** How long (seconds) an idle connection is kept before being closed. */
  @Tag(9)
  @NotMetadataImpacting
  @DisplayMetadata(label = "Connection Idle Time (seconds)")
  public int idleTimeSec = 60;

  // -----------------------------------------------------------------------
  // JDBC URL construction
  // -----------------------------------------------------------------------

  @VisibleForTesting
  public String toJdbcConnectionString() {
    String scheme = useTls ? "jdbc:pinot+ssl" : "jdbc:pinot";
    StringBuilder url = new StringBuilder(
        String.format("%s://%s:%d", scheme, host, port));
    if (brokerList != null && !brokerList.isBlank()) {
      url.append("?brokerList=").append(brokerList.trim());
    }
    return url.toString();
  }

  // -----------------------------------------------------------------------
  // AbstractArpConf overrides
  // -----------------------------------------------------------------------

  @Override
  @VisibleForTesting
  public JdbcPluginConfig buildPluginConfig(
      JdbcPluginConfig.Builder configBuilder,
      CredentialsService credentialsService,
      OptionManager optionManager) {
    return configBuilder
        .withDialect(getDialect())
        .withFetchSize(fetchSize)
        .withDatasourceFactory(this::newDataSource)
        .build();
  }

  private CloseableDataSource newDataSource() {
    String user = (username != null && !username.isBlank()) ? username : null;
    String pass = (password != null && !password.isBlank()) ? password : null;
    return DataSources.newGenericConnectionPoolDataSource(
        DRIVER,
        toJdbcConnectionString(),
        user, pass,
        null,
        DataSources.CommitMode.DRIVER_SPECIFIED_COMMIT_MODE,
        maxIdleConns, idleTimeSec);
  }

  @Override
  public ArpDialect getDialect() {
    return ARP_DIALECT;
  }

  @VisibleForTesting
  public static ArpDialect getDialectSingleton() {
    return ARP_DIALECT;
  }
}
