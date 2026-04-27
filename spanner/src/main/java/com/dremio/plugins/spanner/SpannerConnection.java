package com.dremio.plugins.spanner;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.DatabaseId;
import com.google.cloud.spanner.ReadOnlyTransaction;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Spanner;
import com.google.cloud.spanner.SpannerOptions;
import com.google.cloud.spanner.Statement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Wraps the Spanner Java client for Dremio connector use.
 *
 * Manages one {@link Spanner} instance (gRPC channel pool) per plugin lifecycle.
 * Call {@link #close()} when the plugin shuts down.
 *
 * Authentication:
 *   - If {@code credentialsFile} is non-empty, loads a service account key JSON.
 *   - Otherwise falls back to Application Default Credentials
 *     (GOOGLE_APPLICATION_CREDENTIALS env var, gcloud auth, Workload Identity, etc.).
 *
 * Emulator:
 *   Set SPANNER_EMULATOR_HOST=localhost:9010 before starting Dremio to route
 *   all requests to a local Spanner emulator (anonymous credentials used automatically).
 */
public class SpannerConnection implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SpannerConnection.class);

  private final SpannerStoragePluginConfig config;
  private Spanner spanner;
  private DatabaseClient dbClient;

  public SpannerConnection(SpannerStoragePluginConfig config) {
    this.config = config;
  }

  /** Opens the gRPC channel pool and validates connectivity. */
  public void open() throws Exception {
    SpannerOptions.Builder builder = SpannerOptions.newBuilder()
        .setProjectId(config.project);

    String emulatorHost = System.getenv("SPANNER_EMULATOR_HOST");
    if (emulatorHost != null && !emulatorHost.isEmpty()) {
      // Emulator uses anonymous credentials and plain HTTP; the client SDK
      // picks up SPANNER_EMULATOR_HOST automatically.
      logger.info("Spanner emulator detected at {}", emulatorHost);
    } else if (config.credentialsFile != null && !config.credentialsFile.isEmpty()) {
      try (FileInputStream fis = new FileInputStream(config.credentialsFile)) {
        builder.setCredentials(ServiceAccountCredentials.fromStream(fis));
      }
      logger.info("Spanner using service account credentials from {}", config.credentialsFile);
    } else {
      logger.info("Spanner using Application Default Credentials");
    }

    spanner  = builder.build().getService();
    dbClient = spanner.getDatabaseClient(
        DatabaseId.of(config.project, config.instance, config.database));

    logger.info("Spanner connected: project={} instance={} database={}",
        config.project, config.instance, config.database);
  }

  // -----------------------------------------------------------------------
  // Schema discovery
  // -----------------------------------------------------------------------

  /**
   * Returns all user-table names in the database (excludes INFORMATION_SCHEMA tables).
   */
  public List<String> listTables() {
    List<String> tables = new ArrayList<>();
    try (ReadOnlyTransaction txn = dbClient.singleUseReadOnlyTransaction();
         ResultSet rs = txn.executeQuery(Statement.of(
             "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES " +
             "WHERE TABLE_SCHEMA = '' ORDER BY TABLE_NAME"))) {
      while (rs.next()) {
        tables.add(rs.getString(0));
      }
    }
    return tables;
  }

  /**
   * Returns column metadata for a given table as a list of {@link SpannerColumnInfo}.
   * Column order follows ORDINAL_POSITION.
   */
  public List<SpannerColumnInfo> getColumns(String tableName) {
    List<SpannerColumnInfo> columns = new ArrayList<>();
    try (ReadOnlyTransaction txn = dbClient.singleUseReadOnlyTransaction();
         ResultSet rs = txn.executeQuery(Statement.newBuilder(
                 "SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE " +
                 "FROM INFORMATION_SCHEMA.COLUMNS " +
                 "WHERE TABLE_NAME = @t AND TABLE_SCHEMA = '' " +
                 "ORDER BY ORDINAL_POSITION")
             .bind("t").to(tableName)
             .build())) {
      while (rs.next()) {
        columns.add(new SpannerColumnInfo(
            rs.getString("COLUMN_NAME"),
            rs.getString("SPANNER_TYPE"),
            "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"))));
      }
    }
    return columns;
  }

  /**
   * Returns the primary key column names for a table in key order.
   */
  public List<String> getPrimaryKeys(String tableName) {
    List<String> pks = new ArrayList<>();
    try (ReadOnlyTransaction txn = dbClient.singleUseReadOnlyTransaction();
         ResultSet rs = txn.executeQuery(Statement.newBuilder(
                 "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS " +
                 "WHERE TABLE_NAME = @t AND INDEX_NAME = 'PRIMARY_KEY' " +
                 "ORDER BY ORDINAL_POSITION")
             .bind("t").to(tableName)
             .build())) {
      while (rs.next()) {
        pks.add(rs.getString(0));
      }
    }
    return pks;
  }

  /**
   * Executes a SQL query and returns the open {@link ResultSet}.
   * Caller is responsible for closing the ResultSet.
   */
  public ResultSet executeQuery(String sql) {
    return dbClient.singleUse().executeQuery(Statement.of(sql));
  }

  /**
   * Executes a parameterized SQL query and returns the open {@link ResultSet}.
   */
  public ResultSet executeQuery(Statement stmt) {
    return dbClient.singleUse().executeQuery(stmt);
  }

  /**
   * Returns the approximate row count for a table (from INFORMATION_SCHEMA stats).
   * May be 0 if statistics haven't been computed yet.
   */
  public long getApproximateRowCount(String tableName) {
    try (ReadOnlyTransaction txn = dbClient.singleUseReadOnlyTransaction();
         ResultSet rs = txn.executeQuery(Statement.newBuilder(
                 "SELECT ROW_COUNT FROM INFORMATION_SCHEMA.TABLE_STATISTICS " +
                 "WHERE TABLE_NAME = @t AND TABLE_SCHEMA = ''")
             .bind("t").to(tableName)
             .build())) {
      if (rs.next()) {
        return rs.getLong(0);
      }
    } catch (Exception e) {
      logger.debug("Could not read TABLE_STATISTICS for {}: {}", tableName, e.getMessage());
    }
    return 0;
  }

  // -----------------------------------------------------------------------
  // Accessor
  // -----------------------------------------------------------------------

  public DatabaseClient getDatabaseClient() { return dbClient; }

  // -----------------------------------------------------------------------
  // AutoCloseable
  // -----------------------------------------------------------------------

  @Override
  public void close() {
    if (spanner != null && !spanner.isClosed()) {
      spanner.close();
      logger.info("Spanner connection closed");
    }
  }

  // -----------------------------------------------------------------------
  // Inner type
  // -----------------------------------------------------------------------

  /** Lightweight column descriptor returned by {@link #getColumns}. */
  public static final class SpannerColumnInfo {
    public final String name;
    public final String spannerType;
    public final boolean nullable;

    public SpannerColumnInfo(String name, String spannerType, boolean nullable) {
      this.name        = name;
      this.spannerType = spannerType;
      this.nullable    = nullable;
    }
  }
}
