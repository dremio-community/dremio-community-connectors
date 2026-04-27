package com.dremio.plugins.spanner;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetSplit;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.EntityPath;
import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.PartitionChunk;
import com.dremio.connector.metadata.PartitionChunkListing;
import com.dremio.connector.metadata.extensions.SupportsListingDatasets;
import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.store.StoragePlugin;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.dremio.plugins.spanner.planning.SpannerRulesFactory;
import com.dremio.plugins.spanner.scan.SpannerDatasetHandle;
import com.dremio.plugins.spanner.scan.SpannerDatasetMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dremio storage plugin for Google Cloud Spanner.
 *
 * Implements StoragePlugin (lifecycle + metadata API) and SupportsListingDatasets
 * (table enumeration for the Dremio catalog browser).
 *
 * Architecture:
 *   SpannerStoragePluginConfig.newPlugin() → SpannerStoragePlugin
 *   start()               → opens SpannerConnection (gRPC channel pool)
 *   listDatasetHandles()  → queries INFORMATION_SCHEMA.TABLES
 *   getDatasetMetadata()  → queries INFORMATION_SCHEMA.COLUMNS, builds Arrow schema
 *   listPartitionChunks() → creates N splits for parallel reads
 *   SpannerRulesFactory   → planning rules
 *   SpannerScanCreator    → execution
 */
public class SpannerStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(SpannerStoragePlugin.class);

  private final SpannerStoragePluginConfig config;
  private final PluginSabotContext context;
  private final String name;
  private SpannerConnection connection;

  /** Schema cache: table → (BatchSchema, expiry). Avoids re-querying INFORMATION_SCHEMA. */
  private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();

  private static final class CachedSchema {
    final BatchSchema schema;
    final long expiresAt;

    CachedSchema(BatchSchema schema, long ttlMs) {
      this.schema    = schema;
      this.expiresAt = System.currentTimeMillis() + ttlMs;
    }

    boolean isExpired() { return System.currentTimeMillis() > expiresAt; }
  }

  public SpannerStoragePlugin(SpannerStoragePluginConfig config,
                               PluginSabotContext context,
                               String name) {
    this.config  = config;
    this.context = context;
    this.name    = name;
  }

  // -----------------------------------------------------------------------
  // StoragePlugin lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void start() throws IOException {
    try {
      connection = new SpannerConnection(config);
      connection.open();
      // Quick validation — list tables (cheap metadata read)
      connection.listTables();
      logger.info("Spanner connector started: project={} instance={} database={}",
          config.project, config.instance, config.database);
    } catch (Exception e) {
      throw new IOException("Failed to connect to Spanner: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception {
    if (connection != null) {
      connection.close();
    }
  }

  // -----------------------------------------------------------------------
  // StoragePlugin status + capabilities
  // -----------------------------------------------------------------------

  @Override
  public SourceState getState() {
    if (connection == null) {
      return SourceState.badState("Spanner connection not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      connection.listTables();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("Spanner health check failed ({}), attempting reconnect...", e.getMessage());
      try {
        connection.close();
        connection = new SpannerConnection(config);
        connection.open();
        connection.listTables();
        logger.info("Spanner reconnection successful");
        return SourceState.goodState();
      } catch (Exception reconnectEx) {
        return SourceState.badState(
            "Spanner connection lost and reconnect failed: " + reconnectEx.getMessage(),
            reconnectEx);
      }
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return SpannerRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true; // Access control handled by GCP IAM / Spanner roles
  }

  // -----------------------------------------------------------------------
  // SupportsListingDatasets: table enumeration
  // -----------------------------------------------------------------------

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {
    try {
      List<String> tables = connection.listTables();
      List<DatasetHandle> handles = new ArrayList<>(tables.size());
      for (String table : tables) {
        EntityPath path = new EntityPath(Arrays.asList(name, table));
        handles.add(new SpannerDatasetHandle(path));
      }
      logger.debug("Listed {} Spanner tables from source '{}'", handles.size(), name);
      return () -> handles.iterator();
    } catch (Exception e) {
      throw new ConnectorException("Failed to list Spanner tables: " + e.getMessage(), e);
    }
  }

  // -----------------------------------------------------------------------
  // SourceMetadata: dataset lookup and metadata
  // -----------------------------------------------------------------------

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
      throws ConnectorException {
    List<String> components = path.getComponents();
    if (components.size() < 2) return Optional.empty();
    String tableName = components.get(components.size() - 1);
    try {
      List<String> tables = connection.listTables();
      if (!tables.contains(tableName)) return Optional.empty();
      return Optional.of(new SpannerDatasetHandle(path));
    } catch (Exception e) {
      throw new ConnectorException("Failed to look up Spanner table: " + tableName, e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {
    String tableName = ((SpannerDatasetHandle) handle).getTableName();
    long ttlMs = config.metadataCacheTtlSeconds * 1000L;

    // Serve from cache if fresh
    if (ttlMs > 0) {
      CachedSchema cached = schemaCache.get(tableName);
      if (cached != null && !cached.isExpired()) {
        long rows = connection.getApproximateRowCount(tableName);
        logger.debug("Schema cache hit for '{}': {} fields", tableName,
            cached.schema.getFieldCount());
        return new SpannerDatasetMetadata(cached.schema, DatasetStats.of(rows, 1.0), tableName);
      }
    }

    try {
      List<SpannerConnection.SpannerColumnInfo> cols = connection.getColumns(tableName);
      if (cols.isEmpty()) {
        throw new ConnectorException("No columns found for Spanner table: " + tableName);
      }

      List<Field> fields = new ArrayList<>(cols.size());
      for (SpannerConnection.SpannerColumnInfo col : cols) {
        fields.add(SpannerTypeConverter.toArrowField(col.name, col.spannerType, col.nullable));
      }
      BatchSchema schema = new BatchSchema(fields);
      long rows = connection.getApproximateRowCount(tableName);

      logger.debug("Resolved schema for '{}': {} columns, ~{} rows",
          tableName, fields.size(), rows);

      if (ttlMs > 0) {
        schemaCache.put(tableName, new CachedSchema(schema, ttlMs));
      }
      return new SpannerDatasetMetadata(schema, DatasetStats.of(rows, 1.0), tableName);
    } catch (ConnectorException ce) {
      throw ce;
    } catch (Exception e) {
      throw new ConnectorException("Failed to read schema for Spanner table: " + tableName, e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    String tableName = ((SpannerDatasetHandle) handle).getTableName();
    int parallelism = Math.max(1, config.splitParallelism);
    long estimatedRows = connection.getApproximateRowCount(tableName);
    long rowsPerSplit  = Math.max(1, (estimatedRows > 0 ? estimatedRows : 10_000) / parallelism);
    long sizePerSplit  = Math.max(1024L, rowsPerSplit * 200L); // ~200 bytes/row estimate

    List<PartitionChunk> chunks = new ArrayList<>(parallelism);
    for (int seg = 0; seg < parallelism; seg++) {
      // Encode: "tableName|segment|totalSegments"
      String specStr  = tableName + "|" + seg + "|" + parallelism;
      byte[] specBytes = specStr.getBytes(StandardCharsets.UTF_8);
      DatasetSplit split = DatasetSplit.of(
          Collections.emptyList(),
          sizePerSplit, rowsPerSplit,
          os -> os.write(specBytes));
      chunks.add(PartitionChunk.of(split));
    }
    return () -> chunks.iterator();
  }

  @Override
  public boolean containerExists(EntityPath path, GetMetadataOption... options) {
    return false; // Spanner has no sub-container concept between database and table
  }

  // -----------------------------------------------------------------------
  // Accessors for planning and execution layers
  // -----------------------------------------------------------------------

  public SpannerConnection getConnection() { return connection; }
  public SpannerStoragePluginConfig getConfig() { return config; }

  /** Invalidates the schema cache for a table (e.g. after a DDL change). */
  public void invalidateSchemaCache(String tableName) {
    schemaCache.remove(tableName);
  }
}
