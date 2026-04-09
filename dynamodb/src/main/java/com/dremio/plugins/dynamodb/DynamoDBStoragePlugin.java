package com.dremio.plugins.dynamodb;

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
import com.dremio.plugins.dynamodb.planning.DynamoDBRulesFactory;
import com.dremio.plugins.dynamodb.scan.DynamoDBDatasetHandle;
import com.dremio.plugins.dynamodb.scan.DynamoDBDatasetMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Dremio storage plugin for Amazon DynamoDB.
 *
 * Implements StoragePlugin (lifecycle + metadata API) and SupportsListingDatasets
 * (table enumeration for the Dremio catalog browser).
 *
 * Architecture:
 *   DynamoDBStoragePluginConfig.newPlugin() → DynamoDBStoragePlugin
 *   start() → opens DynamoDBConnection (DynamoDbClient)
 *   listDatasetHandles() → lists all DynamoDB tables
 *   getDatasetMetadata() → samples items, infers Arrow schema, caches result
 *   DynamoDBRulesFactory → planning rules
 *   DynamoDBScanCreator → execution
 */
public class DynamoDBStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDBStoragePlugin.class);

  private final DynamoDBStoragePluginConfig config;
  private final PluginSabotContext context;
  private final String name;
  private DynamoDBConnection connection;

  // Schema metadata cache — avoids re-sampling on every metadata refresh
  private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();

  /** Cached schema entry with TTL. */
  private static final class CachedSchema {
    final BatchSchema schema;
    final String pkName;
    final String skName;
    final long expiresAt;

    CachedSchema(BatchSchema schema, String pkName, String skName, long ttlMs) {
      this.schema    = schema;
      this.pkName    = pkName;
      this.skName    = skName;
      this.expiresAt = System.currentTimeMillis() + ttlMs;
    }

    boolean isExpired() { return System.currentTimeMillis() > expiresAt; }
  }

  public DynamoDBStoragePlugin(DynamoDBStoragePluginConfig config,
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
      connection = new DynamoDBConnection(config);
      // Validate connection by listing tables (cheap operation)
      connection.listTables();
      logger.info("DynamoDB connector started for region={}, endpoint={}",
          config.region,
          (config.endpointOverride != null && !config.endpointOverride.isEmpty())
              ? config.endpointOverride : "AWS default");
    } catch (Exception e) {
      throw new IOException("Failed to connect to DynamoDB: " + e.getMessage(), e);
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
      return SourceState.badState("DynamoDB connection not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      connection.listTables();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("DynamoDB health check failed ({}), attempting reconnect...", e.getMessage());
      try {
        connection.close();
        connection = new DynamoDBConnection(config);
        connection.listTables();
        logger.info("DynamoDB reconnection successful");
        return SourceState.goodState();
      } catch (Exception reconnectEx) {
        return SourceState.badState(
            "DynamoDB connection lost and reconnect failed: " + reconnectEx.getMessage(),
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
    return DynamoDBRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true; // Access control handled by AWS IAM
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
        handles.add(new DynamoDBDatasetHandle(path));
      }
      logger.debug("Listed {} DynamoDB tables from source '{}'", handles.size(), name);
      return () -> handles.iterator();
    } catch (Exception e) {
      throw new ConnectorException("Failed to list DynamoDB tables: " + e.getMessage(), e);
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
      return Optional.of(new DynamoDBDatasetHandle(path));
    } catch (Exception e) {
      throw new ConnectorException("Failed to look up DynamoDB table: " + tableName, e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {
    String tableName = ((DynamoDBDatasetHandle) handle).getTableName();
    long ttlMs = config.metadataCacheTtlSeconds * 1000L;

    // Serve from cache if fresh
    if (ttlMs > 0) {
      CachedSchema cached = schemaCache.get(tableName);
      if (cached != null && !cached.isExpired()) {
        long estimatedRows = connection.getApproximateItemCount(tableName);
        logger.debug("Schema cache hit for '{}': {} fields (from cache)",
            tableName, cached.schema.getFieldCount());
        return new DynamoDBDatasetMetadata(cached.schema,
            DatasetStats.of(estimatedRows, 1.0), tableName, cached.pkName, cached.skName);
      }
    }

    try {
      // Look up table key names (partition key + optional sort key)
      String pkName = connection.getPartitionKeyName(tableName);
      String skName = connection.getSortKeyName(tableName);

      // Sample items and infer schema
      List<Map<String, AttributeValue>> samples =
          connection.sampleItems(tableName, config.sampleSize);
      List<Field> fields = DynamoDBTypeConverter.inferFields(samples);
      BatchSchema schema = new BatchSchema(fields);
      long estimatedRows = connection.getApproximateItemCount(tableName);

      logger.debug("Inferred schema for '{}': {} fields, ~{} rows from {} samples, pk='{}', sk='{}'",
          tableName, fields.size(), estimatedRows, samples.size(), pkName, skName);

      // Cache the result
      if (ttlMs > 0) {
        schemaCache.put(tableName, new CachedSchema(schema, pkName, skName, ttlMs));
      }

      return new DynamoDBDatasetMetadata(schema, DatasetStats.of(estimatedRows, 1.0),
          tableName, pkName, skName);
    } catch (Exception e) {
      throw new ConnectorException("Failed to infer schema for DynamoDB table: " + tableName, e);
    }
  }

  /** Invalidates the schema cache for a single table (e.g. after detecting a schema change). */
  public void invalidateSchemaCache(String tableName) {
    schemaCache.remove(tableName);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    String tableName = ((DynamoDBDatasetHandle) handle).getTableName();
    int parallelism = Math.max(1, config.splitParallelism);
    long estimatedRows = connection.getApproximateItemCount(tableName);
    long rowsPerSplit  = Math.max(1, estimatedRows / parallelism);
    long sizePerSplit  = Math.max(1024L, rowsPerSplit * 100L); // ~100 bytes/row estimate

    List<PartitionChunk> chunks = new ArrayList<>(parallelism);
    for (int seg = 0; seg < parallelism; seg++) {
      // Encode: "tableName|segment|totalSegments"
      String specStr = tableName + "|" + seg + "|" + parallelism;
      byte[] specBytes = specStr.getBytes(StandardCharsets.UTF_8);
      DatasetSplit split = DatasetSplit.of(
          java.util.Collections.emptyList(),
          sizePerSplit, rowsPerSplit,
          os -> os.write(specBytes));
      chunks.add(PartitionChunk.of(split));
    }
    return () -> chunks.iterator();
  }

  @Override
  public boolean containerExists(EntityPath path, GetMetadataOption... options) {
    // DynamoDB has no container concept (no keyspace); always return false
    return false;
  }

  // -----------------------------------------------------------------------
  // Accessors for planning and execution layers
  // -----------------------------------------------------------------------

  public DynamoDBConnection getConnection() {
    return connection;
  }

  public DynamoDBStoragePluginConfig getConfig() {
    return config;
  }
}
