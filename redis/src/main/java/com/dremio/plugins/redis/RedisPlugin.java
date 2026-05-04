package com.dremio.plugins.redis;

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
import com.dremio.plugins.redis.planning.RedisRulesFactory;
import com.dremio.plugins.redis.scan.RedisDatasetHandle;
import com.dremio.plugins.redis.scan.RedisDatasetMetadata;
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
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class RedisPlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(RedisPlugin.class);

  private final RedisConf config;
  private final PluginSabotContext context;
  private final String name;
  private RedisConnection connection;

  private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();
  private static final long CACHE_TTL_MS = 60_000L;

  private static final class CachedSchema {
    final BatchSchema schema;
    final long expiresAt;
    CachedSchema(BatchSchema schema) {
      this.schema    = schema;
      this.expiresAt = System.currentTimeMillis() + CACHE_TTL_MS;
    }
    boolean isExpired() { return System.currentTimeMillis() > expiresAt; }
  }

  public RedisPlugin(RedisConf config, PluginSabotContext context, String name) {
    this.config  = config;
    this.context = context;
    this.name    = name;
  }

  @Override
  public void start() throws IOException {
    try {
      connection = new RedisConnection(config);
      connection.connect();
    } catch (Exception e) {
      throw new IOException("Failed to connect to Redis: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception {
    if (connection != null) connection.close();
  }

  @Override
  public SourceState getState() {
    if (connection == null) {
      return SourceState.badState("Redis connection not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      connection.listTables();
      return SourceState.goodState();
    } catch (Exception e) {
      try {
        connection.close();
        connection = new RedisConnection(config);
        connection.connect();
        return SourceState.goodState();
      } catch (Exception ex) {
        return SourceState.badState("Redis reconnect failed: " + ex.getMessage(), ex);
      }
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return RedisRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {
    try {
      List<String> tables = connection.listTables();
      List<DatasetHandle> handles = new ArrayList<>(tables.size());
      for (String table : tables) {
        EntityPath path = new EntityPath(Arrays.asList(name, table));
        handles.add(new RedisDatasetHandle(path));
      }
      return () -> handles.iterator();
    } catch (Exception e) {
      throw new ConnectorException("Failed to list Redis tables: " + e.getMessage(), e);
    }
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
      throws ConnectorException {
    List<String> components = path.getComponents();
    if (components.size() < 2) return Optional.empty();
    String tableName = components.get(components.size() - 1);
    try {
      List<String> tables = connection.listTables();
      if (!tables.contains(tableName)) return Optional.empty();
      return Optional.of(new RedisDatasetHandle(path));
    } catch (Exception e) {
      throw new ConnectorException("Failed to look up Redis table: " + tableName, e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {
    String tableName = ((RedisDatasetHandle) handle).getTableName();

    CachedSchema cached = schemaCache.get(tableName);
    if (cached != null && !cached.isExpired()) {
      return new RedisDatasetMetadata(cached.schema, DatasetStats.of(100, 1.0), tableName);
    }

    try {
      List<Map<String, String>> samples =
          connection.sampleHashes(tableName, config.sampleSize);
      List<Field> fields = RedisTypeConverter.inferFields(samples);
      BatchSchema schema = new BatchSchema(fields);
      schemaCache.put(tableName, new CachedSchema(schema));
      logger.debug("Inferred schema for Redis table '{}': {} fields from {} samples",
          tableName, fields.size(), samples.size());
      return new RedisDatasetMetadata(schema, DatasetStats.of(100, 1.0), tableName);
    } catch (Exception e) {
      throw new ConnectorException("Failed to infer schema for Redis table: " + tableName, e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    String tableName = ((RedisDatasetHandle) handle).getTableName();
    String specStr = tableName;
    byte[] specBytes = specStr.getBytes(StandardCharsets.UTF_8);
    DatasetSplit split = DatasetSplit.of(
        java.util.Collections.emptyList(),
        1024L, 100L,
        os -> os.write(specBytes));
    List<PartitionChunk> chunks = java.util.Collections.singletonList(PartitionChunk.of(split));
    return () -> chunks.iterator();
  }

  @Override
  public boolean containerExists(EntityPath path, GetMetadataOption... options) {
    return false;
  }

  public RedisConnection getConnection() { return connection; }
  public RedisConf getConfig()           { return config; }
}
