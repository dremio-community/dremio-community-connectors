package com.dremio.plugins.neo4j;

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
import com.dremio.plugins.neo4j.planning.Neo4jRulesFactory;
import com.dremio.plugins.neo4j.scan.Neo4jDatasetHandle;
import com.dremio.plugins.neo4j.scan.Neo4jDatasetMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

public class Neo4jStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(Neo4jStoragePlugin.class);

  private final Neo4jConf config;
  private final PluginSabotContext context;
  private final String name;
  private Neo4jConnection connection;

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

  public Neo4jStoragePlugin(Neo4jConf config, PluginSabotContext context, String name) {
    this.config  = config;
    this.context = context;
    this.name    = name;
  }

  @Override
  public void start() throws IOException {
    try {
      connection = new Neo4jConnection(config);
      connection.connect();
    } catch (Exception e) {
      throw new IOException("Failed to connect to Neo4j: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() throws Exception {
    if (connection != null) connection.close();
  }

  @Override
  public SourceState getState() {
    if (connection == null) {
      return SourceState.badState("Neo4j connection not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      connection.verify();
      return SourceState.goodState();
    } catch (Exception e) {
      try {
        connection.close();
        connection = new Neo4jConnection(config);
        connection.connect();
        return SourceState.goodState();
      } catch (Exception ex) {
        return SourceState.badState("Neo4j reconnect failed: " + ex.getMessage(), ex);
      }
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return Neo4jRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {
    try {
      List<String> labels = connection.listLabels();
      List<DatasetHandle> handles = new ArrayList<>(labels.size());
      for (String label : labels) {
        EntityPath path = new EntityPath(Arrays.asList(name, label));
        handles.add(new Neo4jDatasetHandle(path));
      }
      return () -> handles.iterator();
    } catch (Exception e) {
      throw new ConnectorException("Failed to list Neo4j labels: " + e.getMessage(), e);
    }
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
      throws ConnectorException {
    List<String> components = path.getComponents();
    if (components.size() < 2) return Optional.empty();
    String label = components.get(components.size() - 1);
    try {
      List<String> labels = connection.listLabels();
      if (!labels.contains(label)) return Optional.empty();
      return Optional.of(new Neo4jDatasetHandle(path));
    } catch (Exception e) {
      throw new ConnectorException("Failed to look up Neo4j label: " + label, e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {
    String label = ((Neo4jDatasetHandle) handle).getLabel();

    CachedSchema cached = schemaCache.get(label);
    if (cached != null && !cached.isExpired()) {
      long estimatedRows = connection.estimateCount(label);
      return new Neo4jDatasetMetadata(cached.schema,
          DatasetStats.of(estimatedRows, 1.0), label);
    }

    try {
      Map<String, ArrowType> schemaMap = connection.inferSchema(label, config.sampleSize);
      List<Field> fields = Neo4jTypeConverter.toFields(schemaMap);
      BatchSchema schema = new BatchSchema(fields);
      schemaCache.put(label, new CachedSchema(schema));

      long estimatedRows = connection.estimateCount(label);
      logger.debug("Inferred schema for Neo4j label '{}': {} fields from sampling, ~{} rows",
          label, fields.size(), estimatedRows);
      return new Neo4jDatasetMetadata(schema, DatasetStats.of(estimatedRows, 1.0), label);
    } catch (Exception e) {
      throw new ConnectorException("Failed to infer schema for Neo4j label: " + label, e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    String label = ((Neo4jDatasetHandle) handle).getLabel();
    byte[] specBytes = label.getBytes(StandardCharsets.UTF_8);
    DatasetSplit split = DatasetSplit.of(
        Collections.emptyList(),
        1024L, 100L,
        os -> os.write(specBytes));
    List<PartitionChunk> chunks = Collections.singletonList(PartitionChunk.of(split));
    return () -> chunks.iterator();
  }

  @Override
  public boolean containerExists(EntityPath path, GetMetadataOption... options) {
    return false;
  }

  public Neo4jConnection getConnection() { return connection; }
  public Neo4jConf getConfig()           { return config; }
}
