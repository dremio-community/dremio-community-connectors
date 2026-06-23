package com.dremio.plugins.googleads;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.BytesOutput;
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
import com.dremio.plugins.googleads.planning.GoogleAdsRulesFactory;
import com.dremio.plugins.googleads.scan.GoogleAdsDatasetHandle;
import com.dremio.plugins.googleads.scan.GoogleAdsDatasetMetadata;
import com.dremio.plugins.googleads.scan.GoogleAdsSubScan.GoogleAdsScanSpec;
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

public class GoogleAdsStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(GoogleAdsStoragePlugin.class);

  private final GoogleAdsConf      config;
  private final PluginSabotContext context;
  private final String             name;

  private GoogleAdsClient client;

  public GoogleAdsStoragePlugin(GoogleAdsConf config, PluginSabotContext context, String name) {
    this.config  = config;
    this.context = context;
    this.name    = name;
  }

  @Override
  public void start() throws IOException {
    client = new GoogleAdsClient(config);
    client.connect();
    logger.info("Google Ads plugin '{}' started (customer={})", name, config.customerId);
  }

  @Override
  public void close() {
    client = null;
  }

  @Override
  public SourceState getState() {
    if (client == null) {
      return SourceState.badState("Google Ads client not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      client.healthCheck();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("Google Ads health check failed: {}", e.getMessage());
      return SourceState.badState("Google Ads connection error: " + e.getMessage(), e);
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return GoogleAdsRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    List<DatasetHandle> handles = new ArrayList<>();
    for (String tableName : GoogleAdsTableDef.ALL.keySet()) {
      handles.add(new GoogleAdsDatasetHandle(
          new EntityPath(Arrays.asList(name, tableName))));
    }
    return handles::iterator;
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath entityPath,
                                                    GetDatasetOption... options) {
    String tableName = leaf(entityPath);
    if (!GoogleAdsTableDef.ALL.containsKey(tableName)) return Optional.empty();
    return Optional.of(new GoogleAdsDatasetHandle(entityPath));
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing partitionChunkListing,
                                             GetMetadataOption... options) throws ConnectorException {
    String tableName = leaf(handle.getDatasetPath());
    GoogleAdsTableDef def = GoogleAdsTableDef.ALL.get(tableName);
    if (def == null) throw new ConnectorException("Unknown table: " + tableName);

    List<Field> fields = def.fields;
    BatchSchema schema = BatchSchema.newBuilder().addFields(fields).build();
    return new GoogleAdsDatasetMetadata(schema, DatasetStats.of(10_000, 1.0), tableName);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options) {
    String tableName = leaf(handle.getDatasetPath());
    GoogleAdsScanSpec spec = new GoogleAdsScanSpec(tableName, config.dateRangeDays);
    byte[] specBytes = spec.toExtendedProperty().getBytes(StandardCharsets.UTF_8);
    DatasetSplit split = DatasetSplit.of(Collections.emptyList(), 10_000, 10_000,
        (BytesOutput) os -> os.write(specBytes));
    PartitionChunk chunk = PartitionChunk.of(split);
    return () -> Collections.singletonList(chunk).iterator();
  }

  @Override
  public boolean containerExists(EntityPath entityPath, GetMetadataOption... options) {
    return false;
  }

  public GoogleAdsClient getClient() { return client; }
  public GoogleAdsConf   getConfig() { return config; }

  private static String leaf(EntityPath path) {
    List<String> c = path.getComponents();
    return c.get(c.size() - 1);
  }
}
