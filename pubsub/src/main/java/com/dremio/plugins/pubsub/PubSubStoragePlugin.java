package com.dremio.plugins.pubsub;

import com.dremio.connector.ConnectorException;
import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.DatasetHandleListing;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.BytesOutput;
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
import com.dremio.plugins.pubsub.planning.PubSubRulesFactory;
import com.dremio.plugins.pubsub.scan.PubSubDatasetHandle;
import com.dremio.plugins.pubsub.scan.PubSubDatasetMetadata;
import com.dremio.plugins.pubsub.scan.PubSubSubScan.PubSubScanSpec;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Dremio storage plugin for Google Cloud Pub/Sub.
 *
 * Exposes Pub/Sub subscriptions as tables with pull-without-ack snapshot scans.
 * Messages are pulled, returned as rows, then immediately NACKed so they remain
 * available for other consumers.
 *
 * Architecture:
 *   PubSubConf.newPlugin()    → PubSubStoragePlugin
 *   start()                   → creates PubSubClient, verifies connection
 *   listDatasetHandles()      → enumerates subscriptions (filtered by patterns)
 *   getDatasetMetadata()      → builds Arrow BatchSchema (metadata + JSON payload fields)
 *   listPartitionChunks()     → single PartitionChunk (Pub/Sub has no partitions)
 *   PubSubRulesFactory        → planning rules (ScanCrel → GroupScan → SubScan)
 *   PubSubScanCreator         → execution (SubScan → RecordReader → ScanOperator)
 */
public class PubSubStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(PubSubStoragePlugin.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  // -----------------------------------------------------------------------
  // Standard metadata columns present in every subscription schema
  // -----------------------------------------------------------------------
  public static final String COL_SUBSCRIPTION = "_subscription";
  public static final String COL_MESSAGE_ID   = "_message_id";
  public static final String COL_PUBLISH_TIME = "_publish_time";
  public static final String COL_ORDERING_KEY = "_ordering_key";
  public static final String COL_ATTRIBUTES   = "_attributes";
  public static final String COL_VALUE_RAW    = "_value_raw";

  private static final List<Field> METADATA_FIELDS = Arrays.asList(
      new Field(COL_SUBSCRIPTION, FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_MESSAGE_ID,   FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_PUBLISH_TIME, FieldType.nullable(new ArrowType.Int(64, true)), null),
      new Field(COL_ORDERING_KEY, FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_ATTRIBUTES,   FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_VALUE_RAW,    FieldType.nullable(ArrowType.Utf8.INSTANCE), null)
  );

  private final PubSubConf config;
  private final PluginSabotContext context;
  private final String name;

  private PubSubClient client;

  /** Cache: subscription → cached schema + expiry. */
  private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();

  private static class CachedSchema {
    final BatchSchema schema;
    final long expiresAtMs;

    CachedSchema(BatchSchema schema, long ttlSeconds) {
      this.schema      = schema;
      this.expiresAtMs = System.currentTimeMillis() + ttlSeconds * 1000L;
    }

    boolean isExpired() { return System.currentTimeMillis() > expiresAtMs; }
  }

  public PubSubStoragePlugin(PubSubConf config, PluginSabotContext context, String name) {
    this.config  = config;
    this.context = context;
    this.name    = name;
  }

  // -----------------------------------------------------------------------
  // StoragePlugin lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void start() throws IOException {
    client = new PubSubClient(config.projectId, config.credentialsFile, config.emulatorHost);
    client.connect();
    // Don't health-check at startup — the subscription list call may fail if GCP is unreachable
    // or if the emulator isn't up yet. We'll surface errors lazily via getState().
    logger.info("Pub/Sub plugin '{}' started (project={})", name, config.projectId);
  }

  @Override
  public void close() throws Exception {
    client = null;
  }

  // -----------------------------------------------------------------------
  // StoragePlugin status + capabilities
  // -----------------------------------------------------------------------

  @Override
  public SourceState getState() {
    if (client == null) {
      return SourceState.badState("Pub/Sub client not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      client.healthCheck();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("Pub/Sub health check failed: {}", e.getMessage());
      return SourceState.badState("Pub/Sub connection error: " + e.getMessage(), e);
    }
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return PubSubRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  // -----------------------------------------------------------------------
  // SupportsListingDatasets — subscription enumeration
  // -----------------------------------------------------------------------

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options) {
    List<DatasetHandle> handles = new ArrayList<>();
    try {
      List<String> subscriptions = client.listSubscriptions();
      Pattern includePattern = patternOrNull(config.subscriptionIncludePattern);
      Pattern excludePattern = patternOrNull(config.subscriptionExcludePattern);

      for (String sub : subscriptions) {
        if (excludePattern != null && excludePattern.matcher(sub).find()) continue;
        if (includePattern != null && !includePattern.matcher(sub).find())  continue;
        handles.add(new PubSubDatasetHandle(new EntityPath(Arrays.asList(name, sub))));
      }
    } catch (Exception e) {
      logger.error("Failed to list Pub/Sub subscriptions: {}", e.getMessage(), e);
    }
    return handles::iterator;
  }

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath entityPath, GetDatasetOption... options) {
    return Optional.of(new PubSubDatasetHandle(entityPath));
  }

  // -----------------------------------------------------------------------
  // Schema and partition chunk discovery
  // -----------------------------------------------------------------------

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing partitionChunkListing,
                                             GetMetadataOption... options) throws ConnectorException {
    String sub = ((PubSubDatasetHandle) handle).getSubscriptionName();

    CachedSchema cached = schemaCache.get(sub);
    if (cached != null && !cached.isExpired()) {
      return new PubSubDatasetMetadata(cached.schema, DatasetStats.of(1000, 1.0), sub);
    }

    BatchSchema schema = buildSchema(sub);
    schemaCache.put(sub, new CachedSchema(schema, config.metadataCacheTtlSeconds));
    return new PubSubDatasetMetadata(schema, DatasetStats.of(1000, 1.0), sub);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options) {
    String sub = ((PubSubDatasetHandle) handle).getSubscriptionName();

    // Pub/Sub has no partitions — always a single split
    PubSubScanSpec spec = new PubSubScanSpec(sub, config.defaultMaxMessages, config.schemaMode);
    byte[] specBytes    = spec.toExtendedProperty().getBytes(StandardCharsets.UTF_8);
    DatasetSplit split  = DatasetSplit.of(Collections.emptyList(), 1000, 1000,
        (BytesOutput) os -> os.write(specBytes));
    PartitionChunk chunk = PartitionChunk.of(split);

    return () -> Collections.singletonList(chunk).iterator();
  }

  @Override
  public boolean containerExists(EntityPath entityPath, GetMetadataOption... options) {
    return false;
  }

  // -----------------------------------------------------------------------
  // Schema inference
  // -----------------------------------------------------------------------

  /**
   * Builds the Arrow BatchSchema for a subscription.
   * In JSON mode, samples a few messages to infer field types.
   * Sampled messages are always NACKed so they stay in the subscription.
   */
  private BatchSchema buildSchema(String subscription) {
    List<Field> fields = new ArrayList<>(METADATA_FIELDS);

    if ("JSON".equalsIgnoreCase(config.schemaMode)) {
      try {
        List<PubSubClient.PubSubMessage> sample =
            client.pull(subscription, config.sampleMessagesForSchema);

        if (!sample.isEmpty()) {
          LinkedHashMap<String, ArrowType> inferredTypes = new LinkedHashMap<>();

          for (PubSubClient.PubSubMessage msg : sample) {
            try {
              JsonNode root = MAPPER.readTree(msg.dataAsUtf8());
              if (root != null && root.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> it = root.fields();
                while (it.hasNext()) {
                  Map.Entry<String, JsonNode> entry = it.next();
                  String fname     = entry.getKey();
                  ArrowType existing = inferredTypes.get(fname);
                  ArrowType inferred = inferArrowType(entry.getValue());
                  inferredTypes.put(fname,
                      existing == null ? inferred : promoteType(existing, inferred));
                }
              }
            } catch (Exception ignore) { }
          }

          Set<String> metaCols = METADATA_FIELDS.stream()
              .map(Field::getName).collect(Collectors.toSet());
          for (Map.Entry<String, ArrowType> entry : inferredTypes.entrySet()) {
            if (!metaCols.contains(entry.getKey())) {
              fields.add(new Field(entry.getKey(),
                  FieldType.nullable(entry.getValue()), null));
            }
          }

          // NACK sampled messages so they remain in the subscription
          List<String> ackIds = sample.stream()
              .map(m -> m.ackId).collect(Collectors.toList());
          client.nack(subscription, ackIds);
        }
      } catch (Exception e) {
        logger.warn("[{}] Schema inference failed for '{}': {}", name, subscription, e.getMessage());
      }
    }

    return BatchSchema.newBuilder().addFields(fields).build();
  }

  // -----------------------------------------------------------------------
  // Type inference helpers
  // -----------------------------------------------------------------------

  private static ArrowType inferArrowType(JsonNode node) {
    if (node.isBoolean()) return ArrowType.Bool.INSTANCE;
    if (node.isLong() || (node.isIntegralNumber() && !node.isBigInteger()))
      return new ArrowType.Int(64, true);
    if (node.isDouble() || node.isFloat() || node.isFloatingPointNumber())
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    return ArrowType.Utf8.INSTANCE;
  }

  private static ArrowType promoteType(ArrowType a, ArrowType b) {
    if (a.equals(b)) return a;
    if (a instanceof ArrowType.Utf8 || b instanceof ArrowType.Utf8)
      return ArrowType.Utf8.INSTANCE;
    if ((a instanceof ArrowType.Int && b instanceof ArrowType.FloatingPoint)
        || (b instanceof ArrowType.Int && a instanceof ArrowType.FloatingPoint))
      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    return ArrowType.Utf8.INSTANCE;
  }

  // -----------------------------------------------------------------------
  // Accessors
  // -----------------------------------------------------------------------

  public PubSubConf getConfig() { return config; }

  public PubSubClient getClient() { return client; }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static Pattern patternOrNull(String pattern) {
    if (pattern == null || pattern.isEmpty()) return null;
    try { return Pattern.compile(pattern); }
    catch (Exception e) { return null; }
  }
}
