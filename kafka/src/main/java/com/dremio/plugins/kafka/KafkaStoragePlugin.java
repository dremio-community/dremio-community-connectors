package com.dremio.plugins.kafka;

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
import com.dremio.plugins.kafka.planning.KafkaRulesFactory;
import com.dremio.plugins.kafka.scan.KafkaDatasetHandle;
import com.dremio.plugins.kafka.scan.KafkaDatasetMetadata;
import com.dremio.plugins.kafka.scan.KafkaScanSpec;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Dremio storage plugin for Apache Kafka.
 *
 * Exposes Kafka topics as tables with bounded snapshot scans.
 * Each query freezes partition offsets at planning time — messages that
 * arrive after planning starts are not included (deterministic bounded reads).
 *
 * Architecture:
 *   KafkaConf.newPlugin()   → KafkaStoragePlugin
 *   start()                 → opens AdminClient for topic/offset discovery
 *   listDatasetHandles()    → enumerates visible topics
 *   getDatasetMetadata()    → builds Arrow BatchSchema (metadata fields + JSON payload fields)
 *   listPartitionChunks()   → one PartitionChunk per Kafka partition, frozen offsets
 *   KafkaRulesFactory       → planning rules (ScanCrel → GroupScan → SubScan)
 *   KafkaScanCreator        → execution (SubScan → RecordReader → ScanOperator)
 */
public class KafkaStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(KafkaStoragePlugin.class);

  // -----------------------------------------------------------------------
  // Standard metadata columns present in every topic schema
  // -----------------------------------------------------------------------
  public static final String COL_TOPIC          = "_topic";
  public static final String COL_PARTITION      = "_partition";
  public static final String COL_OFFSET         = "_offset";
  public static final String COL_TIMESTAMP      = "_timestamp";
  public static final String COL_TIMESTAMP_TYPE = "_timestamp_type";
  public static final String COL_KEY            = "_key";
  public static final String COL_HEADERS        = "_headers";
  public static final String COL_VALUE_RAW      = "_value_raw";
  public static final String COL_SCHEMA_ID      = "_schema_id";

  private static final List<Field> METADATA_FIELDS = Arrays.asList(
      new Field(COL_TOPIC,          FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_PARTITION,      FieldType.nullable(new ArrowType.Int(32, true)), null),
      new Field(COL_OFFSET,         FieldType.nullable(new ArrowType.Int(64, true)), null),
      new Field(COL_TIMESTAMP,      FieldType.nullable(new ArrowType.Int(64, true)), null),
      new Field(COL_TIMESTAMP_TYPE, FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_KEY,            FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_HEADERS,        FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_VALUE_RAW,      FieldType.nullable(ArrowType.Utf8.INSTANCE), null),
      new Field(COL_SCHEMA_ID,      FieldType.nullable(new ArrowType.Int(32, true)), null)
  );

  private final KafkaConf config;
  private final PluginSabotContext context;
  private final String name;

  private AdminClient adminClient;

  /** Cache: topic → cached schema + expiry. */
  private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();

  private static class CachedSchema {
    final BatchSchema schema;
    final long estimatedRows;
    final long expiresAtMs;

    CachedSchema(BatchSchema schema, long estimatedRows, long ttlSeconds) {
      this.schema = schema;
      this.estimatedRows = estimatedRows;
      this.expiresAtMs = System.currentTimeMillis() + ttlSeconds * 1000L;
    }

    boolean isExpired() { return System.currentTimeMillis() > expiresAtMs; }
  }

  public KafkaStoragePlugin(KafkaConf config, PluginSabotContext context, String name) {
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
      adminClient = AdminClient.create(buildAdminClientProps());
      // Verify connection by listing topics
      adminClient.listTopics().names().get();
      logger.info("Kafka plugin '{}' connected to {}", name, config.bootstrapServers);
    } catch (Exception e) {
      throw new IOException("Failed to connect to Kafka at " + config.bootstrapServers, e);
    }
  }

  @Override
  public void close() throws Exception {
    if (adminClient != null) {
      adminClient.close();
    }
  }

  // -----------------------------------------------------------------------
  // StoragePlugin status + capabilities
  // -----------------------------------------------------------------------

  @Override
  public SourceState getState() {
    if (adminClient == null) {
      return SourceState.badState("Kafka AdminClient not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      adminClient.listTopics().names().get();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("Kafka health check failed ({}), attempting reconnect...", e.getMessage());
      try {
        if (adminClient != null) {
          try { adminClient.close(); } catch (Exception ignore) { }
        }
        adminClient = AdminClient.create(buildAdminClientProps());
        adminClient.listTopics().names().get();
        schemaCache.clear();
        logger.info("Kafka reconnection successful");
        return SourceState.goodState();
      } catch (Exception reconnectEx) {
        return SourceState.badState(
            "Kafka connection lost and reconnect failed: " + reconnectEx.getMessage(),
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
    return KafkaRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  // -----------------------------------------------------------------------
  // SupportsListingDatasets — topic enumeration
  // -----------------------------------------------------------------------

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {
    try {
      Set<String> allTopics = adminClient.listTopics().names().get();
      List<DatasetHandle> handles = new ArrayList<>();

      Pattern excludePattern = buildExcludePattern();
      Pattern includePattern = buildIncludePattern();

      for (String topic : allTopics) {
        if (excludePattern != null && excludePattern.matcher(topic).find()) continue;
        if (includePattern != null && !includePattern.matcher(topic).find()) continue;
        EntityPath path = new EntityPath(Arrays.asList(name, topic));
        handles.add(new KafkaDatasetHandle(path));
      }

      logger.debug("Listed {} topics from Kafka source '{}'", handles.size(), name);
      return () -> handles.iterator();

    } catch (Exception e) {
      throw new ConnectorException("Failed to list Kafka topics", e);
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

    String topic = components.get(components.size() - 1);
    try {
      Set<String> allTopics = adminClient.listTopics().names().get();
      if (!allTopics.contains(topic)) return Optional.empty();
      return Optional.of(new KafkaDatasetHandle(path));
    } catch (Exception e) {
      throw new ConnectorException("Failed to look up Kafka topic: " + topic, e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {
    String topic = ((KafkaDatasetHandle) handle).getTopicName();

    // Check cache
    CachedSchema cached = schemaCache.get(topic);
    if (cached != null && !cached.isExpired()) {
      return new KafkaDatasetMetadata(cached.schema, DatasetStats.of(cached.estimatedRows, 1.0), topic);
    }

    BatchSchema schema = buildSchema(topic);
    long estimatedRows = estimateTotalRecords(topic);
    if (config.metadataCacheTtlSeconds > 0) {
      schemaCache.put(topic, new CachedSchema(schema, estimatedRows, config.metadataCacheTtlSeconds));
    }

    return new KafkaDatasetMetadata(schema, DatasetStats.of(estimatedRows, 1.0), topic);
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    String topic = ((KafkaDatasetHandle) handle).getTopicName();

    try {
      List<TopicPartition> partitions = getPartitions(topic);
      if (partitions.isEmpty()) {
        return Collections::emptyIterator;
      }

      Map<TopicPartition, Long> earliestOffsets = fetchOffsets(partitions, OffsetSpec.earliest());
      Map<TopicPartition, Long> latestOffsets   = fetchOffsets(partitions, OffsetSpec.latest());

      List<PartitionChunk> chunks = new ArrayList<>();

      for (TopicPartition tp : partitions) {
        long earliest = earliestOffsets.getOrDefault(tp, 0L);
        long latest   = latestOffsets.getOrDefault(tp, 0L);

        if (latest <= earliest) {
          // Empty partition — create a no-op split
          KafkaScanSpec spec = new KafkaScanSpec(
              topic, tp.partition(), earliest, earliest, config.schemaMode);
          byte[] emptySpecBytes = spec.toExtendedProperty().getBytes(StandardCharsets.UTF_8);
          DatasetSplit emptySplit = DatasetSplit.of(
              Collections.emptyList(), 0, 0, os -> os.write(emptySpecBytes));
          chunks.add(PartitionChunk.of(emptySplit));
          continue;
        }

        // Apply default scan window
        long startOffset = earliest;
        if (config.defaultMaxRecordsPerPartition > 0) {
          startOffset = Math.max(earliest, latest - config.defaultMaxRecordsPerPartition);
        }
        long endOffset = latest; // exclusive

        KafkaScanSpec spec = new KafkaScanSpec(
            topic, tp.partition(), startOffset, endOffset, config.schemaMode);

        long recordCount = spec.estimatedRecordCount();
        long sizeBytes   = recordCount * 512L; // rough estimate: 512 bytes per message

        byte[] specBytes = spec.toExtendedProperty().getBytes(StandardCharsets.UTF_8);
        DatasetSplit split = DatasetSplit.of(
            Collections.emptyList(), sizeBytes, recordCount, os -> os.write(specBytes));
        chunks.add(PartitionChunk.of(split));
      }

      return () -> chunks.iterator();

    } catch (Exception e) {
      throw new ConnectorException("Failed to list partition chunks for topic: " + topic, e);
    }
  }

  @Override
  public boolean containerExists(EntityPath entityPath, GetMetadataOption... options) {
    return false;
  }

  // -----------------------------------------------------------------------
  // Package-visible accessors for RecordReader
  // -----------------------------------------------------------------------

  /** Returns the KafkaConf so RecordReader can build consumer properties. */
  public KafkaConf getConfig() {
    return config;
  }

  /**
   * Builds Kafka consumer Properties from this plugin's configuration.
   * RecordReader calls this to create its own KafkaConsumer.
   */
  public Properties buildConsumerProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", config.bootstrapServers);
    props.put("group.id", "dremio-kafka-connector-" + name + "-" + System.currentTimeMillis());
    props.put("auto.offset.reset", "earliest");
    props.put("enable.auto.commit", "false");
    props.put("max.poll.records", String.valueOf(config.maxPollRecords));
    props.put("request.timeout.ms", String.valueOf(config.requestTimeoutMs));
    props.put("session.timeout.ms", "10000");
    props.put("heartbeat.interval.ms", "3000");
    applySecurityProps(props);
    return props;
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  private Properties buildAdminClientProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", config.bootstrapServers);
    props.put("request.timeout.ms", String.valueOf(config.requestTimeoutMs));
    props.put("default.api.timeout.ms", String.valueOf(config.requestTimeoutMs));
    applySecurityProps(props);
    return props;
  }

  private void applySecurityProps(Properties props) {
    String protocol = config.securityProtocol;
    if (protocol == null || protocol.isEmpty()) {
      protocol = "PLAINTEXT";
    }
    props.put("security.protocol", protocol);

    if (protocol.contains("SASL")) {
      String mechanism = config.saslMechanism;
      if (mechanism != null && !mechanism.isEmpty()) {
        props.put("sasl.mechanism", mechanism);

        String username = config.saslUsername != null ? config.saslUsername : "";
        String password = config.saslPassword != null ? config.saslPassword : "";

        String loginModule;
        if (mechanism.startsWith("SCRAM")) {
          loginModule = "org.apache.kafka.common.security.scram.ScramLoginModule";
        } else {
          loginModule = "org.apache.kafka.common.security.plain.PlainLoginModule";
        }
        props.put("sasl.jaas.config",
            loginModule + " required username=\"" + username + "\" password=\"" + password + "\";");
      }
    }

    if (protocol.contains("SSL")) {
      if (config.sslTruststorePath != null && !config.sslTruststorePath.isEmpty()) {
        props.put("ssl.truststore.location", config.sslTruststorePath);
        props.put("ssl.truststore.type",
            config.sslTruststoreType != null ? config.sslTruststoreType : "JKS");
        if (config.sslTruststorePassword != null && !config.sslTruststorePassword.isEmpty()) {
          props.put("ssl.truststore.password", config.sslTruststorePassword);
        }
      }
      if (config.sslKeystorePath != null && !config.sslKeystorePath.isEmpty()) {
        props.put("ssl.keystore.location", config.sslKeystorePath);
        props.put("ssl.keystore.type",
            config.sslKeystoreType != null ? config.sslKeystoreType : "JKS");
        if (config.sslKeystorePassword != null && !config.sslKeystorePassword.isEmpty()) {
          props.put("ssl.keystore.password", config.sslKeystorePassword);
        }
      }
      if (config.sslDisableHostnameVerification) {
        props.put("ssl.endpoint.identification.algorithm", "");
      }
    }
  }

  private List<TopicPartition> getPartitions(String topic) throws Exception {
    return adminClient.describeTopics(Collections.singletonList(topic))
        .all().get()
        .get(topic)
        .partitions()
        .stream()
        .map(info -> new TopicPartition(topic, info.partition()))
        .collect(Collectors.toList());
  }

  private Map<TopicPartition, Long> fetchOffsets(
      List<TopicPartition> partitions, OffsetSpec spec) throws Exception {

    Map<TopicPartition, OffsetSpec> request = new HashMap<>();
    for (TopicPartition tp : partitions) {
      request.put(tp, spec);
    }

    Map<TopicPartition, Long> result = new HashMap<>();
    ListOffsetsResult offsetResult = adminClient.listOffsets(request);
    offsetResult.all().get().forEach((tp, info) -> result.put(tp, info.offset()));
    return result;
  }

  private BatchSchema buildSchema(String topic) throws ConnectorException {
    List<Field> fields = new ArrayList<>(METADATA_FIELDS);

    if ("JSON".equalsIgnoreCase(config.schemaMode)) {
      try {
        List<TopicPartition> partitions = getPartitions(topic);
        if (!partitions.isEmpty()) {
          Map<TopicPartition, Long> earliest = fetchOffsets(partitions, OffsetSpec.earliest());
          Map<TopicPartition, Long> latest   = fetchOffsets(partitions, OffsetSpec.latest());

          KafkaSchemaInferrer inferrer = new KafkaSchemaInferrer(
              buildConsumerProps(), config.sampleRecordsForSchema);
          List<Field> payloadFields = inferrer.inferFields(topic, partitions, earliest, latest);

          // Avoid duplicating metadata field names
          for (Field pf : payloadFields) {
            if (METADATA_FIELDS.stream().noneMatch(mf -> mf.getName().equals(pf.getName()))) {
              fields.add(pf);
            }
          }
        }
      } catch (Exception e) {
        logger.warn("JSON schema inference failed for topic {}, using RAW schema: {}", topic, e.getMessage());
      }
    } else if ("AVRO".equalsIgnoreCase(config.schemaMode)
        && config.schemaRegistryUrl != null && !config.schemaRegistryUrl.isEmpty()) {
      try {
        com.dremio.plugins.kafka.avro.KafkaSchemaRegistryClient client =
            new com.dremio.plugins.kafka.avro.KafkaSchemaRegistryClient(
                config.schemaRegistryUrl, config.requestTimeoutMs,
                config.metadataCacheTtlSeconds * 1000L,
                config.schemaRegistryUsername, config.schemaRegistryPassword,
                config.schemaRegistryDisableSslVerification);
        org.apache.avro.Schema avroSchema = client.getLatestSchema(topic);
        List<Field> avroFields = com.dremio.plugins.kafka.avro.KafkaAvroConverter.avroToArrowFields(avroSchema);
        for (Field f : avroFields) {
          if (METADATA_FIELDS.stream().noneMatch(mf -> mf.getName().equals(f.getName()))) {
            fields.add(f);
          }
        }
      } catch (Exception e) {
        logger.warn("Avro schema fetch failed for topic {}, using metadata-only schema: {}", topic, e.getMessage());
      }
    }

    return new BatchSchema(fields);
  }

  private long estimateTotalRecords(String topic) {
    try {
      List<TopicPartition> partitions = getPartitions(topic);
      Map<TopicPartition, Long> earliest = fetchOffsets(partitions, OffsetSpec.earliest());
      Map<TopicPartition, Long> latest   = fetchOffsets(partitions, OffsetSpec.latest());

      long total = 0;
      for (TopicPartition tp : partitions) {
        long e = earliest.getOrDefault(tp, 0L);
        long l = latest.getOrDefault(tp, 0L);
        long partitionRecords = Math.max(0, l - e);
        if (config.defaultMaxRecordsPerPartition > 0) {
          partitionRecords = Math.min(partitionRecords, config.defaultMaxRecordsPerPartition);
        }
        total += partitionRecords;
      }
      return Math.max(1, total);
    } catch (Exception e) {
      logger.debug("Could not estimate record count for topic {}: {}", topic, e.getMessage());
      return 1000L;
    }
  }

  private Pattern buildExcludePattern() {
    String pattern = config.topicExcludePattern;
    if (pattern == null || pattern.isEmpty()) return null;
    try {
      return Pattern.compile(pattern);
    } catch (Exception e) {
      logger.warn("Invalid topicExcludePattern '{}': {}", pattern, e.getMessage());
      return null;
    }
  }

  private Pattern buildIncludePattern() {
    String pattern = config.topicIncludePattern;
    if (pattern == null || pattern.isEmpty()) return null;
    try {
      return Pattern.compile(pattern);
    } catch (Exception e) {
      logger.warn("Invalid topicIncludePattern '{}': {}", pattern, e.getMessage());
      return null;
    }
  }
}
