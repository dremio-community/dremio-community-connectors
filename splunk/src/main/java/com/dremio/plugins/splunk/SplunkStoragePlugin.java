package com.dremio.plugins.splunk;

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
import com.dremio.plugins.splunk.planning.SplunkRulesFactory;
import com.dremio.plugins.splunk.scan.SplunkDatasetHandle;
import com.dremio.plugins.splunk.scan.SplunkDatasetMetadata;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Dremio storage plugin for Splunk.
 *
 * Exposes Splunk indexes as Dremio tables. SQL queries are translated to SPL
 * with time-range and field-equality pushdown via SplunkFilterRule.
 *
 * Each query creates a Splunk search job at planning time. The job SID is
 * embedded in the SplunkScanSpec and the RecordReader executes it.
 *
 * Schema is inferred by sampling recent events per index and cached for
 * {@code metadataCacheTtlSeconds} to avoid repeated sampling.
 */
public class SplunkStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(SplunkStoragePlugin.class);

  private final SplunkConf config;
  private final PluginSabotContext context;
  private final String name;

  private SplunkClient client;

  /** Schema cache: indexName → CachedSchema */
  private final ConcurrentHashMap<String, CachedSchema> schemaCache = new ConcurrentHashMap<>();

  /**
   * Index event-count cache: populated during listDatasetHandles() so that
   * getDatasetMetadata() can use the cached value without a second HTTP call.
   * Key = index name; value = approximate event count from Splunk index metadata.
   */
  private final ConcurrentHashMap<String, Long> indexCountCache = new ConcurrentHashMap<>();

  private static class CachedSchema {
    final BatchSchema schema;
    final long        estimatedRows;
    final long        expiresAtMs;

    CachedSchema(BatchSchema schema, long estimatedRows, long ttlSeconds) {
      this.schema        = schema;
      this.estimatedRows = estimatedRows;
      this.expiresAtMs   = System.currentTimeMillis() + ttlSeconds * 1000L;
    }

    boolean isExpired() { return System.currentTimeMillis() > expiresAtMs; }
  }

  public SplunkStoragePlugin(SplunkConf config, PluginSabotContext context, String name) {
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
      client = new SplunkClient(config);
      client.authenticate();
      String version = client.getServerVersion();
      logger.info("Splunk plugin '{}' connected to {} (version {})",
          name, config.hostname, version);
    } catch (Exception e) {
      throw new IOException("Failed to connect to Splunk at " + config.hostname
          + ":" + config.port, e);
    }
  }

  @Override
  public void close() throws Exception {
    if (client != null) {
      client.close();
    }
  }

  // -----------------------------------------------------------------------
  // StoragePlugin status + capabilities
  // -----------------------------------------------------------------------

  @Override
  public SourceState getState() {
    if (client == null) {
      return SourceState.badState("Splunk client not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      client.getServerVersion();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("Splunk health check failed ({}), attempting reconnect...", e.getMessage());
      try {
        client.close();
        client = new SplunkClient(config);
        client.authenticate();
        schemaCache.clear();
        logger.info("Splunk reconnection successful");
        return SourceState.goodState();
      } catch (Exception reconnectEx) {
        return SourceState.badState(
            "Splunk connection lost and reconnect failed: " + reconnectEx.getMessage(),
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
    return SplunkRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    return true;
  }

  // -----------------------------------------------------------------------
  // Dataset enumeration (SupportsListingDatasets)
  // -----------------------------------------------------------------------

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {
    try {
      // listIndexesWithCounts() fetches names AND event counts in one HTTP call,
      // so getDatasetMetadata() won't need a second per-index API call for the count.
      java.util.Map<String, Long> indexCounts = client.listIndexesWithCounts();
      List<DatasetHandle> handles = new ArrayList<>();

      Pattern excludePattern = buildPattern(config.indexExcludePattern, "exclude");
      Pattern includePattern = buildPattern(config.indexIncludePattern, "include");

      for (java.util.Map.Entry<String, Long> entry : indexCounts.entrySet()) {
        String index = entry.getKey();
        if (excludePattern != null && excludePattern.matcher(index).matches()) continue;
        if (includePattern != null && !includePattern.matcher(index).matches()) continue;
        indexCountCache.put(index, entry.getValue());
        EntityPath path = new EntityPath(Arrays.asList(name, index));
        handles.add(new SplunkDatasetHandle(path));
      }

      logger.debug("Listed {} indexes from Splunk source '{}'", handles.size(), name);
      return () -> handles.iterator();

    } catch (Exception e) {
      throw new ConnectorException("Failed to list Splunk indexes", e);
    }
  }

  // -----------------------------------------------------------------------
  // Dataset metadata
  // -----------------------------------------------------------------------

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
      throws ConnectorException {
    List<String> components = path.getComponents();
    if (components.size() < 2) return Optional.empty();
    String indexName = components.get(components.size() - 1);
    try {
      if (!client.indexExists(indexName)) return Optional.empty();
      return Optional.of(new SplunkDatasetHandle(path));
    } catch (Exception e) {
      throw new ConnectorException("Failed to look up Splunk index: " + indexName, e);
    }
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {
    String indexName = ((SplunkDatasetHandle) handle).getIndexName();

    // Check cache
    CachedSchema cached = schemaCache.get(indexName);
    if (cached != null && !cached.isExpired()) {
      return new SplunkDatasetMetadata(cached.schema,
          DatasetStats.of(cached.estimatedRows, 1.0), indexName);
    }

    try {
      SplunkSchemaInferrer inferrer = new SplunkSchemaInferrer(client, config.sampleEventsForSchema);
      List<Field> fields = inferrer.inferFields(indexName);
      BatchSchema schema = new BatchSchema(fields);

      // Use cached count from the last listDatasetHandles() call to avoid a second HTTP round-trip.
      // Fall back to a direct API call only if the cache doesn't have an entry for this index
      // (e.g. getDatasetHandle() was called directly without a preceding list).
      Long cachedCount = indexCountCache.get(indexName);
      long estimatedRows = (cachedCount != null)
          ? cachedCount
          : client.getIndexEventCount(indexName);
      if (config.metadataCacheTtlSeconds > 0) {
        schemaCache.put(indexName,
            new CachedSchema(schema, estimatedRows, config.metadataCacheTtlSeconds));
      }
      return new SplunkDatasetMetadata(schema, DatasetStats.of(estimatedRows, 1.0), indexName);

    } catch (Exception e) {
      throw new ConnectorException("Failed to infer schema for Splunk index: " + indexName, e);
    }
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    String indexName = ((SplunkDatasetHandle) handle).getIndexName();
    int buckets = Math.max(1, config.timeBucketCount);

    if (buckets == 1) {
      // Single partition chunk — original behaviour
      SplunkScanSpec spec = new SplunkScanSpec(
          indexName,
          config.defaultEarliest, "now",
          -1L, -1L, "",
          config.defaultMaxEvents,
          Collections.emptyList()
      );
      return singleChunk(spec);
    }

    // -----------------------------------------------------------------------
    // Time-bucketed parallel scans
    // Divide the default time window into `buckets` equal sub-ranges. Each
    // sub-range becomes an independent DatasetSplit dispatched to a different
    // executor fragment, reducing wall-clock time on large/high-volume indexes.
    // -----------------------------------------------------------------------
    long nowMs       = System.currentTimeMillis();
    long earliestMs  = resolveRelativeTime(config.defaultEarliest, nowMs);
    long windowMs    = nowMs - earliestMs;
    long bucketMs    = windowMs / buckets;
    // Distribute the event cap evenly; at least 1 per bucket
    int eventsPerBucket = Math.max(1, config.defaultMaxEvents / buckets);

    List<PartitionChunk> chunks = new ArrayList<>(buckets);
    for (int i = 0; i < buckets; i++) {
      long bucketEarliest = earliestMs + (long) i * bucketMs;
      long bucketLatest   = (i == buckets - 1) ? nowMs : bucketEarliest + bucketMs;

      SplunkScanSpec spec = new SplunkScanSpec(
          indexName,
          config.defaultEarliest, "now",   // kept for display/fallback; effectiveEarliest/Latest use epoch values
          bucketEarliest, bucketLatest,
          "", eventsPerBucket,
          Collections.emptyList()
      );

      long   estimatedRows = eventsPerBucket;
      long   sizeBytes     = estimatedRows * 512L;
      byte[] specBytes     = spec.toExtendedProperty().getBytes(StandardCharsets.UTF_8);
      DatasetSplit split = DatasetSplit.of(
          Collections.emptyList(), sizeBytes, estimatedRows,
          os -> os.write(specBytes));
      chunks.add(PartitionChunk.of(split));
    }

    logger.debug("Created {} time-bucketed splits for index '{}' (window {} → {}, {} events/bucket)",
        buckets, indexName, earliestMs, nowMs, eventsPerBucket);
    return () -> chunks.iterator();
  }

  /** Creates a PartitionChunkListing containing a single split. */
  private static PartitionChunkListing singleChunk(SplunkScanSpec spec) {
    long   estimatedRows = spec.getMaxEvents();
    long   sizeBytes     = estimatedRows * 512L;
    byte[] specBytes     = spec.toExtendedProperty().getBytes(StandardCharsets.UTF_8);
    DatasetSplit split = DatasetSplit.of(
        Collections.emptyList(), sizeBytes, estimatedRows,
        os -> os.write(specBytes));
    List<PartitionChunk> chunks = Collections.singletonList(PartitionChunk.of(split));
    return () -> chunks.iterator();
  }

  @Override
  public boolean containerExists(EntityPath entityPath, GetMetadataOption... options) {
    return false;
  }

  // -----------------------------------------------------------------------
  // Package-visible accessors for RecordReader
  // -----------------------------------------------------------------------

  /** Returns the SplunkClient so RecordReader can execute searches. */
  public SplunkClient getClient() {
    return client;
  }

  public SplunkConf getConfig() {
    return config;
  }

  // -----------------------------------------------------------------------
  // Private helpers
  // -----------------------------------------------------------------------

  private static Pattern buildPattern(String regex, String label) {
    if (regex == null || regex.isBlank()) return null;
    try {
      return Pattern.compile(regex);
    } catch (Exception e) {
      logger.warn("Invalid Splunk index {} pattern '{}': {}", label, regex, e.getMessage());
      return null;
    }
  }

  // -----------------------------------------------------------------------
  // Relative time resolution
  // -----------------------------------------------------------------------

  /**
   * Splunk relative time modifier pattern.
   * Matches strings like: -24h, -7d, -30m, -1w, -0s, 24h (with optional snap: @h, @d …)
   */
  private static final Pattern RELATIVE_TIME_PATTERN =
      Pattern.compile("^-?(\\d+)([smhdwSMHDW])(?:@.*)?$");

  /**
   * Resolves a Splunk time expression to an epoch-millisecond value.
   *
   * Supported forms:
   *   -Ns / -Nm / -Nh / -Nd / -Nw  — relative modifiers (N seconds/minutes/hours/days/weeks ago)
   *   ISO-8601 string               — parsed directly
   *   numeric string                — treated as epoch-seconds
   *   null / blank / "-0s"          — returns 0 (beginning of index retention)
   *
   * @param timeStr Splunk time expression from config
   * @param nowMs   current epoch-milliseconds (injected so callers can fix "now" for consistency)
   * @return epoch-milliseconds
   */
  static long resolveRelativeTime(String timeStr, long nowMs) {
    if (timeStr == null || timeStr.isBlank()) {
      return 0L; // no lower bound — start from beginning of retention
    }
    String trimmed = timeStr.trim();
    if (trimmed.equals("-0s") || trimmed.equals("0")) {
      return 0L;
    }

    // Splunk relative modifier: -Nh, -Nd, -Nw, -Nm, -Ns (leading minus optional)
    Matcher m = RELATIVE_TIME_PATTERN.matcher(trimmed);
    if (m.matches()) {
      long value = Long.parseLong(m.group(1));
      String unit = m.group(2).toLowerCase(java.util.Locale.ROOT);
      long offsetMs;
      switch (unit) {
        case "s": offsetMs = value * 1_000L;              break;
        case "m": offsetMs = value * 60_000L;             break;
        case "h": offsetMs = value * 3_600_000L;          break;
        case "d": offsetMs = value * 86_400_000L;         break;
        case "w": offsetMs = value * 7L * 86_400_000L;    break;
        default:  offsetMs = 86_400_000L;                 break; // fallback: 24 h
      }
      return nowMs - offsetMs;
    }

    // Numeric epoch-seconds
    try {
      long epochSeconds = Long.parseLong(trimmed);
      return epochSeconds * 1_000L;
    } catch (NumberFormatException ignored) { /* not a number */ }

    // ISO-8601 timestamp
    try {
      return Instant.parse(trimmed).toEpochMilli();
    } catch (Exception ignored) { /* not ISO-8601 */ }

    // Last resort: 24 h ago
    logger.warn("Could not parse Splunk time expression '{}', defaulting to -24h", timeStr);
    return nowMs - 86_400_000L;
  }
}
