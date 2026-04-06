package com.dremio.plugins.cassandra;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
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
import com.dremio.plugins.cassandra.planning.CassandraRulesFactory;
import com.dremio.plugins.cassandra.scan.CassandraDatasetHandle;
import com.dremio.plugins.cassandra.scan.CassandraDatasetMetadata;
import com.dremio.service.namespace.NamespaceKey;
import com.dremio.service.namespace.SourceState;
import com.dremio.service.namespace.capabilities.SourceCapabilities;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Dremio storage plugin for Apache Cassandra.
 *
 * Implements StoragePlugin (lifecycle + metadata API) and SupportsListingDatasets
 * (table enumeration for the Dremio catalog browser).
 *
 * Architecture:
 *   CassandraStoragePluginConfig.newPlugin() → CassandraStoragePlugin
 *   start() → opens CassandraConnection (CqlSession)
 *   listDatasetHandles() → enumerates all keyspaces + tables via Cassandra schema metadata
 *   getDatasetMetadata() → builds Arrow BatchSchema from CQL column types
 *   CassandraRulesFactory → planning rules (ScanCrel → GroupScan → SubScan)
 *   CassandraScanCreator → execution (SubScan → RecordReader → ScanOperator)
 */
public class CassandraStoragePlugin implements StoragePlugin, SupportsListingDatasets {

  private static final Logger logger = LoggerFactory.getLogger(CassandraStoragePlugin.class);

  /**
   * Cassandra system keyspaces that are always hidden from Dremio.
   */
  private static final Set<String> SYSTEM_KEYSPACES = new HashSet<>(Arrays.asList(
      "system", "system_auth", "system_schema",
      "system_distributed", "system_traces",
      "system_virtual_schema", "system_views"
  ));

  private final CassandraStoragePluginConfig config;
  private final PluginSabotContext context;
  private final String name;
  private CassandraConnection connection;

  private final ConcurrentHashMap<String, CachedMetadata> metadataCache  = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, CachedLong>    rowCountCache  = new ConcurrentHashMap<>();

  /**
   * Schema hash cache: maps "keyspace.table" → last-seen column fingerprint.
   *
   * Populated by {@link com.dremio.plugins.cassandra.scan.CassandraRecordReader} on every
   * {@code setup()} call.  When the fingerprint changes, the reader throws a
   * {@code UserException.schemaChangeError()} so Dremio re-plans the query with the
   * new schema.  {@link #invalidateMetadataCache()} is called at the same time to ensure
   * the next {@code getDatasetMetadata()} call sees the fresh schema from the driver.
   */
  private final ConcurrentHashMap<String, String> schemaHashCache = new ConcurrentHashMap<>();

  private static class CachedMetadata {
    final com.datastax.oss.driver.api.core.metadata.Metadata metadata;
    final long expiresAtMs;
    CachedMetadata(com.datastax.oss.driver.api.core.metadata.Metadata metadata, long ttlSeconds) {
      this.metadata = metadata;
      this.expiresAtMs = System.currentTimeMillis() + ttlSeconds * 1000L;
    }
    boolean isExpired() { return System.currentTimeMillis() > expiresAtMs; }
  }

  private static class CachedLong {
    final long value;
    final long expiresAtMs;
    CachedLong(long value, long ttlSeconds) {
      this.value = value;
      this.expiresAtMs = System.currentTimeMillis() + ttlSeconds * 1000L;
    }
    boolean isExpired() { return System.currentTimeMillis() > expiresAtMs; }
  }

  public CassandraStoragePlugin(CassandraStoragePluginConfig config,
                                 PluginSabotContext context,
                                 String name) {
    this.config = config;
    this.context = context;
    this.name = name;
  }

  // -----------------------------------------------------------------------
  // StoragePlugin lifecycle (from Service)
  // -----------------------------------------------------------------------

  @Override
  public void start() throws IOException {
    try {
      connection = new CassandraConnection(config);
    } catch (Exception e) {
      throw new IOException("Failed to connect to Cassandra at " + config.host, e);
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
      return SourceState.badState("Cassandra connection not initialized",
          new RuntimeException("Plugin not started"));
    }
    try {
      connection.getSession().getMetadata().getKeyspaces();
      return SourceState.goodState();
    } catch (Exception e) {
      logger.warn("Cassandra health check failed ({}), attempting reconnect...", e.getMessage());
      try {
        reconnect();
        // Verify the new connection is healthy
        connection.getSession().getMetadata().getKeyspaces();
        logger.info("Cassandra reconnection successful");
        return SourceState.goodState();
      } catch (Exception reconnectEx) {
        return SourceState.badState(
            "Cassandra connection lost and reconnect failed: " + reconnectEx.getMessage(),
            reconnectEx);
      }
    }
  }

  /**
   * Closes the current session and opens a fresh one.
   * Called automatically by {@link #getState()} when a health-check failure is detected,
   * so the source recovers on the next health-check poll without requiring a full Dremio restart.
   */
  private synchronized void reconnect() throws IOException {
    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Exception ignore) {
      // Best-effort close of the broken session
    }
    connection = new CassandraConnection(config);
    // Invalidate all caches so the new session fetches fresh schema and statistics
    metadataCache.clear();
    rowCountCache.clear();
    schemaHashCache.clear();
  }

  /**
   * Invalidates the metadata and row-count caches.
   *
   * Called by {@link com.dremio.plugins.cassandra.scan.CassandraRecordReader} when it
   * detects a live schema change so that the next {@code getDatasetMetadata()} call
   * returns the fresh schema from the DataStax driver rather than a stale cached snapshot.
   */
  public void invalidateMetadataCache() {
    metadataCache.clear();
    rowCountCache.clear();
    logger.info("Metadata cache invalidated due to schema change");
  }

  @Override
  public SourceCapabilities getSourceCapabilities() {
    return SourceCapabilities.NONE;
  }

  @Override
  public Class<? extends StoragePluginRulesFactory> getRulesFactoryClass() {
    return CassandraRulesFactory.class;
  }

  @Override
  public boolean hasAccessPermission(String user, NamespaceKey key, DatasetConfig datasetConfig) {
    // Authentication is handled by the Cassandra cluster itself.
    return true;
  }

  // -----------------------------------------------------------------------
  // SourceMetadata: dataset listing (SupportsListingDatasets)
  // -----------------------------------------------------------------------

  @Override
  public DatasetHandleListing listDatasetHandles(GetDatasetOption... options)
      throws ConnectorException {

    List<DatasetHandle> handles = new ArrayList<>();
    Set<String> excluded = buildExcludedKeyspaces();

    for (Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier, KeyspaceMetadata> ksEntry
        : getCachedMetadata().getKeyspaces().entrySet()) {

      String keyspace = ksEntry.getKey().asInternal();
      if (excluded.contains(keyspace)) {
        continue;
      }

      for (Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier, TableMetadata> tblEntry
          : ksEntry.getValue().getTables().entrySet()) {

        String tableName = tblEntry.getKey().asInternal();
        EntityPath path = new EntityPath(Arrays.asList(name, keyspace, tableName));
        handles.add(new CassandraDatasetHandle(path));
      }
    }

    return () -> handles.iterator();
  }

  // -----------------------------------------------------------------------
  // SourceMetadata: dataset lookup and metadata
  // -----------------------------------------------------------------------

  @Override
  public Optional<DatasetHandle> getDatasetHandle(EntityPath path, GetDatasetOption... options)
      throws ConnectorException {

    List<String> components = path.getComponents();
    if (components.size() < 3) {
      return Optional.empty();
    }
    String keyspace = components.get(1);
    String tableName = components.get(2);

    Optional<KeyspaceMetadata> ks = getCachedMetadata().getKeyspace(keyspace);
    if (!ks.isPresent() || !ks.get().getTable(tableName).isPresent()) {
      return Optional.empty();
    }

    return Optional.of(new CassandraDatasetHandle(path));
  }

  @Override
  public DatasetMetadata getDatasetMetadata(DatasetHandle handle,
                                             PartitionChunkListing chunkListing,
                                             GetMetadataOption... options)
      throws ConnectorException {

    EntityPath path = handle.getDatasetPath();
    List<String> components = path.getComponents();
    String keyspace = components.get(1);
    String tableName = components.get(2);

    KeyspaceMetadata ks = getCachedMetadata().getKeyspace(keyspace)
        .orElseThrow(() -> new ConnectorException("Keyspace not found: " + keyspace));

    TableMetadata table = ks.getTable(tableName)
        .orElseThrow(() -> new ConnectorException(
            "Table not found: " + keyspace + "." + tableName));

    // Build Arrow schema from Cassandra column types
    List<Field> fields = new ArrayList<>();
    for (Map.Entry<com.datastax.oss.driver.api.core.CqlIdentifier, ColumnMetadata> col
        : table.getColumns().entrySet()) {
      String colName = col.getKey().asInternal();
      com.datastax.oss.driver.api.core.type.DataType colType = col.getValue().getType();
      fields.add(CassandraTypeConverter.toArrowField(colName, colType));
    }

    BatchSchema batchSchema = new BatchSchema(fields);

    long estimatedRows = estimateRowCount(keyspace, tableName, table);

    return new CassandraDatasetMetadata(
        batchSchema,
        DatasetStats.of(estimatedRows, 1.0),
        keyspace,
        tableName
    );
  }

  @Override
  public PartitionChunkListing listPartitionChunks(DatasetHandle handle,
                                                    ListPartitionChunkOption... options)
      throws ConnectorException {
    EntityPath path = handle.getDatasetPath();
    List<String> components = path.getComponents();
    String keyspace = components.get(1);
    String tableName = components.get(2);

    int parallelism = Math.max(1, config.splitParallelism);

    // Resolve TableMetadata once — used for partition keys, row count, and byte-size estimate.
    TableMetadata tableForSplits = getCachedMetadata()
        .getKeyspace(keyspace)
        .flatMap(ks -> ks.getTable(tableName))
        .orElse(null);

    List<String> partitionKeys = tableForSplits != null
        ? tableForSplits.getPartitionKey().stream()
              .map(col -> col.getName().asInternal())
              .collect(Collectors.toList())
        : getPartitionKeys(keyspace, tableName);   // fallback

    long estimatedRows    = estimateRowCount(keyspace, tableName, tableForSplits);
    long estimatedRowBytes = tableForSplits != null
        ? estimateRowSizeBytes(tableForSplits) : 100L;
    long rowsPerSplit  = Math.max(1, estimatedRows / parallelism);
    long sizePerSplit  = Math.max(1024L, rowsPerSplit * estimatedRowBytes);

    // Generate evenly-spaced token ranges across the Murmur3 ring.
    // Range: [Long.MIN_VALUE, Long.MAX_VALUE]. Use incremental approach to avoid overflow.
    List<PartitionChunk> chunks = new ArrayList<>();
    // step = floor((MAX - MIN + 1) / parallelism) ≈ floor(2^64 / parallelism)
    // Use Long.MAX_VALUE / parallelism as approximate half-ring step (avoids overflow)
    long step = (parallelism <= 1) ? Long.MAX_VALUE : (Long.MAX_VALUE / parallelism) * 2 + 1;
    long rangeStart = Long.MIN_VALUE;

    for (int i = 0; i < parallelism; i++) {
      long start = rangeStart;
      long end;
      if (i == parallelism - 1) {
        end = Long.MAX_VALUE;
      } else {
        // start + step may overflow; clamp to MAX_VALUE - 1 to allow last range to claim MAX
        long next = start + step;
        end = (next > start) ? next - 1 : Long.MAX_VALUE - 1; // overflow guard
      }
      rangeStart = end + 1; // safe because end < MAX_VALUE for non-last iterations

      // Format: keyspace|tableName|pk1,pk2|start|end
      String specJson = keyspace + "|" + tableName + "|" +
          String.join(",", partitionKeys) + "|" + start + "|" + end;
      byte[] specBytes = specJson.getBytes(java.nio.charset.StandardCharsets.UTF_8);

      DatasetSplit split = DatasetSplit.of(
          java.util.Collections.emptyList(),
          sizePerSplit,
          rowsPerSplit,
          os -> os.write(specBytes)
      );
      chunks.add(PartitionChunk.of(split));
    }

    return () -> chunks.iterator();
  }

  @Override
  public boolean containerExists(EntityPath path, GetMetadataOption... options) {
    List<String> components = path.getComponents();
    if (components.size() < 2) {
      return false;
    }
    String keyspace = components.get(1);
    return getCachedMetadata().getKeyspace(keyspace).isPresent();
  }

  // -----------------------------------------------------------------------
  // Accessors used by planning and execution layers
  // -----------------------------------------------------------------------

  public CassandraConnection getConnection() {
    return connection;
  }

  public CassandraStoragePluginConfig getConfig() {
    return config;
  }

  /**
   * Returns the last-seen schema fingerprint for the given table, or {@code null}
   * if this table has not yet been read during this plugin's lifetime.
   *
   * @param tableKey  "keyspace.tablename"
   */
  public String getSchemaHash(String tableKey) {
    return schemaHashCache.get(tableKey);
  }

  /**
   * Updates the schema fingerprint for the given table.
   *
   * @param tableKey  "keyspace.tablename"
   * @param hash      fingerprint produced by {@code CassandraRecordReader.computeSchemaHash()}
   */
  public void updateSchemaHash(String tableKey, String hash) {
    schemaHashCache.put(tableKey, hash);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private com.datastax.oss.driver.api.core.metadata.Metadata getCachedMetadata() {
    CachedMetadata cached = metadataCache.get("metadata");
    if (cached == null || cached.isExpired()) {
      com.datastax.oss.driver.api.core.metadata.Metadata fresh =
          connection.getSession().getMetadata();
      metadataCache.put("metadata", new CachedMetadata(fresh, config.metadataCacheTtlSeconds));
      return fresh;
    }
    return cached.metadata;
  }

  /**
   * Estimates the row count for a Cassandra table using {@code system.size_estimates}.
   *
   * <h4>Algorithm</h4>
   * <ol>
   *   <li>Query {@code system.size_estimates} (parameterized — no string concat) for all
   *       token-range rows of this table.  Each row contains
   *       {@code partitions_count} and {@code mean_partition_size} (bytes).</li>
   *   <li>Sum {@code partitions_count × mean_partition_size} across all ranges → total
   *       estimated data bytes for the table.</li>
   *   <li>Divide by the schema-aware per-row size (see {@link #estimateRowSizeBytes})
   *       to get an estimated row count.</li>
   *   <li>Floor the result at the total {@code partitions_count}: every partition contains
   *       at least one row, so the row count can never be less than the partition count.</li>
   *   <li>If {@code system.size_estimates} returns no rows (empty table, table was just
   *       created, or no major compaction has run yet), fall back to {@code 10 000} rows.</li>
   * </ol>
   *
   * <p>Results are cached per table with the same TTL as the schema metadata cache
   * ({@link CassandraStoragePluginConfig#metadataCacheTtlSeconds}).  The cache is
   * invalidated on reconnect, which also clears the metadata cache.
   *
   * @param keyspace      Cassandra keyspace name
   * @param tableName     Cassandra table name
   * @param tableMeta     live {@link TableMetadata} for schema-aware row-size estimation;
   *                      may be {@code null} to use a conservative 100 bytes/row fallback
   * @return estimated row count; always ≥ 1
   */
  private long estimateRowCount(String keyspace, String tableName, TableMetadata tableMeta) {
    String cacheKey = keyspace + "." + tableName;
    CachedLong cached = rowCountCache.get(cacheKey);
    if (cached != null && !cached.isExpired()) {
      return cached.value;
    }

    long estimate = estimateRowCountUncached(keyspace, tableName, tableMeta);
    rowCountCache.put(cacheKey, new CachedLong(estimate, config.metadataCacheTtlSeconds));
    return estimate;
  }

  private long estimateRowCountUncached(String keyspace, String tableName, TableMetadata tableMeta) {
    try {
      // Parameterized query — no string concatenation / CQL injection risk.
      SimpleStatement stmt = SimpleStatement.builder(
          "SELECT partitions_count, mean_partition_size " +
          "FROM system.size_estimates " +
          "WHERE keyspace_name = ? AND table_name = ?")
          .addPositionalValues(keyspace, tableName)
          .build();

      long estimatedRowBytes = (tableMeta != null) ? estimateRowSizeBytes(tableMeta) : 100L;
      long totalDataBytes  = 0;
      long totalPartitions = 0;
      boolean hasData = false;

      for (Row row : connection.getSession().execute(stmt)) {
        hasData = true;
        long partitions = row.getLong("partitions_count");
        long meanSize   = row.getLong("mean_partition_size");
        totalPartitions += partitions;
        totalDataBytes  += partitions * meanSize;
      }

      if (!hasData || totalPartitions == 0) {
        // system.size_estimates is empty: table was just created, no compaction yet, or
        // a virtual/small table.  Use a conservative fallback.
        logger.debug("system.size_estimates has no data for {}.{} — falling back to 10,000 rows",
            keyspace, tableName);
        return 10_000L;
      }

      // Rows = total bytes / bytes-per-row, floored at partition count
      // (every partition has at least 1 row).
      long byteBasedEstimate = totalDataBytes / Math.max(1, estimatedRowBytes);
      long result = Math.max(totalPartitions, byteBasedEstimate);

      logger.debug(
          "Row count estimate for {}.{}: {} rows " +
          "({} partitions, {}B avg row size, {}B total data)",
          keyspace, tableName, result,
          totalPartitions, estimatedRowBytes, totalDataBytes);
      return result;

    } catch (Exception e) {
      logger.debug("Could not estimate row count for {}.{}: {}",
          keyspace, tableName, e.getMessage());
      return 10_000L;
    }
  }

  /**
   * Estimates the average serialised size of one row in a Cassandra table by
   * summing type-based byte estimates for each column and adding Cassandra's
   * internal per-row and per-cell overhead.
   *
   * <p>These are intentionally conservative averages — not byte-perfect — but
   * they are far more accurate than a flat "100 bytes per row" for wide tables
   * with large text or collection columns, and for narrow tables of small primitives.
   *
   * <p>Overhead model: 8 bytes row header + 8 bytes cell overhead per column.
   *
   * @param table  live table schema
   * @return estimated bytes per row; always ≥ 32
   */
  public static long estimateRowSizeBytes(TableMetadata table) {
    long bytes = 0;
    for (ColumnMetadata col : table.getColumns().values()) {
      bytes += estimateColumnBytes(col.getType());
    }
    // Cassandra SSTable overhead: row header (8) + per-cell tombstone/timestamp overhead (8 each)
    bytes += 8 + 8L * table.getColumns().size();
    return Math.max(32, bytes);
  }

  /**
   * Returns an estimated serialised byte size for a single value of the given Cassandra type.
   *
   * <p>Variable-length types (TEXT, BLOB, collections) use typical averages; if a deployment
   * has unusually large values the planner will underestimate the table size, which is
   * acceptable — it causes the planner to be slightly more aggressive about parallelism,
   * which is the safer direction.
   */
  static long estimateColumnBytes(DataType type) {
    // Fixed-width primitive types
    if (type == DataTypes.BOOLEAN || type == DataTypes.TINYINT) return 1;
    if (type == DataTypes.SMALLINT)                              return 2;
    if (type == DataTypes.INT || type == DataTypes.FLOAT
        || type == DataTypes.DATE)                               return 4;
    if (type == DataTypes.BIGINT   || type == DataTypes.DOUBLE
        || type == DataTypes.TIMESTAMP || type == DataTypes.TIME
        || type == DataTypes.COUNTER)                            return 8;
    if (type == DataTypes.UUID || type == DataTypes.TIMEUUID)    return 16;
    if (type == DataTypes.DECIMAL)                               return 16;
    if (type == DataTypes.VARINT)                                return 8;   // variable; avg ~8
    if (type == DataTypes.INET)                                  return 4;   // IPv4 common case

    // Variable-length scalar types — use typical averages
    if (type == DataTypes.TEXT || type == DataTypes.ASCII)       return 20;  // avg short string
    if (type == DataTypes.BLOB)                                  return 64;  // avg blob

    // Collection types — assume a small number of elements with typical element sizes
    if (type instanceof ListType || type instanceof SetType)     return 48;  // ~3 elems × 16 B
    if (type instanceof MapType)                                 return 96;  // ~3 entries × 32 B
    if (type instanceof UserDefinedType)                         return 48;  // ~3 fields × 16 B

    return 16;  // conservative default for unknown / tuple types
  }

  private List<String> getPartitionKeys(String keyspace, String tableName) {
    try {
      return getCachedMetadata()
          .getKeyspace(keyspace)
          .flatMap(ks -> ks.getTable(tableName))
          .map(tbl -> tbl.getPartitionKey().stream()
              .map(col -> col.getName().asInternal())
              .collect(Collectors.toList()))
          .orElse(Collections.emptyList());
    } catch (Exception e) {
      logger.warn("Could not get partition keys for {}.{}: {}", keyspace, tableName, e.getMessage());
      return Collections.emptyList();
    }
  }

  private Set<String> buildExcludedKeyspaces() {
    Set<String> excluded = new HashSet<>(SYSTEM_KEYSPACES);
    if (config.excludedKeyspaces != null && !config.excludedKeyspaces.isEmpty()) {
      for (String ks : config.excludedKeyspaces.split(",")) {
        String trimmed = ks.trim().toLowerCase();
        if (!trimmed.isEmpty()) {
          excluded.add(trimmed);
        }
      }
    }
    return excluded;
  }
}
