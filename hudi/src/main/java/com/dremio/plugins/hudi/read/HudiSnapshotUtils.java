package com.dremio.plugins.hudi.read;

import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Utility class that reads the Apache Hudi timeline and file system view
 * to determine which files should be read for a given table.
 *
 * Why this is needed
 * ------------------
 * A Hudi table directory contains many generations of files: base Parquet
 * files, Avro/HFile delta logs, and .hoodie metadata. Naively listing the
 * directory and reading every Parquet file would return stale or duplicate
 * data. This class uses Hudi's own timeline and file group APIs to identify:
 *
 *   COW tables: the set of latest base Parquet files (one per file group)
 *   MOR tables: the set of latest (base file + associated log files) per
 *               file group — the "file slice" — which must be merged to
 *               produce the correct current snapshot.
 *
 * Snapshot caching
 * ----------------
 * Building a HoodieTableMetaClient + HoodieTableFileSystemView + partition
 * walk is expensive (multiple filesystem round-trips to read the .hoodie/
 * timeline). On a table with N Parquet files, Dremio creates N splits and
 * calls HudiParquetRecordReader.setup() N times — without caching, this
 * means N full timeline reads of identical data.
 *
 * HudiSnapshotUtils maintains a JVM-level {@link #SNAPSHOT_CACHE} keyed by
 * table root path. The first split for a given table pays the full cost;
 * subsequent splits in the same scan (and within the TTL window) get a free
 * cache hit. The TTL is 60 seconds — long enough for a Dremio query to
 * complete, short enough to pick up the next write operation promptly.
 *
 * After a successful write, {@link #invalidateCache(String)} should be called
 * so the immediately following SELECT sees fresh metadata.
 *
 * Usage
 * -----
 *   // Get the latest base Parquet files for a COW table (cached)
 *   List<String> paths = HudiSnapshotUtils.getLatestBaseFilePaths(tablePath, conf);
 *
 *   // Get the latest file slices (base + logs) for a MOR table (cached)
 *   List<FileSlice> slices = HudiSnapshotUtils.getLatestFileSlices(tablePath, conf);
 *
 *   // Get the table type (cached)
 *   HoodieTableType type = HudiSnapshotUtils.getTableType(tablePath, conf);
 *
 *   // Invalidate after a write so the next read is fresh
 *   HudiSnapshotUtils.invalidateCache(tablePath);
 */
public final class HudiSnapshotUtils {

  private static final Logger logger = LoggerFactory.getLogger(HudiSnapshotUtils.class);

  private HudiSnapshotUtils() {}

  // -----------------------------------------------------------------------
  // Snapshot cache
  // -----------------------------------------------------------------------

  /**
   * Cache TTL in milliseconds. 60 seconds balances read performance against
   * freshness:
   *   - A Dremio query on a single table typically completes within this window,
   *     so all N splits see an identical, consistent snapshot.
   *   - Between queries the cache naturally expires, ensuring the next query
   *     always reads fresh metadata even if the table was written to externally.
   *   - After an internal write, {@link #invalidateCache} is called explicitly
   *     so the immediately following SELECT doesn't see stale data.
   */
  static final long CACHE_TTL_MS = 60_000L;

  /**
   * JVM-level snapshot cache keyed by absolute table root path.
   * ConcurrentHashMap: multiple executor threads may call setup() in parallel
   * for different splits of the same table scan.
   *
   * Package-private for testing.
   */
  static final ConcurrentHashMap<String, CachedSnapshot> SNAPSHOT_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Immutable value object holding all expensive-to-compute snapshot state for
   * one table. Built once per table per TTL window; shared by all readers.
   *
   * Package-private for testing.
   */
  static final class CachedSnapshot {
    /** Absolute paths of all active base Parquet files (one per file group). */
    final List<String>    baseFilePaths;
    /** Latest file slice per file group (base file + log files). */
    final List<FileSlice> fileSlices;
    /** COW or MOR. */
    final HoodieTableType tableType;

    private final long expiresAtMs;

    CachedSnapshot(List<String> baseFilePaths, List<FileSlice> fileSlices,
                   HoodieTableType tableType) {
      this.baseFilePaths = Collections.unmodifiableList(new ArrayList<>(baseFilePaths));
      this.fileSlices    = Collections.unmodifiableList(new ArrayList<>(fileSlices));
      this.tableType     = tableType;
      this.expiresAtMs   = System.currentTimeMillis() + CACHE_TTL_MS;
    }

    boolean isExpired() {
      return System.currentTimeMillis() > expiresAtMs;
    }
  }

  /**
   * Returns the cached snapshot for {@code tablePath}, building a fresh one if
   * the cache is empty or the TTL has expired.
   *
   * This is the single point where the expensive Hudi metadata read happens.
   * All public methods delegate here, so the cost is paid at most once per
   * table per TTL window regardless of how many splits the table has.
   *
   * Thread safety: benign race on cache miss — if two threads both see an
   * expired entry, both may compute a fresh snapshot concurrently. The later
   * {@code put()} simply overwrites with an equivalent result. No locking is
   * needed because both computations produce identical data for the same table
   * and timestamp.
   */
  private static CachedSnapshot getOrBuildSnapshot(
      String tablePath, Configuration conf) throws IOException {

    CachedSnapshot cached = SNAPSHOT_CACHE.get(tablePath);
    if (cached != null && !cached.isExpired()) {
      logger.debug("Snapshot cache HIT  table={} slices={} baseFiles={}",
          tablePath, cached.fileSlices.size(), cached.baseFilePaths.size());
      return cached;
    }

    logger.debug("Snapshot cache MISS table={} — reading Hudi timeline", tablePath);

    // One unified read: build metaClient, load timeline, scan partitions.
    // All derived data (tableType, fileSlices, baseFilePaths) comes from this
    // single pass so we only open filesystem connections once.
    HoodieTableMetaClient metaClient = buildMetaClient(tablePath, conf);
    HoodieTableType tableType = metaClient.getTableType();

    HoodieTimeline completedTimeline = metaClient.getCommitsAndCompactionTimeline()
        .filterCompletedInstants();

    List<FileSlice> fileSlices;
    List<String>    baseFilePaths;

    if (!completedTimeline.lastInstant().isPresent()) {
      logger.warn("No completed instants found at {}. Table may be empty.", tablePath);
      fileSlices    = Collections.emptyList();
      baseFilePaths = Collections.emptyList();
    } else {
      String instantTime = completedTimeline.lastInstant().get().getTimestamp();
      HoodieTableFileSystemView fsView = buildFileSystemView(metaClient);
      try {
        fileSlices    = new ArrayList<>();
        baseFilePaths = new ArrayList<>();
        for (String partition : listPartitions(metaClient, fsView)) {
          fsView.getLatestFileSlicesBeforeOrOn(partition, instantTime, true)
              .forEach(slice -> {
                fileSlices.add(slice);
                if (slice.getBaseFile().isPresent()) {
                  baseFilePaths.add(slice.getBaseFile().get().getPath());
                }
              });
        }
      } finally {
        fsView.close();
      }
    }

    CachedSnapshot fresh = new CachedSnapshot(baseFilePaths, fileSlices, tableType);
    SNAPSHOT_CACHE.put(tablePath, fresh);

    logger.info("Snapshot cached  table={}  type={}  slices={}  baseFiles={}  ttl={}s",
        tablePath, tableType, fileSlices.size(), baseFilePaths.size(),
        CACHE_TTL_MS / 1000);

    return fresh;
  }

  /**
   * Removes the cached snapshot for the given table path, forcing the next
   * read to reload metadata from the Hudi timeline.
   *
   * Should be called by {@code HudiRecordWriter.close()} after a successful
   * commit so that a SELECT immediately following a write sees fresh data.
   *
   * @param tablePath  Absolute path of the Hudi table root
   */
  public static void invalidateCache(String tablePath) {
    CachedSnapshot removed = SNAPSHOT_CACHE.remove(tablePath);
    if (removed != null) {
      logger.debug("Snapshot cache invalidated for table: {}", tablePath);
    }
  }

  // -----------------------------------------------------------------------
  // Table metadata
  // -----------------------------------------------------------------------

  /**
   * Returns the Hudi table type (COPY_ON_WRITE or MERGE_ON_READ).
   * Result is served from the snapshot cache; the underlying
   * {@code .hoodie/hoodie.properties} read only happens on cache miss.
   *
   * @param tablePath  Root path of the Hudi table (contains .hoodie/)
   * @param conf       Hadoop configuration (needed for S3/ADLS filesystem access)
   */
  public static HoodieTableType getTableType(
      String tablePath, Configuration conf) throws IOException {
    return getOrBuildSnapshot(tablePath, conf).tableType;
  }

  /**
   * Reads the Hudi table name from hoodie.properties.
   * Not on the critical read path; bypasses the cache.
   */
  public static String getTableName(String tablePath, Configuration conf) throws IOException {
    return buildMetaClient(tablePath, conf).getTableConfig().getTableName();
  }

  // -----------------------------------------------------------------------
  // File listing: COW tables
  // -----------------------------------------------------------------------

  /**
   * Returns the latest base (Parquet) file for each file group in the table,
   * as of the most recent committed instant on the active timeline.
   *
   * For COPY_ON_WRITE tables these files contain the complete current snapshot
   * and can be read directly by any standard Parquet reader.
   *
   * For MERGE_ON_READ tables, these are only the compacted base files and will
   * miss any updates written as delta logs since the last compaction. Use
   * {@link #getLatestFileSlices} instead for MOR tables to get complete data.
   *
   * Result is derived from the snapshot cache; no filesystem I/O on cache hit.
   *
   * @param tablePath  Root path of the Hudi table
   * @param conf       Hadoop configuration
   * @return List of HoodieBaseFile objects, one per active file group
   */
  public static List<HoodieBaseFile> getLatestBaseFiles(
      String tablePath, Configuration conf) throws IOException {
    return getOrBuildSnapshot(tablePath, conf).fileSlices.stream()
        .filter(s -> s.getBaseFile().isPresent())
        .map(s -> s.getBaseFile().get())
        .collect(Collectors.toList());
  }

  /**
   * Convenience overload: lists all base file paths as strings.
   * Suitable for passing directly to snapshot validation in HudiParquetRecordReader.
   * Served from the snapshot cache.
   */
  public static List<String> getLatestBaseFilePaths(
      String tablePath, Configuration conf) throws IOException {
    return getOrBuildSnapshot(tablePath, conf).baseFilePaths;
  }

  // -----------------------------------------------------------------------
  // File listing: MOR tables (base + delta logs)
  // -----------------------------------------------------------------------

  /**
   * Returns the latest file slice for each file group — the set of files that
   * together represent the current snapshot as of the last committed instant.
   *
   * A FileSlice contains:
   *   - baseFile (Optional): the last compacted Parquet base file
   *   - logFiles: zero or more delta log files written since last compaction
   *
   * For COPY_ON_WRITE tables the log files are always empty; this method works
   * for both table types and is the preferred listing method for any format.
   *
   * Served from the snapshot cache; no filesystem I/O on cache hit.
   *
   * @param tablePath  Root path of the Hudi table
   * @param conf       Hadoop configuration
   * @return List of FileSlice objects; each contains base file + log files
   */
  public static List<FileSlice> getLatestFileSlices(
      String tablePath, Configuration conf) throws IOException {
    return getOrBuildSnapshot(tablePath, conf).fileSlices;
  }

  /**
   * Returns true if any file slice in the table has unapplied log files —
   * i.e., the table has MOR-style pending updates that require log merging.
   * Served from the snapshot cache.
   */
  public static boolean hasPendingLogFiles(String tablePath, Configuration conf) throws IOException {
    return getLatestFileSlices(tablePath, conf).stream()
        .anyMatch(s -> s.getLogFiles().findAny().isPresent());
  }

  // -----------------------------------------------------------------------
  // Timeline inspection
  // -----------------------------------------------------------------------

  /**
   * Returns the timestamp of the latest completed commit/compaction instant,
   * or empty if the table has no completed instants.
   * Not on the critical read path; bypasses the cache.
   */
  public static Optional<String> getLatestCommitTimestamp(
      String tablePath, Configuration conf) throws IOException {
    HoodieTableMetaClient metaClient = buildMetaClient(tablePath, conf);
    org.apache.hudi.common.util.Option<HoodieInstant> last = metaClient
        .getCommitsAndCompactionTimeline()
        .filterCompletedInstants()
        .lastInstant();
    return last.isPresent()
        ? Optional.of(last.get().getTimestamp())
        : Optional.empty();
  }

  /**
   * Returns true if the table at the given path is a valid Hudi table.
   * Not on the critical read path; bypasses the cache.
   */
  public static boolean isHudiTable(String tablePath, Configuration conf) {
    try {
      HoodieTableMetaClient metaClient = buildMetaClient(tablePath, conf);
      return metaClient.getStorage().exists(
          new StoragePath(tablePath + "/.hoodie/hoodie.properties"));
    } catch (IOException e) {
      return false;
    }
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Builds a HoodieTableMetaClient by reading .hoodie/hoodie.properties.
   * Public so HudiParquetRecordReader can use it directly when constructing
   * HudiMORRecordReader instances (which need the metaClient, not just paths).
   */
  public static HoodieTableMetaClient buildMetaClient(
      String tablePath, Configuration conf) throws IOException {
    try {
      return HoodieTableMetaClient.builder()
          .setConf(new HadoopStorageConfiguration(conf))
          .setBasePath(tablePath)
          .setLoadActiveTimelineOnLoad(true)
          .build();
    } catch (Exception e) {
      throw new IOException(
          "Failed to build HoodieTableMetaClient for path: " + tablePath
              + ". Verify that the path is a valid Hudi table root (contains .hoodie/).", e);
    }
  }

  /**
   * Builds a HoodieTableFileSystemView for the given MetaClient.
   * Must be closed after use to release filesystem handles.
   */
  private static HoodieTableFileSystemView buildFileSystemView(
      HoodieTableMetaClient metaClient) throws IOException {
    try {
      return FileSystemViewManager.createInMemoryFileSystemView(
          new org.apache.hudi.common.engine.HoodieLocalEngineContext(metaClient.getStorageConf()),
          metaClient,
          HoodieMetadataConfig.newBuilder().build());
    } catch (Exception e) {
      throw new IOException("Failed to create HoodieTableFileSystemView", e);
    }
  }

  /**
   * Lists all partition paths for the table.
   * For non-partitioned tables, returns a list with a single empty string "".
   */
  private static List<String> listPartitions(
      HoodieTableMetaClient metaClient,
      HoodieTableFileSystemView fsView) {
    try {
      List<String> partitions = org.apache.hudi.common.fs.FSUtils.getAllPartitionPaths(
          new org.apache.hudi.common.engine.HoodieLocalEngineContext(metaClient.getStorageConf()),
          metaClient.getStorage(),
          metaClient.getBasePath(),
          false,
          false);

      return partitions.isEmpty()
          ? Collections.singletonList("")
          : partitions;
    } catch (Exception e) {
      logger.warn("Could not list partitions for table at {}. Falling back to root partition. Error: {}",
          metaClient.getBasePath(), e.getMessage());
      return Collections.singletonList("");
    }
  }
}
