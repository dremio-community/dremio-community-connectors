package com.dremio.plugins.delta.read;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.DeltaScan;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.data.CloseableIterator;
import io.delta.standalone.data.RowRecord;
import io.delta.standalone.expressions.Expression;
import io.delta.standalone.types.StructType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * Utility class that reads the Delta Lake transaction log to determine
 * which Parquet files represent the current snapshot of a table.
 *
 * Why this is needed
 * ------------------
 * A Delta table directory contains many generations of Parquet files:
 * some current, some superseded by later writes or deletes. Naively
 * listing the directory and reading every .parquet file would produce
 * stale or duplicate data.
 *
 * Delta's _delta_log/ directory contains JSON (and periodically Parquet
 * checkpoint) files that record every ADD and REMOVE action. The current
 * snapshot is derived from the latest checkpoint + all subsequent JSON
 * commits. DeltaSnapshotUtils uses delta-standalone to perform this
 * computation and return only the active file set.
 *
 * This is fundamentally simpler than Hudi's timeline model — Delta has
 * no concept of base files + delta logs; every file is a self-contained
 * Parquet file representing a set of rows.
 *
 * Usage
 * -----
 *   // Get all active Parquet file paths for Dremio to scan
 *   List<String> files = DeltaSnapshotUtils.getLatestFilePaths(tablePath, hadoopConf);
 *
 *   // Get files matching a partition filter
 *   List<AddFile> files = DeltaSnapshotUtils.getFilesMatchingFilter(tablePath, expr, conf);
 *
 *   // Get table schema for schema inference
 *   StructType schema = DeltaSnapshotUtils.getTableSchema(tablePath, conf);
 */
public final class DeltaSnapshotUtils {

  private static final Logger logger = LoggerFactory.getLogger(DeltaSnapshotUtils.class);

  private DeltaSnapshotUtils() {}

  // -----------------------------------------------------------------------
  // Snapshot cache
  // -----------------------------------------------------------------------

  /**
   * Default cache TTL (60 seconds). Overridden per-call via cacheTtlMs.
   */
  static final long DEFAULT_CACHE_TTL_MS = 60_000L;

  /**
   * JVM-level snapshot cache keyed by canonical table path.
   * ConcurrentHashMap: executor threads call setup() in parallel for different
   * splits of the same table. Benign race on miss — two threads may both build
   * a fresh snapshot; the later put() wins. No locking needed; both results
   * are equivalent for the same table at the same point in time.
   */
  static final ConcurrentHashMap<String, CachedDeltaSnapshot> SNAPSHOT_CACHE =
      new ConcurrentHashMap<>();

  /**
   * Immutable cached snapshot for one Delta table.
   *
   * Holds resolved absolute file paths (relative paths from Delta log are
   * resolved at build time so each cache hit pays no path work), the Delta
   * version at build time (used for the cheap version check on subsequent
   * hits), and the wall-clock expiry for TTL-based eviction.
   */
  static final class CachedDeltaSnapshot {
    /** Absolute paths of all AddFile entries in the current snapshot. */
    final List<String>  filePaths;
    /** Full AddFile objects (includes partition values, size, stats). */
    final List<AddFile> files;
    /** Delta commit version at the time this snapshot was built. */
    final long          version;

    private final long expiresAtMs;

    CachedDeltaSnapshot(List<AddFile> files, long version, long ttlMs) {
      List<AddFile> immutableFiles = Collections.unmodifiableList(new ArrayList<>(files));
      this.files      = immutableFiles;
      this.filePaths  = Collections.unmodifiableList(
          immutableFiles.stream().map(AddFile::getPath).collect(Collectors.toList()));
      this.version    = version;
      this.expiresAtMs = System.currentTimeMillis() + ttlMs;
    }

    boolean isExpired() { return System.currentTimeMillis() > expiresAtMs; }
  }

  /**
   * Returns the snapshot for {@code tablePath}, using a two-layer freshness
   * strategy:
   *
   *   Layer 1 — version check (cheap):
   *     On a cache hit, tests whether a commit file for {@code cachedVersion+1}
   *     exists in {@code _delta_log/}. This is a single filesystem stat — one
   *     file-exists call on local FS, one HEAD request on S3/ADLS. If a newer
   *     commit exists the snapshot is rebuilt immediately regardless of TTL.
   *     This catches external writers (Spark, other engines) without waiting
   *     for the TTL to expire.
   *
   *   Layer 2 — TTL fallback:
   *     Even if the version check finds no newer commit, a sufficiently old
   *     cache entry is evicted and rebuilt. This bounds memory retention and
   *     handles edge cases where the version check cannot inspect the log
   *     (e.g., permission errors, unusual Delta log layouts).
   *
   *   Write invalidation:
   *     {@link #invalidateCache(String)} is called by DeltaRecordWriter after
   *     every successful commit, so writes through this connector are always
   *     immediately visible to the next read.
   *
   * @param tablePath  Delta table root (contains _delta_log/)
   * @param conf       Hadoop configuration
   * @param ttlMs      Cache TTL in milliseconds (0 = disabled; always rebuild)
   */
  static CachedDeltaSnapshot getOrBuildSnapshot(
      String tablePath, Configuration conf, long ttlMs) throws IOException {

    String key = canonicalize(tablePath);

    if (ttlMs > 0) {
      CachedDeltaSnapshot cached = SNAPSHOT_CACHE.get(key);
      if (cached != null && !cached.isExpired()) {
        // Layer 1: cheap version check — does the next commit file exist?
        if (!hasNewerCommit(tablePath, cached.version, conf)) {
          logger.debug("Delta snapshot cache HIT  key={}  version={}  files={}",
              key, cached.version, cached.filePaths.size());
          return cached;
        }
        logger.debug("Delta snapshot cache STALE (new commit detected)  key={}  "
            + "cachedVersion={}", key, cached.version);
      }
    }

    logger.debug("Delta snapshot cache MISS  key={} — reading Delta log", key);

    DeltaLog log = openLog(tablePath, conf);
    Snapshot snapshot = log.snapshot();

    if (snapshot.getVersion() < 0) {
      logger.warn("Delta table at {} has no commits yet.", tablePath);
      CachedDeltaSnapshot empty = new CachedDeltaSnapshot(Collections.emptyList(), -1L, ttlMs);
      if (ttlMs > 0) SNAPSHOT_CACHE.put(key, empty);
      return empty;
    }

    List<AddFile> files = new ArrayList<>();
    try (CloseableIterator<AddFile> it = snapshot.scan().getFiles()) {
      while (it.hasNext()) {
        AddFile f = it.next();
        // Resolve relative paths to absolute once at cache-build time.
        String abs = toAbsolutePath(f.getPath(), tablePath);
        files.add(abs.equals(f.getPath()) ? f
            : new AddFile(abs, f.getPartitionValues(), f.getSize(),
                          f.getModificationTime(), f.isDataChange(),
                          f.getStats(), f.getTags()));
      }
    } catch (Exception e) {
      throw new IOException("Failed to list files in Delta snapshot at " + tablePath, e);
    }

    CachedDeltaSnapshot fresh = new CachedDeltaSnapshot(files, snapshot.getVersion(), ttlMs);
    if (ttlMs > 0) SNAPSHOT_CACHE.put(key, fresh);

    logger.info("Delta snapshot cached  key={}  version={}  files={}  ttl={}s",
        key, snapshot.getVersion(), files.size(), ttlMs / 1000);

    return fresh;
  }

  /**
   * Removes the cached snapshot for the given table path, forcing the next
   * read to reload from the Delta log.
   *
   * Called by DeltaRecordWriter after every successful commit so that a SELECT
   * immediately following a write always sees the just-committed data.
   */
  public static void invalidateCache(String tablePath) {
    String key = canonicalize(tablePath);
    CachedDeltaSnapshot removed = SNAPSHOT_CACHE.remove(key);
    if (removed != null) {
      logger.debug("Delta snapshot cache invalidated  key={}", key);
    }
  }

  /**
   * Converts a table root path to a canonical cache key.
   *
   * Normalizes path separators, removes trailing slashes, and collapses
   * {@code .} / {@code ..} components so that {@code file:///data/table},
   * {@code /data/table}, and {@code /data/other/../table} all produce the
   * same key and share a single cache entry.
   *
   * Best-effort: for object stores (S3, ADLS), normalizes URI scheme and
   * path but cannot resolve symlinks. The result is always a non-null string
   * safe to use as a ConcurrentHashMap key.
   */
  static String canonicalize(String tablePath) {
    if (tablePath == null || tablePath.isEmpty()) return "";
    try {
      java.net.URI uri = java.net.URI.create(tablePath.trim()).normalize();
      String scheme = uri.getScheme();
      if (scheme == null || scheme.equals("file")) {
        // Local filesystem: resolve to real absolute path
        return java.nio.file.Paths.get(uri.getSchemeSpecificPart() != null
            && !uri.getSchemeSpecificPart().isEmpty()
            ? uri.getSchemeSpecificPart() : tablePath.trim())
            .normalize().toString();
      }
      // Object store: normalize URI (removes ./ ../ and trailing slash)
      String authority = uri.getAuthority() != null ? uri.getAuthority() : "";
      String path = uri.getPath() != null ? uri.getPath() : "";
      // Strip trailing slash from path (but not scheme-authority root "/")
      if (path.length() > 1 && path.endsWith("/")) path = path.substring(0, path.length() - 1);
      return scheme + "://" + authority + path;
    } catch (Exception e) {
      // Unparseable path — use trimmed original as key (still safe)
      return tablePath.trim();
    }
  }

  /**
   * Checks whether a newer commit exists in the Delta log beyond the given
   * cached version. Returns {@code true} if the table has changed, meaning
   * the caller should rebuild the snapshot.
   *
   * Implementation: tests whether {@code _delta_log/{version+1:020d}.json}
   * exists. This is a single file-exists call — one stat on local FS, one
   * HEAD request on S3/ADLS — making it cheap enough to run on every cache
   * hit without measurable overhead.
   *
   * Conservatively returns {@code true} (rebuild) on any I/O error, so a
   * transient filesystem failure never causes a permanently stale cache.
   */
  static boolean hasNewerCommit(String tablePath, long cachedVersion, Configuration conf) {
    if (cachedVersion < 0) return true; // empty table; always check
    try {
      FileSystem fs = FileSystem.get(new Path(tablePath).toUri(), conf);
      String nextFile = String.format("%020d.json", cachedVersion + 1);
      return fs.exists(new Path(tablePath + "/_delta_log/" + nextFile));
    } catch (Exception e) {
      logger.debug("Version check failed for {}; assuming stale: {}", tablePath, e.getMessage());
      return true; // safe default: rebuild on any error
    }
  }

  // -----------------------------------------------------------------------
  // Core file listing
  // -----------------------------------------------------------------------

  /**
   * Returns all active Parquet file paths in the current Delta snapshot.
   * Served from the snapshot cache (version-check + TTL strategy).
   *
   * @param tablePath  Root path of the Delta table (contains _delta_log/)
   * @param conf       Hadoop configuration
   * @param cacheTtlMs Cache TTL in milliseconds (0 = always rebuild)
   */
  public static List<String> getLatestFilePaths(
      String tablePath, Configuration conf, long cacheTtlMs) throws IOException {
    return getOrBuildSnapshot(tablePath, conf, cacheTtlMs).filePaths;
  }

  /**
   * Convenience overload using the default 60-second TTL.
   * Prefer the 3-arg version when DeltaPluginConfig is available.
   */
  public static List<String> getLatestFilePaths(String tablePath, Configuration conf)
      throws IOException {
    return getLatestFilePaths(tablePath, conf, DEFAULT_CACHE_TTL_MS);
  }

  /**
   * Returns AddFile objects for all active files. Served from the cache.
   *
   * @param cacheTtlMs Cache TTL in milliseconds (0 = always rebuild)
   */
  public static List<AddFile> getLatestFiles(
      String tablePath, Configuration conf, long cacheTtlMs) throws IOException {
    return getOrBuildSnapshot(tablePath, conf, cacheTtlMs).files;
  }

  /** Convenience overload using the default 60-second TTL. */
  public static List<AddFile> getLatestFiles(String tablePath, Configuration conf)
      throws IOException {
    return getLatestFiles(tablePath, conf, DEFAULT_CACHE_TTL_MS);
  }

  /**
   * Returns only the files relevant to a given partition filter expression.
   * Note: partition pruning requires calling delta-standalone's scan(filterExpr),
   * which bypasses the file-list cache. The cache is still used for the
   * no-filter path via getLatestFiles().
   *
   * Delta's partition pruning can eliminate entire files when the query
   * predicate constrains partition columns. For non-partitioned tables or
   * when no filter is provided, all files are returned.
   *
   * @param tablePath    Root path of the Delta table
   * @param filterExpr   Delta filter expression (e.g. EqualTo("year", 2025))
   *                     Pass null to return all files.
   * @param conf         Hadoop configuration
   * @return Filtered list of AddFile objects
   */
  public static List<AddFile> getFilesMatchingFilter(
      String tablePath, Expression filterExpr, Configuration conf) throws IOException {

    DeltaLog log = openLog(tablePath, conf);
    Snapshot snapshot = log.snapshot();

    if (snapshot.getVersion() < 0) return Collections.emptyList();

    List<AddFile> files = new ArrayList<>();
    DeltaScan scan = (filterExpr != null)
        ? snapshot.scan(filterExpr)
        : snapshot.scan();

    try (CloseableIterator<AddFile> it = scan.getFiles()) {
      while (it.hasNext()) files.add(it.next());
    } catch (Exception e) {
      throw new IOException("Failed to apply filter scan at " + tablePath, e);
    }
    return files;
  }

  // -----------------------------------------------------------------------
  // Table metadata
  // -----------------------------------------------------------------------

  /**
   * Returns the Delta table's current schema as a StructType.
   * The schema is stored in the last Metadata action in the Delta log.
   *
   * @param tablePath  Root path of the Delta table
   * @param conf       Hadoop configuration
   * @return Delta StructType schema
   */
  public static StructType getTableSchema(String tablePath, Configuration conf) throws IOException {
    return openLog(tablePath, conf).snapshot().getMetadata().getSchema();
  }

  /**
   * Returns the current committed version number of the Delta table.
   * Returns -1 if no commits have been made yet (empty table).
   */
  public static long getTableVersion(String tablePath, Configuration conf) throws IOException {
    return openLog(tablePath, conf).snapshot().getVersion();
  }

  /**
   * Returns the Delta table's partition columns.
   * Empty list for non-partitioned tables.
   */
  public static List<String> getPartitionColumns(String tablePath, Configuration conf)
      throws IOException {
    return openLog(tablePath, conf).snapshot().getMetadata().getPartitionColumns();
  }

  /**
   * Returns the table-level configuration properties stored in the Delta log.
   * Examples: delta.autoOptimize.autoCompact, delta.dataSkippingNumIndexedCols
   */
  public static Map<String, String> getTableProperties(String tablePath, Configuration conf)
      throws IOException {
    return openLog(tablePath, conf).snapshot().getMetadata().getConfiguration();
  }

  /**
   * Returns a summary of the current snapshot: version, file count, total size.
   */
  public static SnapshotSummary getSnapshotSummary(String tablePath, Configuration conf)
      throws IOException {
    CachedDeltaSnapshot snap = getOrBuildSnapshot(tablePath, conf, DEFAULT_CACHE_TTL_MS);
    long totalBytes = snap.files.stream().mapToLong(AddFile::getSize).sum();
    return new SnapshotSummary(snap.version, snap.files.size(), totalBytes);
  }

  // -----------------------------------------------------------------------
  // Table detection
  // -----------------------------------------------------------------------

  /**
   * Returns true if the given path is a Delta Lake table.
   * A Delta table must have a _delta_log/ subdirectory.
   */
  public static boolean isDeltaTable(String tablePath, Configuration conf) {
    try {
      FileSystem fs = FileSystem.get(new Path(tablePath).toUri(), conf);
      return fs.exists(new Path(tablePath, "_delta_log"));
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Returns true if the Delta table at the given path has any partitioning.
   */
  public static boolean isPartitioned(String tablePath, Configuration conf) throws IOException {
    return !getPartitionColumns(tablePath, conf).isEmpty();
  }

  // -----------------------------------------------------------------------
  // Change Data Feed (CDC)
  // -----------------------------------------------------------------------

  /**
   * Returns the file paths added between two Delta versions (exclusive start,
   * inclusive end). Used for incremental processing / CDC pipelines.
   *
   * @param tablePath    Root path of the Delta table
   * @param startVersion Exclusive lower bound (changes AFTER this version)
   * @param endVersion   Inclusive upper bound (changes UP TO AND INCLUDING this version)
   * @param conf         Hadoop configuration
   * @return List of absolute paths of files added in the version range
   */
  public static List<String> getAddedFilesSince(
      String tablePath, long startVersion, long endVersion, Configuration conf) throws IOException {

    DeltaLog log = openLog(tablePath, conf);
    List<String> addedPaths = new ArrayList<>();

    try {
      java.util.Iterator<io.delta.standalone.VersionLog> versionLogs =
          log.getChanges(startVersion + 1, /* failOnDataLoss */ false);

      while (versionLogs.hasNext()) {
        io.delta.standalone.VersionLog vl = versionLogs.next();
        if (vl.getVersion() > endVersion) break;

        for (Action action : vl.getActions()) {
          if (action instanceof AddFile) {
            addedPaths.add(toAbsolutePath(((AddFile) action).getPath(), tablePath));
          }
        }
      }
    } catch (Exception e) {
      throw new IOException(
          "Failed to read Delta changelog for " + tablePath
              + " between versions " + startVersion + " and " + endVersion, e);
    }

    return addedPaths;
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  /**
   * Opens the DeltaLog for the given table path.
   * DeltaLog.forTable() reads the latest _delta_log/*.json checkpoint and
   * any subsequent JSON commit files to reconstruct the current snapshot.
   */
  static DeltaLog openLog(String tablePath, Configuration conf) throws IOException {
    try {
      return DeltaLog.forTable(conf, tablePath);
    } catch (Exception e) {
      throw new IOException(
          "Failed to open Delta log at " + tablePath
              + ". Verify the path contains a _delta_log/ directory.", e);
    }
  }

  /**
   * Resolves a relative file path from the Delta log to an absolute path.
   * Delta log stores paths relative to the table root.
   */
  private static String toAbsolutePath(String relativePath, String tableRoot) {
    if (relativePath.startsWith("/") || relativePath.contains("://")) {
      return relativePath; // already absolute
    }
    return tableRoot.endsWith("/")
        ? tableRoot + relativePath
        : tableRoot + "/" + relativePath;
  }

  // -----------------------------------------------------------------------
  // Value types
  // -----------------------------------------------------------------------

  /** Summary statistics for a Delta snapshot. */
  public static class SnapshotSummary {
    public final long version;
    public final int  fileCount;
    public final long totalSizeBytes;

    SnapshotSummary(long version, int fileCount, long totalSizeBytes) {
      this.version        = version;
      this.fileCount      = fileCount;
      this.totalSizeBytes = totalSizeBytes;
    }

    @Override
    public String toString() {
      return String.format("SnapshotSummary{version=%d, files=%d, size=%d bytes}",
          version, fileCount, totalSizeBytes);
    }
  }
}
