package com.dremio.plugins.hudi;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility for bulk-discovering Apache Hudi tables by walking a Hadoop FileSystem.
 *
 * Purpose
 * -------
 * When connecting to an environment that already has many Hudi tables, Dremio
 * normally requires each table to be manually promoted in the UI before it
 * appears in the catalog. HudiTableDiscovery provides a programmatic alternative:
 * walk the source root, identify every directory that contains a {@code .hoodie/}
 * subdirectory, and return their paths for bulk catalog registration.
 *
 * Used by
 * -------
 * {@link HudiStoragePlugin#discoverHudiTables()} — call this method to get a
 * list of all Hudi table root paths under the source's configured rootPath.
 * Pass those paths to Dremio's catalog API (REST or internal) to register them.
 *
 * Detection rule
 * --------------
 * A directory is a Hudi table if it contains a {@code .hoodie/} subdirectory.
 * This is true for all Hudi table types (COW and MOR) regardless of partitioning.
 * Once a Hudi table root is detected, the walker does NOT recurse into it
 * (nested Hudi tables are not a supported pattern).
 *
 * Depth limit
 * -----------
 * The default maximum recursion depth is 5. This prevents runaway scans on
 * deep or very large directory trees. Adjust via
 * {@link #findHudiTables(FileSystem, Path, int)} if your layout is deeper.
 */
public final class HudiTableDiscovery {

  private static final Logger logger = LoggerFactory.getLogger(HudiTableDiscovery.class);
  private static final String HOODIE_DIR = ".hoodie";
  public static final int DEFAULT_MAX_DEPTH = 5;

  private HudiTableDiscovery() {}

  /**
   * Walks {@code root} on {@code fs} up to {@link #DEFAULT_MAX_DEPTH} levels deep,
   * returning the path of every directory that contains a {@code .hoodie/} subdirectory.
   *
   * @param fs    Hadoop FileSystem for the source (S3, ADLS, HDFS, local, etc.)
   * @param root  Root directory to scan — typically the source's configured rootPath
   * @return Unmodifiable list of Hudi table root paths; empty if none found
   * @throws IOException if the root directory cannot be listed
   */
  public static List<Path> findHudiTables(FileSystem fs, Path root) throws IOException {
    return findHudiTables(fs, root, DEFAULT_MAX_DEPTH);
  }

  /**
   * Walks {@code root} on {@code fs} up to {@code maxDepth} levels deep,
   * returning the path of every directory that contains a {@code .hoodie/} subdirectory.
   *
   * @param fs       Hadoop FileSystem for the source
   * @param root     Root directory to scan
   * @param maxDepth Maximum directory depth to recurse into (0 = root only)
   * @return Unmodifiable list of Hudi table root paths; empty if none found
   * @throws IOException if the root directory cannot be listed
   */
  public static List<Path> findHudiTables(FileSystem fs, Path root, int maxDepth)
      throws IOException {
    if (!fs.exists(root)) {
      logger.warn("HudiTableDiscovery: root path does not exist: {}", root);
      return Collections.emptyList();
    }

    List<Path> tables = new ArrayList<>();
    scan(fs, root, 0, maxDepth, tables);
    logger.info("HudiTableDiscovery: found {} Hudi table(s) under '{}'", tables.size(), root);
    return Collections.unmodifiableList(tables);
  }

  // -----------------------------------------------------------------------
  // Internal recursive scan
  // -----------------------------------------------------------------------

  private static void scan(
      FileSystem fs,
      Path dir,
      int depth,
      int maxDepth,
      List<Path> tables) throws IOException {

    if (depth > maxDepth) {
      return;
    }

    // If this directory IS a Hudi table, record it and stop recursing.
    // We do not support nested Hudi tables.
    if (isHudiTable(fs, dir)) {
      logger.debug("HudiTableDiscovery: found Hudi table at depth={} path='{}'", depth, dir);
      tables.add(dir);
      return;
    }

    // Not a Hudi table — recurse into subdirectories.
    FileStatus[] children;
    try {
      children = fs.listStatus(dir);
    } catch (IOException e) {
      // Log and skip unreadable directories (e.g. permission denied on S3 prefixes).
      logger.warn("HudiTableDiscovery: cannot list '{}': {} — skipping", dir, e.getMessage());
      return;
    }

    if (children == null) {
      return;
    }

    for (FileStatus child : children) {
      if (child.isDirectory()) {
        scan(fs, child.getPath(), depth + 1, maxDepth, tables);
      }
    }
  }

  /**
   * Returns true if {@code dir} contains a {@code .hoodie/} subdirectory,
   * indicating it is the root of a Hudi table.
   */
  static boolean isHudiTable(FileSystem fs, Path dir) {
    try {
      return fs.exists(new Path(dir, HOODIE_DIR));
    } catch (IOException e) {
      logger.debug("HudiTableDiscovery: exists check failed for '{}/.hoodie': {}",
          dir, e.getMessage());
      return false;
    }
  }
}
