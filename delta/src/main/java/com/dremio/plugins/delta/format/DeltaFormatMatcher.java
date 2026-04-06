package com.dremio.plugins.delta.format;

import com.dremio.exec.store.dfs.FileSelection;
import com.dremio.exec.store.dfs.FormatMatcher;
import com.dremio.exec.store.dfs.FormatPlugin;
import com.dremio.io.CompressionCodecFactory;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * FormatMatcher that identifies Delta Lake tables by the presence of a
 * {@code _delta_log/} subdirectory within a candidate directory.
 *
 * How Dremio uses this
 * --------------------
 * During metadata refresh, Dremio's FileSystemPlugin crawls the configured
 * source root and calls {@link FormatMatcher#matches} on each subdirectory
 * it encounters. If matches() returns true, Dremio registers that directory
 * as a dataset of this format — without any manual promotion step.
 *
 * This replaces EasyFormatPlugin's default BasicFormatMatcher (which matches
 * by file extension) with a directory-structure-based matcher, following the
 * same pattern as Dremio's built-in IcebergFormatMatcher (metadata/) and
 * the HudiFormatMatcher in this project (.hoodie/).
 *
 * Detection logic
 * ---------------
 * A directory is a Delta Lake table if and only if it contains a
 * {@code _delta_log/} subdirectory. This is Delta's canonical table marker —
 * every Delta table (partitioned and non-partitioned, any version) has this
 * directory containing the transaction log.
 *
 * Effect
 * ------
 * Before this matcher: after CTAS, users had to manually promote each table
 * folder in the Dremio UI (hover → Format → Parquet → Save) before querying.
 *
 * After this matcher: tables with {@code _delta_log/} are auto-discovered
 * during metadata refresh and immediately queryable via SELECT.
 */
public class DeltaFormatMatcher extends FormatMatcher {

  private static final Logger logger = LoggerFactory.getLogger(DeltaFormatMatcher.class);
  private static final String DELTA_LOG_DIR = "_delta_log";

  private final DeltaFormatPlugin plugin;

  public DeltaFormatMatcher(DeltaFormatPlugin plugin) {
    this.plugin = plugin;
  }

  /**
   * Returns true if {@code fileSelection}'s root directory contains a
   * {@code _delta_log/} subdirectory, identifying it as a Delta Lake table.
   *
   * @param fs            Dremio FileSystem for the source (S3, ADLS, NAS, etc.)
   * @param fileSelection The directory being evaluated for format detection
   * @param codecFactory  Unused — Delta detection is directory-based, not content-based
   */
  @Override
  public boolean matches(
      FileSystem fs,
      FileSelection fileSelection,
      CompressionCodecFactory codecFactory) throws IOException {
    String root = fileSelection.getSelectionRoot();
    Path deltaLogPath = Path.of(root + "/" + DELTA_LOG_DIR);
    boolean isDelta = fs.exists(deltaLogPath);
    if (isDelta) {
      logger.debug("DeltaFormatMatcher: detected Delta table at '{}'", root);
    }
    return isDelta;
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }
}
