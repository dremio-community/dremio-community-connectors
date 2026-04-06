package com.dremio.plugins.hudi.format;

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
 * FormatMatcher that identifies Apache Hudi tables by the presence of a
 * {@code .hoodie/} subdirectory within a candidate directory.
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
 * same pattern as Dremio's built-in DeltaLakeFormatMatcher (_delta_log/) and
 * IcebergFormatMatcher (metadata/).
 *
 * Detection logic
 * ---------------
 * A directory is a Hudi table if and only if it contains a {@code .hoodie/}
 * subdirectory. This is Hudi's canonical table marker — all Hudi tables
 * (COW and MOR, partitioned and non-partitioned) have this directory.
 */
public class HudiFormatMatcher extends FormatMatcher {

  private static final Logger logger = LoggerFactory.getLogger(HudiFormatMatcher.class);
  private static final String HOODIE_DIR = ".hoodie";

  private final HudiFormatPlugin plugin;

  public HudiFormatMatcher(HudiFormatPlugin plugin) {
    this.plugin = plugin;
  }

  /**
   * Returns true if {@code fileSelection}'s root directory contains a
   * {@code .hoodie/} subdirectory, identifying it as a Hudi table.
   *
   * @param fs            Dremio FileSystem for the source (S3, ADLS, NAS, etc.)
   * @param fileSelection The directory being evaluated for format detection
   * @param codecFactory  Unused — Hudi detection is directory-based, not content-based
   */
  @Override
  public boolean matches(
      FileSystem fs,
      FileSelection fileSelection,
      CompressionCodecFactory codecFactory) throws IOException {
    String root = fileSelection.getSelectionRoot();
    Path hoodiePath = Path.of(root + "/" + HOODIE_DIR);
    boolean isHudi = fs.exists(hoodiePath);
    if (isHudi) {
      logger.debug("HudiFormatMatcher: detected Hudi table at '{}'", root);
    }
    return isHudi;
  }

  @Override
  public FormatPlugin getFormatPlugin() {
    return plugin;
  }
}
