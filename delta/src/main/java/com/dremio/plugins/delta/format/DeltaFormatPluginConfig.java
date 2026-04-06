package com.dremio.plugins.delta.format;

import com.dremio.common.logical.FormatPluginConfig;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Format-level configuration for the Delta Lake format plugin.
 *
 * Stored alongside the source config in Dremio's catalog.
 * Keeps format concerns (schema evolution, stats collection) separate
 * from the connection-level concerns in DeltaPluginConfig.
 *
 * NOTE: The type name is "delta_write" (not "delta") to avoid conflicting with
 * Dremio's built-in DeltaLakeFormatPlugin which is registered under the name "delta"
 * but returns null from getWriter() (write-unsupported). If we used "delta" here,
 * STORE AS (type => 'delta') would resolve to Dremio's native read-only plugin via
 * Jackson type resolution, and WriterCommitterPOP.child would be null → NPE.
 * Using "delta_write" routes STORE AS (type => 'delta_write') to this plugin.
 */
@JsonTypeName("delta_write")
public class DeltaFormatPluginConfig implements FormatPluginConfig {

  /**
   * Collect column statistics (min/max/null counts) during write.
   * Delta Lake stores these in the _delta_log as JSON stats on each AddFile.
   * Used by Dremio's query planner for file skipping / partition pruning.
   *
   * Enabling this adds a small overhead per write but significantly improves
   * read query performance on large tables with selective filters.
   */
  public boolean collectStats = true;

  /**
   * Number of columns for which to collect statistics.
   * Delta's default is 32; set to -1 for all columns.
   * Collecting stats on all columns of very wide tables can be expensive.
   */
  public int statsNumIndexedCols = 32;

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof DeltaFormatPluginConfig)) return false;
    DeltaFormatPluginConfig that = (DeltaFormatPluginConfig) o;
    return collectStats == that.collectStats
        && statsNumIndexedCols == that.statsNumIndexedCols;
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(collectStats, statsNumIndexedCols);
  }
}
