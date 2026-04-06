package com.dremio.plugins.hudi.format;

import com.dremio.common.logical.FormatPluginConfig;
import com.fasterxml.jackson.annotation.JsonTypeName;

/**
 * Immutable configuration for HudiFormatPlugin.
 *
 * Dremio's format plugin system requires a FormatPluginConfig class annotated
 * with @JsonTypeName. The name here ("hudi") is used in Dremio's internal
 * JSON serialization to deserialize the correct FormatPlugin when loading
 * stored source configurations from the catalog.
 *
 * This config is intentionally minimal - all Hudi write options live in
 * HudiPluginConfig (the StoragePlugin config) for simplicity. If you need
 * per-table format overrides (e.g., different compression per table), add
 * fields here and annotate with @JsonProperty.
 */
@JsonTypeName("hudi")
public class HudiFormatPluginConfig implements FormatPluginConfig {

  /**
   * Whether to enable Hudi's schema evolution support.
   * When true, schema changes (column adds/drops) are applied on write.
   */
  public boolean schemaEvolutionEnabled = true;

  /**
   * Parquet compression codec for Hudi base files.
   * Options: SNAPPY, GZIP, LZ4, ZSTD, NONE
   */
  public String parquetCompression = "SNAPPY";

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof HudiFormatPluginConfig)) return false;
    HudiFormatPluginConfig other = (HudiFormatPluginConfig) obj;
    return schemaEvolutionEnabled == other.schemaEvolutionEnabled
        && parquetCompression.equals(other.parquetCompression);
  }

  @Override
  public int hashCode() {
    return java.util.Objects.hash(schemaEvolutionEnabled, parquetCompression);
  }
}
