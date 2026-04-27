package com.dremio.plugins.spanner.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.fasterxml.jackson.annotation.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Physical operator representing a Spanner table scan in the planning layer.
 *
 * Produced by SpannerScanPrel.getPhysicalOperator(). Dremio's fragment scheduler
 * calls getSpecificScan() with the splits assigned to each executor fragment.
 *
 * Each split encodes "tableName|segment|totalSegments" as UTF-8 bytes in the
 * split's extended property (written by SpannerStoragePlugin.listPartitionChunks()).
 */
public class SpannerGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(SpannerGroupScan.class);

  private final StoragePluginId pluginId;
  private final SpannerScanSpec scanSpec;

  public SpannerGroupScan(OpProps props,
                           TableMetadata tableMetadata,
                           List<SchemaPath> columns,
                           StoragePluginId pluginId,
                           SpannerScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    // Decode all splits assigned to this executor fragment.
    // On a single-node cluster every split lands in one fragment; on multi-node
    // each fragment gets a subset. Each split reads its own partition of the table.
    List<SpannerScanSpec> specs = new ArrayList<>();
    for (SplitWork sw : work) {
      SpannerScanSpec decoded = decodeSplit(sw);
      if (decoded != null) {
        // Propagate pushed-down filters and limit from the planning-time spec
        if (scanSpec.hasFilters() || scanSpec.hasLimit()) {
          decoded = new SpannerScanSpec(
              decoded.getTableName(),
              decoded.getSegment(),
              decoded.getTotalSegments(),
              decoded.getColumns(),
              scanSpec.getFilters(),
              scanSpec.getLimit());
        }
        specs.add(decoded);
      }
    }
    if (specs.isEmpty()) {
      specs.add(scanSpec); // fallback: full table scan with no segmentation
    }

    return new SpannerSubScan(
        getProps(), getFullSchema(),
        List.of(specs.get(0).toTablePath()),
        getColumns(), pluginId, specs);
  }

  /** Decodes "tableName|segment|totalSegments" bytes from the split extended property. */
  private SpannerScanSpec decodeSplit(SplitWork sw) {
    try {
      byte[] bytes = null;
      try {
        Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
        if (extProp != null) {
          bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
        }
      } catch (Exception ignore) {}

      if (bytes != null && bytes.length > 0) {
        String spec  = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        String[] parts = spec.split("\\|", 3);
        if (parts.length == 3) {
          return new SpannerScanSpec(parts[0],
              Integer.parseInt(parts[1]),
              Integer.parseInt(parts[2]));
        }
      }
    } catch (Exception e) {
      logger.warn("Could not decode Spanner split spec, using fallback: {}", e.getMessage());
    }
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    return Integer.MAX_VALUE; // bounded by number of splits = splitParallelism
  }

  @Override
  public int getOperatorType() { return 0; } // UNKNOWN

  @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
  @JsonIgnore public SpannerScanSpec getScanSpec()  { return scanSpec; }
}
