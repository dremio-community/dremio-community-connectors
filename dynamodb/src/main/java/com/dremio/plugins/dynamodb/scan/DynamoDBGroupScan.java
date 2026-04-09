package com.dremio.plugins.dynamodb.scan;

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

import java.util.Collections;
import java.util.List;

/**
 * Physical operator representing a DynamoDB table scan in the planning layer.
 *
 * Produced by DynamoDBScanPrel.getPhysicalOperator(). Dremio's fragment
 * scheduler calls getSpecificScan() with the splits assigned to each executor.
 *
 * Each split corresponds to one DynamoDB parallel scan segment.
 * The split's extended property encodes: "tableName|segment|totalSegments".
 */
public class DynamoDBGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDBGroupScan.class);

  private final StoragePluginId pluginId;
  private final DynamoDBScanSpec scanSpec;

  public DynamoDBGroupScan(OpProps props,
                             TableMetadata tableMetadata,
                             List<SchemaPath> columns,
                             StoragePluginId pluginId,
                             DynamoDBScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    // Query-path mode: predicates include a partition key EQ — run a single Query
    if (scanSpec.hasPkPredicate()) {
      return new DynamoDBSubScan(
          getProps(), getFullSchema(),
          List.of(scanSpec.toTablePath()),
          getColumns(), pluginId, List.of(scanSpec));
    }

    // Scan-path: decode ALL assigned splits into separate DynamoDBScanSpecs.
    // On a single-node Dremio cluster all splits may be assigned to a single
    // fragment; we must read every assigned segment to avoid missing rows.
    List<DynamoDBScanSpec> specs = new java.util.ArrayList<>();
    for (SplitWork sw : work) {
      DynamoDBScanSpec decoded = decodeSplit(sw);
      if (decoded != null) {
        // Propagate pushed-down limit and predicates
        if (scanSpec.hasLimit() || scanSpec.hasPredicates() || scanSpec.hasPkPredicate()) {
          decoded = new DynamoDBScanSpec(
              decoded.getTableName(),
              decoded.getSegment(),
              decoded.getTotalSegments(),
              scanSpec.getPredicates(),
              scanSpec.getPkPredicate(),
              scanSpec.getSortKeyPredicate(),
              scanSpec.getLimit(),
              scanSpec.getPkName(),
              scanSpec.getSkName());
        }
        specs.add(decoded);
      }
    }
    if (specs.isEmpty()) specs.add(scanSpec); // fallback: scan without segmentation

    return new DynamoDBSubScan(
        getProps(), getFullSchema(),
        List.of(specs.get(0).toTablePath()),
        getColumns(), pluginId, specs);
  }

  /** Decodes the split's extended property bytes into a DynamoDBScanSpec. */
  private DynamoDBScanSpec decodeSplit(SplitWork sw) {
    try {
      byte[] bytes = null;
      try {
        Object extProp = sw.getClass().getMethod("getSplitExtendedProperty").invoke(sw);
        if (extProp != null) {
          bytes = (byte[]) extProp.getClass().getMethod("toByteArray").invoke(extProp);
        }
      } catch (Exception ignore) {}

      if (bytes != null && bytes.length > 0) {
        String spec = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        // Format: "tableName|segment|totalSegments"
        String[] parts = spec.split("\\|", 3);
        if (parts.length == 3) {
          return new DynamoDBScanSpec(parts[0],
              Integer.parseInt(parts[1]),
              Integer.parseInt(parts[2]));
        }
      }
    } catch (Exception e) {
      logger.warn("Could not decode DynamoDB split spec, using fallback: {}", e.getMessage());
    }
    return null;
  }

  @Override
  public int getMaxParallelizationWidth() {
    // Query-path: single fragment (partition key lookup returns one logical result set)
    if (scanSpec.hasPkPredicate()) return 1;
    // Scan-path: bounded by the number of splits = totalSegments
    return Integer.MAX_VALUE;
  }

  @Override
  public int getOperatorType() {
    return 0; // UNKNOWN
  }

  @JsonIgnore public StoragePluginId getPluginId() { return pluginId; }
  @JsonIgnore public DynamoDBScanSpec getScanSpec() { return scanSpec; }
}
