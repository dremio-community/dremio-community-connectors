package com.dremio.plugins.cassandra.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.AbstractGroupScan;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.SubScan;
import com.dremio.exec.store.SplitWork;
import com.dremio.exec.store.TableMetadata;
import com.dremio.common.expression.SchemaPath;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Physical operator representing a full Cassandra table scan in the planning layer.
 *
 * Produced by CassandraScanPrel.getPhysicalOperator() during physical planning.
 * Dremio's fragment scheduler calls getSplits() to distribute work, then
 * getSpecificScan() to produce one SubScan per executor fragment.
 *
 * v1: single split → single SubScan → entire table scanned by one reader.
 */
public class CassandraGroupScan extends AbstractGroupScan {

  private static final Logger logger = LoggerFactory.getLogger(CassandraGroupScan.class);

  private final StoragePluginId pluginId;
  private final CassandraScanSpec scanSpec;

  public CassandraGroupScan(OpProps props,
                              TableMetadata tableMetadata,
                              List<SchemaPath> columns,
                              StoragePluginId pluginId,
                              CassandraScanSpec scanSpec) {
    super(props, tableMetadata, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @Override
  public SubScan getSpecificScan(List<SplitWork> work) throws ExecutionSetupException {
    // Predicate pushdown mode: ignore token-range splits entirely.
    // The scan spec already carries the predicates; run a single direct CQL query.
    // (Parallelism is limited to 1 in getMaxParallelizationWidth() when in this mode,
    //  so this method is only called once with all splits going to one fragment.)
    if (scanSpec.hasPredicates()) {
      return new CassandraSubScan(
          getProps(),
          getFullSchema(),
          List.of(scanSpec.toTablePath()),
          getColumns(),
          pluginId,
          scanSpec,
          List.of(scanSpec)
      );
    }

    // Normal mode: decode ALL split specs assigned to this executor fragment.
    // Multiple splits per executor is common in single-node deployments or low-parallelism configs.
    List<CassandraScanSpec> allSpecs = new ArrayList<>();
    for (SplitWork sw : work) {
      CassandraScanSpec decoded = decodeSplitSpec(sw);
      // Propagate a pushed-down LIMIT to the per-fragment token-range spec.
      // Each fragment will include LIMIT N in its CQL query, stopping its
      // token-range scan early.  The LimitRel above ensures the total result
      // is capped at N across all fragments.
      if (scanSpec.hasLimit()) {
        decoded = decoded.withLimit(scanSpec.getLimit());
      }
      allSpecs.add(decoded);
    }
    if (allSpecs.isEmpty()) {
      allSpecs.add(scanSpec); // fallback: full table scan
    }

    return new CassandraSubScan(
        getProps(),
        getFullSchema(),
        List.of(allSpecs.get(0).toTablePath()),
        getColumns(),
        pluginId,
        allSpecs.get(0),
        allSpecs
    );
  }

  /**
   * Decodes a single SplitWork's extended property into a CassandraScanSpec.
   * Falls back to the full-table scanSpec if decoding fails.
   */
  private CassandraScanSpec decodeSplitSpec(SplitWork sw) {
    try {
      byte[] extBytes = null;
      try {
        // getSplitExtendedProperty() returns com.google.protobuf.ByteString — access via reflection
        // to avoid a compile-time dependency on protobuf
        Object extPropObj = sw.getClass()
            .getMethod("getSplitExtendedProperty").invoke(sw);
        if (extPropObj != null) {
          extBytes = (byte[]) extPropObj.getClass().getMethod("toByteArray").invoke(extPropObj);
        }
      } catch (Exception ignore) { /* reflection failed — full scan fallback */ }

      if (extBytes != null && extBytes.length > 0) {
        String specStr = new String(extBytes, java.nio.charset.StandardCharsets.UTF_8);
        // Format: keyspace|tableName|pk1,pk2|start|end
        String[] parts = specStr.split("\\|", 5);
        if (parts.length == 5) {
          String ks = parts[0];
          String tbl = parts[1];
          List<String> pks = parts[2].isEmpty() ? Collections.emptyList()
              : Arrays.asList(parts[2].split(","));
          long start = Long.parseLong(parts[3]);
          long end = Long.parseLong(parts[4]);
          return new CassandraScanSpec(ks, tbl, pks, start, end);
        }
      }
    } catch (Exception e) {
      logger.warn("Could not decode split token range, falling back to full scan: {}", e.getMessage());
    }
    return scanSpec; // fallback: full table scan
  }

  @Override
  public int getMaxParallelizationWidth() {
    // When predicate pushdown is active, run a single direct CQL query on one fragment.
    // Multiple fragments would each issue the same partition-key query → duplicate rows.
    if (scanSpec.hasPredicates()) {
      return 1;
    }
    // Return MAX_VALUE so Dremio uses all available splits.
    // The actual degree of parallelism is naturally bounded by the number of splits
    // created in listPartitionChunks() (= splitParallelism config value).
    return Integer.MAX_VALUE;
  }

  @Override
  public int getOperatorType() {
    // Use 0 (UNKNOWN) — operator type is only used for metrics labeling.
    // A dedicated CoreOperatorType enum value would require modifying Dremio's proto.
    return 0;
  }

  @JsonIgnore
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @JsonIgnore
  public CassandraScanSpec getScanSpec() {
    return scanSpec;
  }
}
