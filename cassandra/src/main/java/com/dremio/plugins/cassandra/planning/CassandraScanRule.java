package com.dremio.plugins.cassandra.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.cassandra.CassandraStoragePluginConfig;
import com.dremio.plugins.cassandra.scan.CassandraScanSpec;
import org.apache.calcite.plan.RelOptRuleCall;

import java.util.List;

/**
 * Converts a generic ScanCrel (Dremio's catalog-level scan) into a
 * CassandraScanDrel (Cassandra-specific logical scan) during the LOGICAL
 * planning phase.
 *
 * SourceLogicalConverter automatically matches ScanCrel nodes for the
 * source type associated with CassandraStoragePluginConfig (@SourceType("APACHE_CASSANDRA")).
 */
public class CassandraScanRule extends SourceLogicalConverter {

  public static final CassandraScanRule INSTANCE = new CassandraScanRule();

  private CassandraScanRule() {
    super(CassandraStoragePluginConfig.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    // Extract keyspace and table from the qualified name: [source, keyspace, table]
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String keyspace = qualifiedName.size() >= 2
        ? qualifiedName.get(qualifiedName.size() - 2) : "";
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    CassandraScanSpec spec = new CassandraScanSpec(keyspace, tableName);

    return new CassandraScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(),
        scan.getPluginId(),
        scan.getTableMetadata(),
        scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(),
        scan.getHints(),
        spec
    );
  }
}
