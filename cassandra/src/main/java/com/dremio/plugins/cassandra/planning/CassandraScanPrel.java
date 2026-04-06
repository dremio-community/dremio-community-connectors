package com.dremio.plugins.cassandra.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.cassandra.scan.CassandraGroupScan;
import com.dremio.plugins.cassandra.scan.CassandraScanSpec;
import com.dremio.exec.physical.base.GroupScan;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Physical plan node for a Cassandra table scan.
 *
 * Extends ScanPrelBase (which implements Prel's standard methods: accept,
 * getSupportedEncodings, getEncoding, needsFinalColumnReordering,
 * getDistributionAffinity, etc.) and additionally implements OldScanPrelBase
 * to expose the GroupScan to Dremio's fragment scheduler.
 *
 * getPhysicalOperator() returns a CassandraGroupScan, which the fragment
 * scheduler parallelizes and passes to CassandraGroupScan.getSpecificScan()
 * to produce CassandraSubScan instances for each executor fragment.
 */
public class CassandraScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final CassandraGroupScan groupScan;
  private final CassandraScanSpec scanSpec;

  public CassandraScanPrel(RelOptCluster cluster,
                             RelTraitSet traitSet,
                             RelOptTable table,
                             StoragePluginId pluginId,
                             TableMetadata tableMetadata,
                             List<SchemaPath> projectedColumns,
                             double observedRowcountAdjustment,
                             List<RelHint> hints,
                             CassandraGroupScan groupScan,
                             CassandraScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec = scanSpec;
  }

  @Override
  public GroupScan getGroupScan() {
    return groupScan;
  }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    // Return the GroupScan with proper Dremio-assigned OpProps.
    // The GroupScan IS a PhysicalOperator (AbstractBase implements PhysicalOperator).
    // Dremio's fragment scheduler calls getSplits() then getSpecificScan() on it.
    return new CassandraGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(),
        getProjectedColumns(),
        getPluginId(),
        scanSpec
    );
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new CassandraScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new CassandraScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(),
        groupScan, scanSpec);
  }
}
