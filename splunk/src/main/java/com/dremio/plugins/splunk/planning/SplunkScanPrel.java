package com.dremio.plugins.splunk.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.plugins.splunk.scan.SplunkGroupScan;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
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
 * Physical plan node for a Splunk index scan.
 *
 * getPhysicalOperator() returns a SplunkGroupScan, which Dremio's fragment
 * scheduler uses to produce SplunkSubScan instances for executor fragments.
 */
public class SplunkScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final SplunkGroupScan groupScan;
  private final SplunkScanSpec  scanSpec;

  public SplunkScanPrel(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelOptTable table,
                         StoragePluginId pluginId,
                         TableMetadata tableMetadata,
                         List<SchemaPath> projectedColumns,
                         double observedRowcountAdjustment,
                         List<RelHint> hints,
                         SplunkGroupScan groupScan,
                         SplunkScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec  = scanSpec;
  }

  @Override
  public GroupScan getGroupScan() { return groupScan; }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new SplunkGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(),
        getProjectedColumns(),
        getPluginId(),
        scanSpec
    );
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SplunkScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new SplunkScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(),
        groupScan, scanSpec);
  }
}
