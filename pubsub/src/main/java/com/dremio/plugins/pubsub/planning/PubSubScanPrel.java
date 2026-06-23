package com.dremio.plugins.pubsub.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.plugins.pubsub.scan.PubSubGroupScan;
import com.dremio.plugins.pubsub.scan.PubSubSubScan.PubSubScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * Physical plan node for a Pub/Sub subscription scan.
 * getPhysicalOperator() returns a PubSubGroupScan (single split).
 */
public class PubSubScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final PubSubGroupScan groupScan;
  private final PubSubScanSpec  scanSpec;

  public PubSubScanPrel(RelOptCluster cluster,
                         RelTraitSet traitSet,
                         RelOptTable table,
                         StoragePluginId pluginId,
                         TableMetadata tableMetadata,
                         List<SchemaPath> projectedColumns,
                         double observedRowcountAdjustment,
                         List<RelHint> hints,
                         PubSubGroupScan groupScan,
                         PubSubScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec  = scanSpec;
  }

  @Override
  public GroupScan getGroupScan() { return groupScan; }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new PubSubGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(),
        getProjectedColumns(),
        getPluginId(),
        scanSpec
    );
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new PubSubScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new PubSubScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(),
        groupScan, scanSpec);
  }
}
