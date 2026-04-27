package com.dremio.plugins.spanner.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.plugins.spanner.scan.SpannerGroupScan;
import com.dremio.plugins.spanner.scan.SpannerScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/** Physical plan node for a Spanner table scan. */
public class SpannerScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final SpannerGroupScan groupScan;
  private final SpannerScanSpec  scanSpec;

  public SpannerScanPrel(RelOptCluster cluster, RelTraitSet traitSet,
                          RelOptTable table, StoragePluginId pluginId,
                          TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
                          double observedRowcountAdjustment, List<RelHint> hints,
                          SpannerGroupScan groupScan, SpannerScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec  = scanSpec;
  }

  @Override
  public GroupScan getGroupScan() { return groupScan; }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new SpannerGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(), getProjectedColumns(), getPluginId(), scanSpec);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new SpannerScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new SpannerScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), groupScan, scanSpec);
  }
}
