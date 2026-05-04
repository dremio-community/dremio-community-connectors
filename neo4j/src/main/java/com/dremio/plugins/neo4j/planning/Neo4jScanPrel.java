package com.dremio.plugins.neo4j.planning;

import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.planner.common.ScanRelBase;
import com.dremio.exec.planner.physical.OldScanPrelBase;
import com.dremio.exec.planner.physical.PhysicalPlanCreator;
import com.dremio.exec.planner.physical.ScanPrelBase;
import com.dremio.exec.store.TableMetadata;
import com.dremio.exec.physical.base.GroupScan;
import com.dremio.plugins.neo4j.scan.Neo4jGroupScan;
import com.dremio.plugins.neo4j.scan.Neo4jScanSpec;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.hint.RelHint;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class Neo4jScanPrel extends ScanPrelBase implements OldScanPrelBase {

  private final Neo4jGroupScan groupScan;
  private final Neo4jScanSpec  scanSpec;

  public Neo4jScanPrel(RelOptCluster cluster, RelTraitSet traitSet,
                        RelOptTable table, StoragePluginId pluginId,
                        TableMetadata tableMetadata, List<SchemaPath> projectedColumns,
                        double observedRowcountAdjustment, List<RelHint> hints,
                        Neo4jGroupScan groupScan, Neo4jScanSpec scanSpec) {
    super(cluster, traitSet, table, pluginId, tableMetadata,
        projectedColumns, observedRowcountAdjustment, hints, Collections.emptyList());
    this.groupScan = groupScan;
    this.scanSpec  = scanSpec;
  }

  @Override public GroupScan getGroupScan() { return groupScan; }

  @Override
  public PhysicalOperator getPhysicalOperator(PhysicalPlanCreator creator) throws IOException {
    return new Neo4jGroupScan(
        creator.props(this, null, getTableMetadata().getSchema()),
        getTableMetadata(), getProjectedColumns(), getPluginId(), scanSpec);
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new Neo4jScanPrel(getCluster(), traitSet, getTable(), getPluginId(),
        getTableMetadata(), getProjectedColumns(), getCostAdjustmentFactor(),
        getHints(), groupScan, scanSpec);
  }

  @Override
  public ScanRelBase cloneWithProject(List<SchemaPath> columns) {
    return new Neo4jScanPrel(getCluster(), getTraitSet(), getTable(), getPluginId(),
        getTableMetadata(), columns, getCostAdjustmentFactor(), getHints(), groupScan, scanSpec);
  }
}
