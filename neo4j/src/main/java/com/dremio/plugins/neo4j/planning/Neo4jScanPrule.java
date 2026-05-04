package com.dremio.plugins.neo4j.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.neo4j.scan.Neo4jGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class Neo4jScanPrule extends RelOptRule {

  public static final Neo4jScanPrule INSTANCE = new Neo4jScanPrule();

  private Neo4jScanPrule() {
    super(RelOptHelper.any(Neo4jScanDrel.class, Rel.LOGICAL), "Neo4jScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof Neo4jScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    Neo4jScanDrel drel = call.rel(0);
    Neo4jGroupScan groupScan = new Neo4jGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getPluginId(), drel.getScanSpec());

    call.transformTo(new Neo4jScanPrel(
        drel.getCluster(), drel.getTraitSet().replace(Prel.PHYSICAL),
        drel.getTable(), drel.getPluginId(), drel.getTableMetadata(),
        drel.getProjectedColumns(), drel.getCostAdjustmentFactor(),
        drel.getHints(), groupScan, drel.getScanSpec()));
  }
}
