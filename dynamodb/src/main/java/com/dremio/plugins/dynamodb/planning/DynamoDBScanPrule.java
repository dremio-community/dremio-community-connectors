package com.dremio.plugins.dynamodb.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.dynamodb.scan.DynamoDBGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/** Converts DynamoDBScanDrel (logical) → DynamoDBScanPrel (physical). */
public class DynamoDBScanPrule extends RelOptRule {

  public static final DynamoDBScanPrule INSTANCE = new DynamoDBScanPrule();

  private DynamoDBScanPrule() {
    super(RelOptHelper.any(DynamoDBScanDrel.class, Rel.LOGICAL), "DynamoDBScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof DynamoDBScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    DynamoDBScanDrel drel = call.rel(0);
    DynamoDBGroupScan groupScan = new DynamoDBGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getPluginId(), drel.getScanSpec());

    call.transformTo(new DynamoDBScanPrel(
        drel.getCluster(), drel.getTraitSet().replace(Prel.PHYSICAL),
        drel.getTable(), drel.getPluginId(), drel.getTableMetadata(),
        drel.getProjectedColumns(), drel.getCostAdjustmentFactor(),
        drel.getHints(), groupScan, drel.getScanSpec()));
  }
}
