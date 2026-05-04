package com.dremio.plugins.redis.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.redis.scan.RedisGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class RedisScanPrule extends RelOptRule {

  public static final RedisScanPrule INSTANCE = new RedisScanPrule();

  private RedisScanPrule() {
    super(RelOptHelper.any(RedisScanDrel.class, Rel.LOGICAL), "RedisScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof RedisScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    RedisScanDrel drel = call.rel(0);
    RedisGroupScan groupScan = new RedisGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getPluginId(), drel.getScanSpec());

    call.transformTo(new RedisScanPrel(
        drel.getCluster(), drel.getTraitSet().replace(Prel.PHYSICAL),
        drel.getTable(), drel.getPluginId(), drel.getTableMetadata(),
        drel.getProjectedColumns(), drel.getCostAdjustmentFactor(),
        drel.getHints(), groupScan, drel.getScanSpec()));
  }
}
