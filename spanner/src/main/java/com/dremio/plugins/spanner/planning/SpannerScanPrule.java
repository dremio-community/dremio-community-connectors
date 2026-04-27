package com.dremio.plugins.spanner.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.spanner.scan.SpannerGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/** Converts SpannerScanDrel (logical) → SpannerScanPrel (physical). */
public class SpannerScanPrule extends RelOptRule {

  public static final SpannerScanPrule INSTANCE = new SpannerScanPrule();

  private SpannerScanPrule() {
    super(RelOptHelper.any(SpannerScanDrel.class, Rel.LOGICAL), "SpannerScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof SpannerScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SpannerScanDrel drel = call.rel(0);
    SpannerGroupScan groupScan = new SpannerGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(), drel.getProjectedColumns(),
        drel.getPluginId(), drel.getScanSpec());

    call.transformTo(new SpannerScanPrel(
        drel.getCluster(), drel.getTraitSet().replace(Prel.PHYSICAL),
        drel.getTable(), drel.getPluginId(), drel.getTableMetadata(),
        drel.getProjectedColumns(), drel.getCostAdjustmentFactor(),
        drel.getHints(), groupScan, drel.getScanSpec()));
  }
}
