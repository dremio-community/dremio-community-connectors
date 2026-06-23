package com.dremio.plugins.googleads.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.googleads.scan.GoogleAdsGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class GoogleAdsScanPrule extends RelOptRule {

  public static final GoogleAdsScanPrule INSTANCE = new GoogleAdsScanPrule();

  private GoogleAdsScanPrule() {
    super(RelOptHelper.any(GoogleAdsScanDrel.class, Rel.LOGICAL), "GoogleAdsScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof GoogleAdsScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    GoogleAdsScanDrel drel = call.rel(0);

    GoogleAdsGroupScan groupScan = new GoogleAdsGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getPluginId(),
        drel.getScanSpec()
    );

    GoogleAdsScanPrel prel = new GoogleAdsScanPrel(
        drel.getCluster(),
        drel.getTraitSet().replace(Prel.PHYSICAL),
        drel.getTable(),
        drel.getPluginId(),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getCostAdjustmentFactor(),
        drel.getHints(),
        groupScan,
        drel.getScanSpec()
    );

    call.transformTo(prel);
  }
}
