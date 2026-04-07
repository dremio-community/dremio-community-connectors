package com.dremio.plugins.splunk.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.splunk.scan.SplunkGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Converts a SplunkScanDrel (logical) into a SplunkScanPrel (physical)
 * during the PHYSICAL planning phase.
 */
public class SplunkScanPrule extends RelOptRule {

  public static final SplunkScanPrule INSTANCE = new SplunkScanPrule();

  private SplunkScanPrule() {
    super(RelOptHelper.any(SplunkScanDrel.class, Rel.LOGICAL), "SplunkScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof SplunkScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    SplunkScanDrel drel = call.rel(0);

    SplunkGroupScan groupScan = new SplunkGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getPluginId(),
        drel.getScanSpec()
    );

    SplunkScanPrel prel = new SplunkScanPrel(
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
