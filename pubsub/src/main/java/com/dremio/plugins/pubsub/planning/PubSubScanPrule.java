package com.dremio.plugins.pubsub.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.pubsub.scan.PubSubGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import java.util.Collections;

/**
 * Converts a PubSubScanDrel (logical) into a PubSubScanPrel (physical) during PHYSICAL planning.
 */
public class PubSubScanPrule extends RelOptRule {

  public static final PubSubScanPrule INSTANCE = new PubSubScanPrule();

  private PubSubScanPrule() {
    super(RelOptHelper.any(PubSubScanDrel.class, Rel.LOGICAL), "PubSubScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof PubSubScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    PubSubScanDrel drel = call.rel(0);

    PubSubGroupScan groupScan = new PubSubGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getPluginId(),
        drel.getScanSpec()
    );

    PubSubScanPrel prel = new PubSubScanPrel(
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
