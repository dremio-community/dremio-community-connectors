package com.dremio.plugins.pagerduty;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

/**
 * Converts a PagerDutyScanDrel (logical) into a PagerDutyScanPrel (physical).
 */
public class PagerDutyScanPrule extends RelOptRule {

    public static final PagerDutyScanPrule INSTANCE = new PagerDutyScanPrule();

    private PagerDutyScanPrule() {
        super(RelOptHelper.any(PagerDutyScanDrel.class, Rel.LOGICAL), "PagerDutyScanPrule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return call.rel(0) instanceof PagerDutyScanDrel;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        PagerDutyScanDrel drel = call.rel(0);
        PagerDutyGroupScan groupScan = new PagerDutyGroupScan(
                OpProps.prototype(0),
                drel.getTableMetadata(),
                drel.getProjectedColumns(),
                drel.getPluginId(),
                drel.getScanSpec());

        call.transformTo(new PagerDutyScanPrel(
                drel.getCluster(),
                drel.getTraitSet().replace(Prel.PHYSICAL),
                drel.getTable(),
                drel.getPluginId(),
                drel.getTableMetadata(),
                drel.getProjectedColumns(),
                drel.getCostAdjustmentFactor(),
                drel.getHints(),
                groupScan,
                drel.getScanSpec()));
    }
}
