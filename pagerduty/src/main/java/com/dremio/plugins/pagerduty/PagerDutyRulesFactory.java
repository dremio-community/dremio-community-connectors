package com.dremio.plugins.pagerduty;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers PagerDuty planner rules with Dremio's optimizer.
 */
public class PagerDutyRulesFactory extends StoragePluginTypeRulesFactory {

    @Override
    public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                     PlannerPhase phase,
                                     SourceType pluginType) {
        switch (phase) {
            case LOGICAL:
                return ImmutableSet.of(PagerDutyScanRule.INSTANCE);
            case PHYSICAL:
                return ImmutableSet.of(PagerDutyScanPrule.INSTANCE);
            default:
                return ImmutableSet.of();
        }
    }
}
