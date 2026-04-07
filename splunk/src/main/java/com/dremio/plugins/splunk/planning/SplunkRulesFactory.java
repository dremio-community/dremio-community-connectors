package com.dremio.plugins.splunk.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers Splunk's planner rules with Dremio's optimizer.
 *
 *   LOGICAL  → SplunkScanRule (ScanCrel → SplunkScanDrel)
 *              SplunkFilterRule (pushes WHERE _time / field=value into SPL)
 *   PHYSICAL → SplunkScanPrule (SplunkScanDrel → SplunkScanPrel)
 */
public class SplunkRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(SplunkScanRule.INSTANCE, SplunkFilterRule.INSTANCE);
      case PHYSICAL:
        return ImmutableSet.of(SplunkScanPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }
}
