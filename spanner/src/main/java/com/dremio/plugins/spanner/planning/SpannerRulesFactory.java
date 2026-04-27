package com.dremio.plugins.spanner.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers Spanner planner rules with Dremio's optimizer.
 *
 *   LOGICAL  → SpannerScanRule, SpannerFilterRule, SpannerLimitRule
 *   PHYSICAL → SpannerScanPrule
 */
public class SpannerRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(
            SpannerScanRule.INSTANCE,
            SpannerFilterRule.INSTANCE,
            SpannerLimitRule.INSTANCE);
      case PHYSICAL:
        return ImmutableSet.of(SpannerScanPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }
}
