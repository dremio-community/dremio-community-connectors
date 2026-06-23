package com.dremio.plugins.googleads.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

public class GoogleAdsRulesFactory extends StoragePluginRulesFactory.StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext optimizerContext,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(GoogleAdsScanRule.INSTANCE);
      case PHYSICAL:
        return ImmutableSet.of(GoogleAdsScanPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }
}
