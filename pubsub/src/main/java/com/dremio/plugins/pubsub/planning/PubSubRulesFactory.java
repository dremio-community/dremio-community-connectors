package com.dremio.plugins.pubsub.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers Pub/Sub planner rules with Dremio's optimizer.
 *
 *   LOGICAL  → PubSubScanRule  (ScanCrel → PubSubScanDrel)
 *   PHYSICAL → PubSubScanPrule (PubSubScanDrel → PubSubScanPrel)
 */
public class PubSubRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:  return ImmutableSet.of(PubSubScanRule.INSTANCE);
      case PHYSICAL: return ImmutableSet.of(PubSubScanPrule.INSTANCE);
      default:       return ImmutableSet.of();
    }
  }
}
