package com.dremio.plugins.kafka.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers Kafka's planner rules with Dremio's optimizer.
 *
 *   LOGICAL  → KafkaScanRule (ScanCrel → KafkaScanDrel)
 *   PHYSICAL → KafkaScanPrule (KafkaScanDrel → KafkaScanPrel)
 */
public class KafkaRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(KafkaScanRule.INSTANCE, KafkaFilterRule.INSTANCE);
      case PHYSICAL:
        return ImmutableSet.of(KafkaScanPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }
}
