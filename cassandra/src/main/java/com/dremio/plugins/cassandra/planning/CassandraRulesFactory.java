package com.dremio.plugins.cassandra.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers Cassandra's planner rules with Dremio's optimizer.
 *
 * Called during planner initialization (once per query).
 * Returns the appropriate rule set for each planning phase:
 *
 *   LOGICAL  → CassandraScanRule, CassandraFilterRule, CassandraLimitRule.*
 *   PHYSICAL → CassandraScanPrule (CassandraScanDrel → CassandraScanPrel)
 */
public class CassandraRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(
            CassandraScanRule.INSTANCE,
            CassandraFilterRule.INSTANCE,
            CassandraLimitRule.DIRECT,
            CassandraLimitRule.WITH_FILTER);
      case PHYSICAL:
        return ImmutableSet.of(CassandraScanPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }
}
