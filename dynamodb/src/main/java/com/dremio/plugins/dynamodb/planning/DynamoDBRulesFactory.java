package com.dremio.plugins.dynamodb.planning;

import com.dremio.exec.catalog.conf.SourceType;
import com.dremio.exec.ops.OptimizerRulesContext;
import com.dremio.exec.planner.PlannerPhase;
import com.dremio.exec.store.StoragePluginRulesFactory.StoragePluginTypeRulesFactory;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;

import java.util.Set;

/**
 * Registers DynamoDB planner rules with Dremio's optimizer.
 *
 *   LOGICAL  → DynamoDBScanRule, DynamoDBFilterRule
 *   PHYSICAL → DynamoDBScanPrule
 */
public class DynamoDBRulesFactory extends StoragePluginTypeRulesFactory {

  @Override
  public Set<RelOptRule> getRules(OptimizerRulesContext context,
                                   PlannerPhase phase,
                                   SourceType pluginType) {
    switch (phase) {
      case LOGICAL:
        return ImmutableSet.of(DynamoDBScanRule.INSTANCE, DynamoDBFilterRule.INSTANCE);
      case PHYSICAL:
        return ImmutableSet.of(DynamoDBScanPrule.INSTANCE);
      default:
        return ImmutableSet.of();
    }
  }
}
