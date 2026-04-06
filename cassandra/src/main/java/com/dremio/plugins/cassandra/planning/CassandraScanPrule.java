package com.dremio.plugins.cassandra.planning;

import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.exec.planner.physical.Prel;
import com.dremio.plugins.cassandra.scan.CassandraGroupScan;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

import java.util.Collections;

/**
 * Converts a CassandraScanDrel (logical) into a CassandraScanPrel (physical)
 * during the PHYSICAL planning phase.
 *
 * This rule fires after CassandraScanRule has produced the logical scan node.
 * The physical node wraps a CassandraGroupScan that the fragment scheduler
 * will call to produce per-executor SubScans.
 */
public class CassandraScanPrule extends RelOptRule {

  public static final CassandraScanPrule INSTANCE = new CassandraScanPrule();

  private CassandraScanPrule() {
    super(RelOptHelper.any(CassandraScanDrel.class, Rel.LOGICAL),
        "CassandraScanPrule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    return call.rel(0) instanceof CassandraScanDrel;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    CassandraScanDrel drel = call.rel(0);

    // Build a prototype GroupScan for planning cost estimates.
    // The real GroupScan (with proper OpProps) is created in
    // CassandraScanPrel.getPhysicalOperator() using PhysicalPlanCreator.
    CassandraGroupScan groupScan = new CassandraGroupScan(
        OpProps.prototype(0),
        drel.getTableMetadata(),
        drel.getProjectedColumns(),
        drel.getPluginId(),
        drel.getScanSpec()
    );

    CassandraScanPrel prel = new CassandraScanPrel(
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
