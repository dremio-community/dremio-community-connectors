package com.dremio.plugins.spanner.planning;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.spanner.scan.SpannerScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;

/**
 * Pushes filter predicates from a {@link FilterRel} node down into the
 * {@link SpannerScanDrel} so they become SQL WHERE clauses in the Spanner query.
 *
 * Partial pushdown: AND-connected terms that can be converted are pushed;
 * the remaining terms stay in a residual FilterRel above the scan.
 *
 * Pattern matched:  FilterRel → SpannerScanDrel
 * Produces:         [FilterRel(residual)] → SpannerScanDrel(pushed filters)
 */
public class SpannerFilterRule extends RelOptRule {

  public static final SpannerFilterRule INSTANCE = new SpannerFilterRule();

  private SpannerFilterRule() {
    super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
        RelOptHelper.any(SpannerScanDrel.class)), "SpannerFilterRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    FilterRel filter = call.rel(0);
    // Don't re-process a filter we already pushed
    return !filter.isAlreadyPushedDown();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterRel       filter = call.rel(0);
    SpannerScanDrel scan   = call.rel(1);

    RexNode condition = filter.getCondition();

    // Try to convert the entire filter condition to a Spanner SQL fragment.
    // SpannerFilterConverter handles partial AND pushdown internally.
    String pushed = SpannerFilterConverter.convert(condition, scan.getRowType());
    if (pushed == null) return; // nothing to push

    // Build the new scan spec with the pushed filter
    SpannerScanSpec oldSpec = scan.getScanSpec();
    List<String> newFilters = new ArrayList<>(oldSpec.getFilters());
    newFilters.add(pushed);
    SpannerScanSpec newSpec = oldSpec.withFilters(newFilters);

    SpannerScanDrel newScan = new SpannerScanDrel(
        scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getCostAdjustmentFactor(), scan.getHints(), newSpec);

    // Check whether the full condition was pushed or only part of it.
    // For safety we always keep a residual filter marked as already-pushed-down
    // so Dremio still validates the result; it will be eliminated if redundant.
    FilterRel residual = new FilterRel(
        filter.getCluster(), filter.getTraitSet(), newScan,
        filter.getCondition(), true /* alreadyPushedDown */);

    call.transformTo(residual);
  }
}
