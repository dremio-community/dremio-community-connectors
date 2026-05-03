package com.dremio.plugins.spanner.planning;

import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.spanner.scan.SpannerSubScan.SpannerScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

/**
 * Pushes a LIMIT value from a {@link LimitRel} node into the {@link SpannerScanDrel}
 * so it becomes a SQL LIMIT clause in the Spanner query.
 *
 * Only pushes when:
 *   - There is no OFFSET (or offset = 0); pushing LIMIT with a non-zero OFFSET
 *     would return wrong results because each Spanner segment applies the LIMIT
 *     independently.
 *   - The fetch value is a compile-time constant (RexLiteral).
 *
 * The LimitRel is left in place above the scan so Dremio still enforces the
 * correct final row count across all parallel segments.
 *
 * Pattern matched:  LimitRel → SpannerScanDrel
 * Produces:         LimitRel → SpannerScanDrel(limit pushed)
 */
public class SpannerLimitRule extends RelOptRule {

  public static final SpannerLimitRule INSTANCE = new SpannerLimitRule();

  private SpannerLimitRule() {
    super(RelOptHelper.some(LimitRel.class, Rel.LOGICAL,
        RelOptHelper.any(SpannerScanDrel.class)), "SpannerLimitRule");
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LimitRel        limit = call.rel(0);
    SpannerScanDrel scan  = call.rel(1);

    // Don't push if there's already a limit in this scan
    if (scan.getScanSpec().hasLimit()) return;

    // No OFFSET support — each segment would apply OFFSET independently
    RexNode offset = limit.getOffset();
    if (offset != null) {
      long off = toLong(offset);
      if (off != 0) return;
    }

    RexNode fetch = limit.getFetch();
    if (fetch == null) return;
    long fetchValue = toLong(fetch);
    if (fetchValue <= 0) return;

    SpannerScanSpec newSpec = scan.getScanSpec().withLimit(fetchValue);
    SpannerScanDrel newScan = new SpannerScanDrel(
        scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getCostAdjustmentFactor(), scan.getHints(), newSpec);

    // Keep the LimitRel on top — it enforces the correct merged row count
    call.transformTo(limit.copy(limit.getTraitSet(), newScan,
        limit.getCollation(), limit.getOffset(), limit.getFetch()));
  }

  private static long toLong(RexNode node) {
    if (node instanceof RexLiteral) {
      Number n = (Number) ((RexLiteral) node).getValue2();
      if (n != null) return n.longValue();
    }
    return -1;
  }
}
