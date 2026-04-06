package com.dremio.plugins.cassandra.planning;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.LimitRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.cassandra.scan.CassandraScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Pushes a SQL {@code LIMIT N} clause into the Cassandra scan spec so that
 * generated CQL statements include {@code LIMIT N}, reducing the number of rows
 * fetched from Cassandra nodes.
 *
 * <h3>Two match patterns</h3>
 *
 * <dl>
 *   <dt>{@link #DIRECT} — {@code LimitRel → CassandraScanDrel}</dt>
 *   <dd>Matches when there is no filter between the LIMIT and the scan.
 *       Typical for {@code SELECT … FROM t LIMIT N} with no WHERE clause,
 *       or when the WHERE clause contained no pushable predicates so
 *       {@code CassandraFilterRule} did not fire.</dd>
 *
 *   <dt>{@link #WITH_FILTER} — {@code LimitRel → FilterRel → CassandraScanDrel}</dt>
 *   <dd>Matches after {@code CassandraFilterRule} has fired and left a residual
 *       filter above the scan.  Typical for
 *       {@code SELECT … FROM t WHERE col = ? LIMIT N}.</dd>
 * </dl>
 *
 * <h3>Correctness</h3>
 * The {@code LimitRel} is <em>kept</em> in the plan in both cases.
 * This ensures a correct final row count for multi-fragment (token-range)
 * scans: each fragment returns at most {@code N} rows from its token range,
 * and the {@code LimitRel} above caps the total result to {@code N}.
 * For single-fragment predicate-pushdown scans the CQL {@code LIMIT} is
 * exact and the {@code LimitRel} becomes a no-op.
 *
 * <h3>Not supported</h3>
 * <ul>
 *   <li>{@code OFFSET} — CQL has no native OFFSET; the rule does not fire
 *       when an offset &gt; 0 is present.</li>
 *   <li>Repeated firing — the rule does not fire when the scan already
 *       carries a limit.</li>
 * </ul>
 */
public class CassandraLimitRule extends RelOptRule {

  private static final Logger logger = LoggerFactory.getLogger(CassandraLimitRule.class);

  /**
   * Matches {@code LimitRel(CassandraScanDrel)} — no filter in between.
   * Handles {@code SELECT … FROM t LIMIT N} (no WHERE or WHERE didn't push).
   */
  public static final CassandraLimitRule DIRECT = new CassandraLimitRule(
      RelOptHelper.some(LimitRel.class, Rel.LOGICAL,
          RelOptHelper.any(CassandraScanDrel.class)),
      "CassandraLimitRule:direct",
      false);

  /**
   * Matches {@code LimitRel(FilterRel(CassandraScanDrel))} — a residual filter
   * was left by {@code CassandraFilterRule}.
   * Handles {@code SELECT … FROM t WHERE col = ? LIMIT N}.
   */
  public static final CassandraLimitRule WITH_FILTER = new CassandraLimitRule(
      RelOptHelper.some(LimitRel.class, Rel.LOGICAL,
          RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
              RelOptHelper.any(CassandraScanDrel.class))),
      "CassandraLimitRule:withFilter",
      true);

  /** Whether a {@code FilterRel} sits between the {@code LimitRel} and the scan. */
  private final boolean hasFilter;

  private CassandraLimitRule(RelOptRuleOperand operand, String description, boolean hasFilter) {
    super(operand, description);
    this.hasFilter = hasFilter;
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    LimitRel        limit = call.rel(0);
    CassandraScanDrel scan = call.rel(hasFilter ? 2 : 1);

    // Only fire when:
    //   1. There is a finite fetch (null fetch = no LIMIT in SQL)
    //   2. No offset, or offset is explicitly 0
    //   3. The scan does not already have a limit embedded (avoid infinite rule cycles)
    return limit.getFetch() != null
        && isZeroOrNull(limit.getOffset())
        && !scan.getScanSpec().hasLimit();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LimitRel        limit = call.rel(0);
    CassandraScanDrel scan = call.rel(hasFilter ? 2 : 1);

    long limitValue = extractLimitValue(limit.getFetch());
    if (limitValue <= 0) {
      return; // non-literal or zero fetch — bail out
    }

    CassandraScanSpec newSpec = scan.getScanSpec().withLimit(limitValue);

    CassandraScanDrel newScan = new CassandraScanDrel(
        scan.getCluster(),
        scan.getTraitSet(),
        scan.getTable(),
        scan.getPluginId(),
        scan.getTableMetadata(),
        scan.getProjectedColumns(),
        scan.getCostAdjustmentFactor(),
        scan.getHints(),
        newSpec
    );

    // Reconstruct the subtree keeping the LimitRel and (if present) FilterRel.
    RelNode result;
    if (hasFilter) {
      FilterRel filter    = call.rel(1);
      RelNode   newFilter = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));
      result = limit.copy(limit.getTraitSet(), List.of(newFilter));
    } else {
      result = limit.copy(limit.getTraitSet(), List.of((RelNode) newScan));
    }

    logger.debug("CassandraLimitRule({}): pushed LIMIT {} to scan spec for {}.{}",
        hasFilter ? "withFilter" : "direct",
        limitValue,
        newSpec.getKeyspace(), newSpec.getTableName());

    call.transformTo(result);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /**
   * Returns {@code true} if {@code offsetNode} is {@code null} or a literal 0.
   * A non-zero OFFSET cannot be expressed in CQL, so we do not push LIMIT in that case.
   */
  private static boolean isZeroOrNull(RexNode offsetNode) {
    if (offsetNode == null) {
      return true;
    }
    if (offsetNode instanceof RexLiteral) {
      Object val = ((RexLiteral) offsetNode).getValue2();
      return val instanceof Number && ((Number) val).longValue() == 0;
    }
    return false;
  }

  /**
   * Extracts the numeric limit value from a fetch {@link RexNode}.
   * Returns {@code 0} if the node is not a numeric literal.
   */
  private static long extractLimitValue(RexNode fetchNode) {
    if (!(fetchNode instanceof RexLiteral)) {
      return 0;
    }
    Object val = ((RexLiteral) fetchNode).getValue2();
    if (val instanceof Number) {
      return ((Number) val).longValue();
    }
    return 0;
  }
}
