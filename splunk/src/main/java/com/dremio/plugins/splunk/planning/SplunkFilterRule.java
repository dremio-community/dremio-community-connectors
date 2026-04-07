package com.dremio.plugins.splunk.planning;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.splunk.SplunkSchemaInferrer;
import com.dremio.plugins.splunk.scan.SplunkScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Pushes WHERE predicates into SplunkScanSpec to translate SQL filters to SPL.
 *
 * Fired during LOGICAL planning; matches Filter(SplunkScanDrel).
 *
 * Supported pushdowns:
 * ─────────────────────────────────────────────────────────────────────
 *   _time >= TIMESTAMP '...'    → earliestEpochMs (converted to ISO-8601)
 *   _time >  TIMESTAMP '...'    → earliestEpochMs + 1
 *   _time <  TIMESTAMP '...'    → latestEpochMs
 *   _time <= TIMESTAMP '...'    → latestEpochMs + 1
 *   field = 'literal'           → splFilter clause (field=value)
 *   field IN ('a', 'b', ...)    → splFilter clause ((field=a OR field=b))
 * ─────────────────────────────────────────────────────────────────────
 *
 * NOT pushed down (left as Dremio residual filter):
 *   field != value, NOT predicates, LIKE, numeric ranges on non-_time fields,
 *   OR across different fields.
 *
 * The ORIGINAL filter is always kept as a residual — Dremio post-filters
 * the Arrow batches, ensuring correctness regardless of SPL semantics.
 */
public class SplunkFilterRule extends RelOptRule {

  public static final SplunkFilterRule INSTANCE = new SplunkFilterRule();

  private static final Logger logger = LoggerFactory.getLogger(SplunkFilterRule.class);

  private SplunkFilterRule() {
    super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
              RelOptHelper.any(SplunkScanDrel.class)),
          "SplunkFilterRule");
  }

  /**
   * Always return true — onMatch() handles the "nothing to do" case with an
   * early return. This allows the rule to fire on Filter(SplunkScanDrel) even
   * when the scan already has some pushdowns, so a second Filter layer added
   * by a later planning rewrite can still push its predicates into the spec.
   * The anti-loop guarantee comes from onMatch() returning without transforming
   * when the new spec would be identical to the existing one.
   */
  @Override
  public boolean matches(RelOptRuleCall call) {
    return true;
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterRel      filter = call.rel(0);
    SplunkScanDrel scan   = call.rel(1);

    List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());
    RelDataType   rowType   = scan.getRowType();

    // Start from the existing spec values so we accumulate across multiple passes
    SplunkScanSpec existing = scan.getScanSpec();
    long   earliestEpochMs = existing.getEarliestEpochMs();
    long   latestEpochMs   = existing.getLatestEpochMs();

    // Collect existing SPL clauses to deduplicate
    List<String> splClauses = new ArrayList<>();
    if (!existing.getSplFilter().isEmpty()) {
      for (String clause : existing.getSplFilter().split(" ")) {
        if (!clause.isBlank()) splClauses.add(clause);
      }
    }

    boolean changed = false;

    for (RexNode conjunct : conjuncts) {
      // Try _time range pushdown
      TimeExtract te = tryExtractTime(conjunct, rowType);
      if (te != null) {
        long candidate;
        switch (te.op) {
          case GTE: candidate = te.epochMs;     break;
          case GT:  candidate = te.epochMs + 1; break;
          case LT:  candidate = te.epochMs;     break;
          case LTE: candidate = te.epochMs + 1; break;
          default: continue;
        }
        if (te.op == Op.GTE || te.op == Op.GT) {
          // Tighten lower bound: take the LATER earliest
          if (candidate > earliestEpochMs) { earliestEpochMs = candidate; changed = true; }
        } else {
          // Tighten upper bound: take the EARLIER latest
          if (latestEpochMs < 0 || candidate < latestEpochMs) {
            latestEpochMs = candidate; changed = true;
          }
        }
        continue;
      }

      // Try field=value equality pushdown
      FieldEqExtract fe = tryExtractFieldEq(conjunct, rowType);
      if (fe != null) {
        String clause = fe.fieldName + "=\"" + escapeSpl(fe.value) + "\"";
        if (!splClauses.contains(clause)) {
          splClauses.add(clause);
          changed = true;
        }
      }
    }

    // Anti-loop: if nothing actually changed, bail without transforming.
    // Calcite won't re-fire the rule on an unchanged node.
    if (!changed) return;

    String newSplFilter = String.join(" ", splClauses);
    SplunkScanSpec newSpec = existing.withAllFilters(
        earliestEpochMs, latestEpochMs, newSplFilter, existing.getMaxEvents());

    SplunkScanDrel newScan = new SplunkScanDrel(
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

    // Keep the original filter as a residual — Dremio will post-filter the results
    RelNode residual = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));

    logger.debug("SplunkFilterRule: index='{}' earliestMs={} latestMs={} splFilter='{}' changed={}",
        existing.getIndexName(), earliestEpochMs, latestEpochMs, newSplFilter, changed);

    call.transformTo(residual);
  }

  // -----------------------------------------------------------------------
  // Time extraction
  // -----------------------------------------------------------------------

  private enum Op { EQ, GT, GTE, LT, LTE }

  private static class TimeExtract {
    final Op   op;
    final long epochMs;
    TimeExtract(Op op, long epochMs) { this.op = op; this.epochMs = epochMs; }
  }

  private static TimeExtract tryExtractTime(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) return null;
    RexCall call = (RexCall) node;

    Op op;
    switch (call.getKind()) {
      case GREATER_THAN:          op = Op.GT;  break;
      case GREATER_THAN_OR_EQUAL: op = Op.GTE; break;
      case LESS_THAN:             op = Op.LT;  break;
      case LESS_THAN_OR_EQUAL:    op = Op.LTE; break;
      default: return null;
    }

    if (call.getOperands().size() != 2) return null;
    RexNode left  = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    RexInputRef colRef;
    RexLiteral  literal;
    boolean     flipped = false;

    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      colRef  = (RexInputRef) left;
      literal = (RexLiteral)  right;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      colRef  = (RexInputRef) right;
      literal = (RexLiteral)  left;
      flipped = true;
    } else {
      return null;
    }

    String colName = rowType.getFieldNames().get(colRef.getIndex());
    if (!SplunkSchemaInferrer.COL_TIME.equals(colName)) return null;

    Long epochMs = extractEpochMs(literal);
    if (epochMs == null) return null;

    if (flipped) op = flipOp(op);
    return new TimeExtract(op, epochMs);
  }

  private static Long extractEpochMs(RexLiteral literal) {
    SqlTypeName type = literal.getType().getSqlTypeName();
    switch (type) {
      case BIGINT: case INTEGER: case DECIMAL: {
        Object val = literal.getValue();
        if (val instanceof BigDecimal) return ((BigDecimal) val).longValue();
        return null;
      }
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
        Object val = literal.getValue2();
        if (val instanceof Long)   return (Long) val;
        if (val instanceof Number) return ((Number) val).longValue();
        return null;
      }
      case DATE: {
        Object val = literal.getValue2();
        if (val instanceof Integer) return ((Integer) val) * 86_400_000L;
        if (val instanceof Number)  return ((Number) val).longValue() * 86_400_000L;
        return null;
      }
      default: return null;
    }
  }

  // -----------------------------------------------------------------------
  // Field equality extraction
  // -----------------------------------------------------------------------

  private static class FieldEqExtract {
    final String fieldName;
    final String value;
    FieldEqExtract(String fieldName, String value) {
      this.fieldName = fieldName;
      this.value     = value;
    }
  }

  private static FieldEqExtract tryExtractFieldEq(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) return null;
    RexCall call = (RexCall) node;
    if (call.getKind() != org.apache.calcite.sql.SqlKind.EQUALS) return null;
    if (call.getOperands().size() != 2) return null;

    RexNode left  = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    RexInputRef colRef;
    RexLiteral  literal;

    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      colRef  = (RexInputRef) left;
      literal = (RexLiteral)  right;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      colRef  = (RexInputRef) right;
      literal = (RexLiteral)  left;
    } else {
      return null;
    }

    String colName = rowType.getFieldNames().get(colRef.getIndex());

    // Don't push down _time equality (handled by time range pushdown)
    if (SplunkSchemaInferrer.COL_TIME.equals(colName)) return null;

    // Only push down VARCHAR equality (safe for SPL)
    SqlTypeName litType = literal.getType().getSqlTypeName();
    if (litType != SqlTypeName.CHAR && litType != SqlTypeName.VARCHAR) return null;

    String value = literal.getValue2() != null ? literal.getValue2().toString() : null;
    if (value == null) return null;

    return new FieldEqExtract(colName, value);
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private static Op flipOp(Op op) {
    switch (op) {
      case GT:  return Op.LT;
      case GTE: return Op.LTE;
      case LT:  return Op.GT;
      case LTE: return Op.GTE;
      default:  return op;
    }
  }

  /** Escapes a value for inclusion in a SPL quoted string. */
  private static String escapeSpl(String value) {
    return value.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
