package com.dremio.plugins.kafka.planning;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.kafka.KafkaStoragePlugin;
import com.dremio.plugins.kafka.scan.KafkaScanSpec;
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
import java.util.List;

/**
 * Pushes _partition, _offset, and _timestamp predicates from a logical Filter
 * into KafkaScanSpec so the RecordReader can skip irrelevant partitions and
 * seek directly to the correct offset/timestamp range.
 *
 * Fired during LOGICAL planning phase; matches Filter(KafkaScanDrel).
 *
 * Supported predicate shapes
 * --------------------------
 *   _partition = <int literal>          → partitionFilter
 *   _offset >= / > / < / <= <literal>   → offsetStartFilter / offsetEndFilter
 *   _timestamp >= / > / < / <= <ms>     → timestampStartMs / timestampEndMs
 *
 * Timestamp literals accepted
 * ---------------------------
 *   BIGINT epoch-milliseconds:   WHERE _timestamp >= 1700000000000
 *   TIMESTAMP literal:           WHERE _timestamp >= TIMESTAMP '2023-11-15 00:00:00'
 *   DATE literal:                WHERE _timestamp >= DATE '2023-11-15'
 *
 * Timestamp → offset resolution
 * -----------------------------
 * timestampStartMs / timestampEndMs are stored in KafkaScanSpec as pre-adjusted
 * epoch-millisecond values. KafkaRecordReader resolves them to actual partition
 * offsets at execution time via KafkaConsumer.offsetsForTimes(), then seeks
 * directly — zero records polled before the matching range.
 *
 * Strategy
 * --------
 * 1. Split the filter condition on AND conjuncts.
 * 2. Extract pushable predicates from each conjunct.
 * 3. Encode them into a new KafkaScanSpec via withFilters().
 * 4. Keep the ORIGINAL filter as a residual — Dremio post-filters, so results
 *    are always correct regardless of what the reader skips.
 */
public class KafkaFilterRule extends RelOptRule {

  public static final KafkaFilterRule INSTANCE = new KafkaFilterRule();

  private static final Logger logger = LoggerFactory.getLogger(KafkaFilterRule.class);

  private KafkaFilterRule() {
    super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
              RelOptHelper.any(KafkaScanDrel.class)),
          "KafkaFilterRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    KafkaScanDrel scan = call.rel(1);
    // Don't re-fire once pushdown has already been recorded on the spec
    return !scan.getScanSpec().hasFilterPushdown();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterRel     filter = call.rel(0);
    KafkaScanDrel scan   = call.rel(1);

    List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());
    RelDataType   rowType   = scan.getRowType();

    // Mutable pushdown state (start with "no filter" sentinel values)
    int  partitionFilter   = -1;
    long offsetStartFilter = -1L;
    long offsetEndFilter   = -1L;
    long timestampStartMs  = -1L;
    long timestampEndMs    = -1L;

    for (RexNode conjunct : conjuncts) {
      PushdownExtract ex = tryExtract(conjunct, rowType);
      if (ex == null) continue;

      switch (ex.column) {
        case KafkaStoragePlugin.COL_PARTITION:
          if (ex.op == Op.EQ) {
            partitionFilter = (int) ex.value;
          }
          break;

        case KafkaStoragePlugin.COL_OFFSET:
          switch (ex.op) {
            case GTE: offsetStartFilter = ex.value;         break;
            case GT:  offsetStartFilter = ex.value + 1;     break;
            case LT:  offsetEndFilter   = ex.value;         break;
            case LTE: offsetEndFilter   = ex.value + 1;     break;
            default:  break; // EQ on offset: no efficient pushdown
          }
          break;

        case KafkaStoragePlugin.COL_TIMESTAMP:
          // Pre-adjust for operator semantics so RecordReader passes the value
          // directly to offsetsForTimes() without further adjustment.
          switch (ex.op) {
            case GTE: timestampStartMs = ex.value;         break;
            case GT:  timestampStartMs = ex.value + 1;     break;
            case LT:  timestampEndMs   = ex.value;         break;
            case LTE: timestampEndMs   = ex.value + 1;     break;
            default:  break; // EQ on timestamp: no efficient pushdown
          }
          break;

        default:
          break;
      }
    }

    if (partitionFilter < 0 && offsetStartFilter < 0 && offsetEndFilter < 0
        && timestampStartMs < 0 && timestampEndMs < 0) {
      return; // nothing pushable found
    }

    KafkaScanSpec newSpec = scan.getScanSpec()
        .withFilters(partitionFilter, offsetStartFilter, offsetEndFilter,
                     timestampStartMs, timestampEndMs);

    KafkaScanDrel newScan = new KafkaScanDrel(
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

    // Keep original filter as a residual — Dremio will post-filter the results
    RelNode residual = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));

    logger.debug("KafkaFilterRule: pushed down for topic {}: "
        + "partitionFilter={}, offsetStart={}, offsetEnd={}, tsStart={}, tsEnd={}",
        scan.getScanSpec().getTopic(),
        partitionFilter, offsetStartFilter, offsetEndFilter,
        timestampStartMs, timestampEndMs);

    call.transformTo(residual);
  }

  // ---------------------------------------------------------------------------
  // Extraction helpers
  // ---------------------------------------------------------------------------

  private enum Op { EQ, GT, GTE, LT, LTE }

  private static class PushdownExtract {
    final String column;
    final Op     op;
    final long   value;

    PushdownExtract(String column, Op op, long value) {
      this.column = column;
      this.op     = op;
      this.value  = value;
    }
  }

  /**
   * Tries to extract a pushable predicate from a single conjunct.
   * Handles: col op literal  AND  literal op col (operator flipped for the latter).
   * Returns null if not a supported shape or column.
   */
  private static PushdownExtract tryExtract(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) return null;
    RexCall call = (RexCall) node;

    Op op;
    switch (call.getKind()) {
      case EQUALS:                op = Op.EQ;  break;
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

    // Only push down our three metadata columns
    if (!KafkaStoragePlugin.COL_PARTITION.equals(colName)
        && !KafkaStoragePlugin.COL_OFFSET.equals(colName)
        && !KafkaStoragePlugin.COL_TIMESTAMP.equals(colName)) {
      return null;
    }

    Long val = extractLongValue(literal);
    if (val == null) return null;

    if (flipped) {
      op = flipOp(op);
    }

    return new PushdownExtract(colName, op, val);
  }

  private static Op flipOp(Op op) {
    switch (op) {
      case GT:  return Op.LT;
      case GTE: return Op.LTE;
      case LT:  return Op.GT;
      case LTE: return Op.GTE;
      default:  return op;
    }
  }

  /**
   * Extracts a long value from a Calcite literal.
   *
   * Handles:
   *   BIGINT / INTEGER / DECIMAL  — direct numeric value (e.g. epoch-ms as BIGINT)
   *   TIMESTAMP                   — epoch milliseconds via getValue2()
   *   DATE                        — days since epoch × 86400000 → epoch ms
   */
  private static Long extractLongValue(RexLiteral literal) {
    SqlTypeName type = literal.getType().getSqlTypeName();
    switch (type) {
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case TINYINT:
      case DECIMAL: {
        Object val = literal.getValue();
        if (val instanceof BigDecimal) {
          return ((BigDecimal) val).longValue();
        }
        return null;
      }
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
        // getValue2() returns milliseconds since epoch for TIMESTAMP types in Calcite
        Object val = literal.getValue2();
        if (val instanceof Long)    return (Long) val;
        if (val instanceof Number)  return ((Number) val).longValue();
        return null;
      }
      case DATE: {
        // getValue2() returns days since epoch for DATE types; convert to ms
        Object val = literal.getValue2();
        if (val instanceof Integer) return ((Integer) val) * 86_400_000L;
        if (val instanceof Number)  return ((Number) val).longValue() * 86_400_000L;
        return null;
      }
      default:
        return null;
    }
  }
}
