package com.dremio.plugins.cassandra.planning;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.cassandra.scan.CassandraPredicate;
import com.dremio.plugins.cassandra.scan.CassandraScanSpec;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Pushes simple column-comparison-literal predicates from a logical Filter
 * into the CassandraScanSpec so the RecordReader can emit a more selective CQL query.
 *
 * Fired during the LOGICAL planning phase; matches Filter(CassandraScanDrel).
 *
 * Supported predicate shapes
 * --------------------------
 *   col = literal          → EQ
 *   col > / >= / < / <=    → GT / GTE / LT / LTE
 *   col IN (v1, v2, ...)   → IN  (via SqlKind.IN or OR-chain expansion)
 *
 * Strategy
 * --------
 * 1. Split the filter condition on top-level AND conjuncts.
 * 2. For each conjunct try (in order):
 *    a. Simple col-op-literal  → single-value CassandraPredicate
 *    b. SqlKind.IN             → multi-value CassandraPredicate
 *    c. OR(col=v1, col=v2, …)  → multi-value CassandraPredicate
 *       (Dremio/Calcite sometimes expands IN to OR chains before rules fire)
 * 3. Bake matching predicates into a new CassandraScanDrel.
 * 4. Keep the ORIGINAL filter as a residual — Dremio always post-filters,
 *    so results are always correct regardless of what CQL pushes.
 *
 * The RecordReader (CassandraRecordReader) validates predicates at execution
 * time against live Cassandra schema metadata before issuing any CQL.
 */
public class CassandraFilterRule extends RelOptRule {

  public static final CassandraFilterRule INSTANCE = new CassandraFilterRule();

  private static final Logger logger = LoggerFactory.getLogger(CassandraFilterRule.class);

  private CassandraFilterRule() {
    super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
              RelOptHelper.any(CassandraScanDrel.class)),
          "CassandraFilterRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    CassandraScanDrel scan = call.rel(1);
    // Don't fire again if the scan already carries predicates
    return !scan.getScanSpec().hasPredicates();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterRel        filter = call.rel(0);
    CassandraScanDrel  scan = call.rel(1);

    // Split the filter condition on AND boundaries
    List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());
    List<CassandraPredicate> pushed = new ArrayList<>();

    RelDataType rowType = scan.getRowType();
    for (RexNode conjunct : conjuncts) {
      // 1. Try col op literal  (EQ / GT / GTE / LT / LTE)
      CassandraPredicate pred = tryExtract(conjunct, rowType);
      if (pred != null) {
        pushed.add(pred);
        continue;
      }
      // 2. Try SqlKind.IN  and  OR(col=v1, col=v2, …) → IN
      CassandraPredicate inPred = tryExtractIn(conjunct, rowType);
      if (inPred != null) {
        pushed.add(inPred);
      }
    }

    if (pushed.isEmpty()) {
      return; // nothing pushable — leave tree unchanged
    }

    // Build a new scan spec that carries the candidate predicates
    CassandraScanSpec oldSpec = scan.getScanSpec();
    CassandraScanSpec newSpec = new CassandraScanSpec(
        oldSpec.getKeyspace(),
        oldSpec.getTableName(),
        oldSpec.getPartitionKeys(),
        oldSpec.getTokenRangeStart(),
        oldSpec.getTokenRangeEnd(),
        pushed,
        oldSpec.getSplitParallelism()
    );

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

    // Keep the original filter as a residual — correctness is always guaranteed
    // by Dremio's post-scan filter regardless of what CQL pushes.
    RelNode residual = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));

    logger.debug("CassandraFilterRule: pushed {} predicate(s) to scan spec for {}.{}",
        pushed.size(), oldSpec.getKeyspace(), oldSpec.getTableName());

    call.transformTo(residual);
  }

  // ---------------------------------------------------------------------------
  // Single-value predicate extraction (EQ, GT, GTE, LT, LTE)
  // ---------------------------------------------------------------------------

  /**
   * Attempts to convert a conjunct into a single-value CassandraPredicate.
   * Handles: col op literal  AND  literal op col (operator is flipped for the latter).
   * Returns null if the conjunct is not a simple binary comparison.
   */
  private static CassandraPredicate tryExtract(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) node;

    CassandraPredicate.Op op;
    switch (call.getKind()) {
      case EQUALS:                op = CassandraPredicate.Op.EQ;  break;
      case GREATER_THAN:          op = CassandraPredicate.Op.GT;  break;
      case GREATER_THAN_OR_EQUAL: op = CassandraPredicate.Op.GTE; break;
      case LESS_THAN:             op = CassandraPredicate.Op.LT;  break;
      case LESS_THAN_OR_EQUAL:    op = CassandraPredicate.Op.LTE; break;
      default: return null;
    }

    if (call.getOperands().size() != 2) {
      return null;
    }

    RexNode left  = call.getOperands().get(0);
    RexNode right = call.getOperands().get(1);

    // Accept: col op literal  —OR—  literal op col (flip operator for the latter)
    RexInputRef colRef;
    RexLiteral  literal;
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      colRef  = (RexInputRef) left;
      literal = (RexLiteral)  right;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      colRef  = (RexInputRef) right;
      literal = (RexLiteral)  left;
      op      = op.flip();
    } else {
      return null;
    }

    String colName = rowType.getFieldNames().get(colRef.getIndex());
    LiteralValue lv = extractLiteralValue(literal);
    if (lv == null) {
      return null;
    }

    return new CassandraPredicate(colName, op, lv.value, lv.isString);
  }

  // ---------------------------------------------------------------------------
  // IN predicate extraction
  // ---------------------------------------------------------------------------

  /**
   * Attempts to extract an IN predicate from a conjunct.
   *
   * Handles two representations that Dremio/Calcite may produce:
   *
   *   Form A — SqlKind.IN (static list not yet expanded):
   *     operands[0] = column ref
   *     operands[1] = RexCall(ARRAY_VALUE_CONSTRUCTOR | ROW, [lit1, lit2, …])
   *     — or all literals are direct operands: [col, lit1, lit2, …]
   *
   *   Form B — OR chain (Calcite expands IN to OR before rules fire):
   *     OR( =(col, v1), =(col, v2), … )
   *     All EQ branches must reference the same column.
   *
   * Returns null if neither form matches.
   */
  private static CassandraPredicate tryExtractIn(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) {
      return null;
    }
    RexCall call = (RexCall) node;

    if (call.getKind() == SqlKind.IN) {
      return tryExtractSqlKindIn(call, rowType);
    }
    if (call.getKind() == SqlKind.OR) {
      return tryExtractOrChainAsIn(call, rowType);
    }
    return null;
  }

  /**
   * Handles SqlKind.IN where Calcite has kept the IN node intact.
   *
   * Expected operand structures:
   *   [colRef, lit1, lit2, …]
   *   [colRef, RexCall(ROW|ARRAY_VALUE_CONSTRUCTOR, [lit1, lit2, …])]
   */
  private static CassandraPredicate tryExtractSqlKindIn(RexCall call, RelDataType rowType) {
    if (call.getOperands().size() < 2) {
      return null;
    }
    RexNode first = call.getOperands().get(0);
    if (!(first instanceof RexInputRef)) {
      return null; // LHS must be a column reference
    }

    String colName = rowType.getFieldNames().get(((RexInputRef) first).getIndex());

    // Collect the literal nodes — they may be direct operands or wrapped in ROW/ARRAY
    List<RexNode> valueNodes;
    if (call.getOperands().size() == 2
        && call.getOperands().get(1) instanceof RexCall) {
      // [col, ROW(lit1, lit2, …)] or [col, ARRAY(lit1, lit2, …)]
      valueNodes = ((RexCall) call.getOperands().get(1)).getOperands();
    } else {
      // [col, lit1, lit2, …]
      valueNodes = call.getOperands().subList(1, call.getOperands().size());
    }

    return buildInPredicate(colName, valueNodes);
  }

  /**
   * Handles OR(=(col,v1), =(col,v2), …) produced when Calcite expands a static IN list.
   * All branches must be EQ comparisons on the same column.
   */
  private static CassandraPredicate tryExtractOrChainAsIn(RexCall orCall, RelDataType rowType) {
    if (orCall.getOperands().size() < 2) {
      return null;
    }

    String commonCol = null;
    List<RexNode> literalNodes = new ArrayList<>();

    for (RexNode operand : orCall.getOperands()) {
      if (!(operand instanceof RexCall)) return null;
      RexCall eq = (RexCall) operand;
      if (eq.getKind() != SqlKind.EQUALS) return null;
      if (eq.getOperands().size() != 2) return null;

      RexNode left  = eq.getOperands().get(0);
      RexNode right = eq.getOperands().get(1);

      RexInputRef colRef;
      RexLiteral  lit;
      if (left instanceof RexInputRef && right instanceof RexLiteral) {
        colRef = (RexInputRef) left;
        lit    = (RexLiteral)  right;
      } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
        colRef = (RexInputRef) right;
        lit    = (RexLiteral)  left;
      } else {
        return null; // branch is not a simple col=literal
      }

      String colName = rowType.getFieldNames().get(colRef.getIndex());
      if (commonCol == null) {
        commonCol = colName;
      } else if (!commonCol.equals(colName)) {
        return null; // different columns in different branches — not an IN
      }
      literalNodes.add(lit);
    }

    if (commonCol == null || literalNodes.isEmpty()) {
      return null;
    }
    return buildInPredicate(commonCol, literalNodes);
  }

  /**
   * Converts a list of RexLiteral nodes into an IN CassandraPredicate.
   * Returns null if any node is not a literal or the list is empty.
   */
  private static CassandraPredicate buildInPredicate(String colName,
                                                      List<RexNode> valueNodes) {
    if (valueNodes.isEmpty()) {
      return null;
    }

    List<String> values    = new ArrayList<>(valueNodes.size());
    Boolean      isString  = null; // determined from the first non-null value

    for (RexNode vn : valueNodes) {
      if (!(vn instanceof RexLiteral)) {
        return null; // non-literal in the list — bail out
      }
      LiteralValue lv = extractLiteralValue((RexLiteral) vn);
      if (lv == null) {
        return null; // null literal or unsupported type
      }
      values.add(lv.value);
      // All literals in an IN list share the same type, so isString is consistent
      isString = lv.isString;
    }

    if (values.isEmpty() || isString == null) {
      return null;
    }
    return CassandraPredicate.in(colName, values, isString);
  }

  // ---------------------------------------------------------------------------
  // Shared literal extraction
  // ---------------------------------------------------------------------------

  /** Holds the string form of a literal and whether it needs CQL single-quoting. */
  private static class LiteralValue {
    final String  value;
    final boolean isString;
    LiteralValue(String value, boolean isString) {
      this.value    = value;
      this.isString = isString;
    }
  }

  /**
   * Extracts the string representation and quoting flag from a Calcite RexLiteral.
   * Returns null for null literals or unsupported types.
   */
  private static LiteralValue extractLiteralValue(RexLiteral literal) {
    SqlTypeName typeName = literal.getType().getSqlTypeName();
    switch (typeName) {
      case CHAR:
      case VARCHAR: {
        Object val = literal.getValue();
        String str;
        if (val instanceof NlsString) {
          str = ((NlsString) val).getValue();
        } else if (val != null) {
          str = val.toString();
        } else {
          return null; // null literal — skip
        }
        return new LiteralValue(str, true);
      }
      case BOOLEAN: {
        Object val = literal.getValue();
        if (val == null) return null;
        return new LiteralValue(val.toString().toLowerCase(), false);
      }
      case INTEGER:
      case BIGINT:
      case SMALLINT:
      case TINYINT:
      case DECIMAL:
      case FLOAT:
      case DOUBLE:
      case REAL: {
        Object val = literal.getValue();
        String str;
        if (val instanceof BigDecimal) {
          str = ((BigDecimal) val).toPlainString();
        } else if (val != null) {
          str = val.toString();
        } else {
          return null;
        }
        return new LiteralValue(str, false);
      }
      default: {
        // DATE, TIMESTAMP, UUID, and other types: getValue2() produces a
        // Java object whose toString() is CQL-compatible; wrap in single quotes.
        Object val = literal.getValue2();
        if (val == null) return null;
        return new LiteralValue(val.toString(), true);
      }
    }
  }
}
