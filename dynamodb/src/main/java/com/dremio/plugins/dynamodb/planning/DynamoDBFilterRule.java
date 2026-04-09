package com.dremio.plugins.dynamodb.planning;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
import com.dremio.plugins.dynamodb.scan.DynamoDBPredicate;
import com.dremio.plugins.dynamodb.scan.DynamoDBScanSpec;
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
 * into the DynamoDBScanSpec so the RecordReader can emit more selective requests.
 *
 * Fired during LOGICAL planning; matches Filter(DynamoDBScanDrel).
 *
 * Supported predicates: EQ, GT, GTE, LT, LTE, IN (SqlKind.IN and OR chains).
 *
 * Predicate routing:
 *   - Partition key EQ  → pkPredicate  (enables Query API; avoids full scan)
 *   - Sort key range    → sortKeyPredicate (KeyConditionExpression in Query mode)
 *   - All others        → predicates list (FilterExpression)
 *
 * Sort key predicates are only promoted when a pk EQ predicate is also present
 * (DynamoDB Query requires a pk EQ condition).  Otherwise they fall into FilterExpression.
 *
 * The original FilterRel is always kept as a residual for correctness.
 */
public class DynamoDBFilterRule extends RelOptRule {

  public static final DynamoDBFilterRule INSTANCE = new DynamoDBFilterRule();

  private static final Logger logger = LoggerFactory.getLogger(DynamoDBFilterRule.class);

  private DynamoDBFilterRule() {
    super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
              RelOptHelper.any(DynamoDBScanDrel.class)),
          "DynamoDBFilterRule");
  }

  @Override
  public boolean matches(RelOptRuleCall call) {
    DynamoDBScanDrel scan = call.rel(1);
    return !scan.getScanSpec().hasPredicates() && !scan.getScanSpec().hasPkPredicate();
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    FilterRel         filter = call.rel(0);
    DynamoDBScanDrel  scan   = call.rel(1);

    List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());
    List<DynamoDBPredicate> pushed = new ArrayList<>();
    RelDataType rowType = scan.getRowType();

    for (RexNode conjunct : conjuncts) {
      DynamoDBPredicate pred = tryExtract(conjunct, rowType);
      if (pred != null) { pushed.add(pred); continue; }
      DynamoDBPredicate inPred = tryExtractIn(conjunct, rowType);
      if (inPred != null) pushed.add(inPred);
    }

    if (pushed.isEmpty()) return;

    // Route predicates using pk/sk names stored in the scan spec at rule-creation time
    DynamoDBScanSpec spec = scan.getScanSpec();
    String pkName = spec.getPkName();
    String skName = spec.getSkName();

    DynamoDBPredicate pkPredicate      = null;
    DynamoDBPredicate skCandidate      = null; // sort key range — only promoted when pk EQ found
    List<DynamoDBPredicate> filterPreds = new ArrayList<>();

    for (DynamoDBPredicate p : pushed) {
      if (pkName != null && p.getColumn().equals(pkName) && p.isEq() && !p.isIn()) {
        // Partition key EQ → Query API
        pkPredicate = p;
      } else if (skName != null && p.getColumn().equals(skName) && !p.isIn()) {
        // Sort key comparison — keep as candidate; promote only if pk EQ is also present
        skCandidate = p;
      } else {
        filterPreds.add(p);
      }
    }

    // Sort key predicate is only valid in Query mode (requires pk EQ)
    DynamoDBPredicate sortKeyPredicate = null;
    if (pkPredicate != null && skCandidate != null) {
      sortKeyPredicate = skCandidate;
    } else if (skCandidate != null) {
      // No pk EQ — fall back to FilterExpression
      filterPreds.add(skCandidate);
    }

    DynamoDBScanSpec newSpec = spec.withPredicates(filterPreds, pkPredicate, sortKeyPredicate);

    DynamoDBScanDrel newScan = new DynamoDBScanDrel(
        scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getCostAdjustmentFactor(), scan.getHints(), newSpec);

    RelNode residual = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));
    logger.debug("DynamoDBFilterRule: pushed {} predicates (pk={}, sk={}, filter={})",
        pushed.size(), pkPredicate, sortKeyPredicate, filterPreds.size());
    call.transformTo(residual);
  }

  // -----------------------------------------------------------------------
  // Predicate extraction
  // -----------------------------------------------------------------------

  private static DynamoDBPredicate tryExtract(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) return null;
    RexCall call = (RexCall) node;
    DynamoDBPredicate.Op op;
    switch (call.getKind()) {
      case EQUALS:                op = DynamoDBPredicate.Op.EQ;  break;
      case GREATER_THAN:          op = DynamoDBPredicate.Op.GT;  break;
      case GREATER_THAN_OR_EQUAL: op = DynamoDBPredicate.Op.GTE; break;
      case LESS_THAN:             op = DynamoDBPredicate.Op.LT;  break;
      case LESS_THAN_OR_EQUAL:    op = DynamoDBPredicate.Op.LTE; break;
      default: return null;
    }
    if (call.getOperands().size() != 2) return null;
    RexNode left = call.getOperands().get(0), right = call.getOperands().get(1);
    RexInputRef colRef; RexLiteral literal;
    if (left instanceof RexInputRef && right instanceof RexLiteral) {
      colRef = (RexInputRef) left; literal = (RexLiteral) right;
    } else if (left instanceof RexLiteral && right instanceof RexInputRef) {
      colRef = (RexInputRef) right; literal = (RexLiteral) left; op = op.flip();
    } else return null;
    String colName = rowType.getFieldNames().get(colRef.getIndex());
    LiteralValue lv = extractLiteralValue(literal);
    if (lv == null) return null;
    return DynamoDBPredicate.single(colName, op, lv.value, lv.isString);
  }

  private static DynamoDBPredicate tryExtractIn(RexNode node, RelDataType rowType) {
    if (!(node instanceof RexCall)) return null;
    RexCall call = (RexCall) node;
    if (call.getKind() == SqlKind.IN)  return tryExtractSqlKindIn(call, rowType);
    if (call.getKind() == SqlKind.OR)  return tryExtractOrChainAsIn(call, rowType);
    return null;
  }

  private static DynamoDBPredicate tryExtractSqlKindIn(RexCall call, RelDataType rowType) {
    if (call.getOperands().size() < 2) return null;
    RexNode first = call.getOperands().get(0);
    if (!(first instanceof RexInputRef)) return null;
    String colName = rowType.getFieldNames().get(((RexInputRef) first).getIndex());
    List<RexNode> valueNodes;
    if (call.getOperands().size() == 2 && call.getOperands().get(1) instanceof RexCall) {
      valueNodes = ((RexCall) call.getOperands().get(1)).getOperands();
    } else {
      valueNodes = call.getOperands().subList(1, call.getOperands().size());
    }
    return buildInPredicate(colName, valueNodes);
  }

  private static DynamoDBPredicate tryExtractOrChainAsIn(RexCall orCall, RelDataType rowType) {
    if (orCall.getOperands().size() < 2) return null;
    String commonCol = null;
    List<RexNode> literalNodes = new ArrayList<>();
    for (RexNode operand : orCall.getOperands()) {
      if (!(operand instanceof RexCall)) return null;
      RexCall eq = (RexCall) operand;
      if (eq.getKind() != SqlKind.EQUALS || eq.getOperands().size() != 2) return null;
      RexNode l = eq.getOperands().get(0), r = eq.getOperands().get(1);
      RexInputRef colRef; RexLiteral lit;
      if (l instanceof RexInputRef && r instanceof RexLiteral) { colRef = (RexInputRef) l; lit = (RexLiteral) r; }
      else if (l instanceof RexLiteral && r instanceof RexInputRef) { colRef = (RexInputRef) r; lit = (RexLiteral) l; }
      else return null;
      String colName = rowType.getFieldNames().get(colRef.getIndex());
      if (commonCol == null) commonCol = colName;
      else if (!commonCol.equals(colName)) return null;
      literalNodes.add(lit);
    }
    if (commonCol == null || literalNodes.isEmpty()) return null;
    return buildInPredicate(commonCol, literalNodes);
  }

  private static DynamoDBPredicate buildInPredicate(String colName, List<RexNode> valueNodes) {
    if (valueNodes.isEmpty()) return null;
    List<String> values = new ArrayList<>(valueNodes.size());
    Boolean isString = null;
    for (RexNode vn : valueNodes) {
      if (!(vn instanceof RexLiteral)) return null;
      LiteralValue lv = extractLiteralValue((RexLiteral) vn);
      if (lv == null) return null;
      values.add(lv.value);
      isString = lv.isString;
    }
    if (values.isEmpty() || isString == null) return null;
    return DynamoDBPredicate.in(colName, values, isString);
  }

  private static class LiteralValue {
    final String value; final boolean isString;
    LiteralValue(String v, boolean s) { value = v; isString = s; }
  }

  private static LiteralValue extractLiteralValue(RexLiteral literal) {
    SqlTypeName typeName = literal.getType().getSqlTypeName();
    switch (typeName) {
      case CHAR: case VARCHAR: {
        Object val = literal.getValue();
        String str = (val instanceof NlsString) ? ((NlsString) val).getValue()
            : (val != null ? val.toString() : null);
        return str != null ? new LiteralValue(str, true) : null;
      }
      case BOOLEAN: {
        Object val = literal.getValue();
        return val != null ? new LiteralValue(val.toString(), false) : null;
      }
      case INTEGER: case BIGINT: case SMALLINT: case TINYINT:
      case DECIMAL: case FLOAT: case DOUBLE: case REAL: {
        Object val = literal.getValue();
        String str = (val instanceof BigDecimal) ? ((BigDecimal) val).toPlainString()
            : (val != null ? val.toString() : null);
        return str != null ? new LiteralValue(str, false) : null;
      }
      default: {
        Object val = literal.getValue2();
        return val != null ? new LiteralValue(val.toString(), true) : null;
      }
    }
  }
}
