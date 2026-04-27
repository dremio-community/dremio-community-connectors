package com.dremio.plugins.spanner.planning;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Converts a Calcite RexNode expression tree into a Spanner GoogleSQL WHERE-clause fragment.
 *
 * Returns null from any visit method to signal that a node cannot be converted
 * (unsupported function, subquery, etc.). The caller should check for null and
 * fall back to Dremio-side filtering.
 *
 * Supported:
 *   Comparisons: =  <>  <  <=  >  >=
 *   Logical:     AND  OR  NOT
 *   Null checks: IS NULL  IS NOT NULL
 *   Pattern:     LIKE
 *   Range:       BETWEEN … AND …
 *   Set:         IN (…)
 *   Literals:    INT64, FLOAT64, STRING, BOOL, DATE, TIMESTAMP, NULL
 */
public class SpannerFilterConverter extends RexVisitorImpl<String> {

  private final RelDataType rowType;

  public SpannerFilterConverter(RelDataType rowType) {
    super(true);
    this.rowType = rowType;
  }

  /**
   * Entry point: try to convert the given condition. Returns null if not pushable.
   */
  public static String convert(RexNode condition, RelDataType rowType) {
    SpannerFilterConverter conv = new SpannerFilterConverter(rowType);
    try {
      return condition.accept(conv);
    } catch (Exception e) {
      return null;
    }
  }

  // ─── RexInputRef → backtick-quoted column name ──────────────────────────────

  @Override
  public String visitInputRef(RexInputRef ref) {
    String name = rowType.getFieldNames().get(ref.getIndex());
    return "`" + name + "`";
  }

  // ─── RexLiteral → SQL literal ───────────────────────────────────────────────

  @Override
  public String visitLiteral(RexLiteral literal) {
    if (literal.isNull()) return "NULL";

    SqlTypeName typeName = literal.getType().getSqlTypeName();
    switch (typeName) {
      case BOOLEAN:
        return Boolean.TRUE.equals(RexLiteral.booleanValue(literal)) ? "TRUE" : "FALSE";

      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return String.valueOf(((Number) literal.getValue2()).longValue());

      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
        return String.valueOf(((Number) literal.getValue2()).doubleValue());

      case CHAR:
      case VARCHAR:
        return "'" + literal.getValue2().toString().replace("'", "\\'") + "'";

      case DATE: {
        DateString ds = literal.getValueAs(DateString.class);
        return ds != null ? "DATE '" + ds.toString() + "'" : null;
      }

      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE: {
        TimestampString ts = literal.getValueAs(TimestampString.class);
        return ts != null ? "TIMESTAMP '" + ts.toString() + "'" : null;
      }

      default:
        return null; // unsupported literal type — don't push
    }
  }

  // ─── RexCall → SQL expression ───────────────────────────────────────────────

  @Override
  public String visitCall(RexCall call) {
    SqlKind kind = call.getKind();
    List<RexNode> operands = call.getOperands();

    switch (kind) {

      // Binary comparisons
      case EQUALS:             return binary(operands, "=");
      case NOT_EQUALS:         return binary(operands, "<>");
      case LESS_THAN:          return binary(operands, "<");
      case LESS_THAN_OR_EQUAL: return binary(operands, "<=");
      case GREATER_THAN:       return binary(operands, ">");
      case GREATER_THAN_OR_EQUAL: return binary(operands, ">=");

      // Logical
      case AND: return nary(operands, "AND", true);
      case OR:  return nary(operands, "OR",  true);
      case NOT: {
        String inner = operands.get(0).accept(this);
        return inner != null ? "NOT (" + inner + ")" : null;
      }

      // Null checks (unary postfix)
      case IS_NULL: {
        String col = operands.get(0).accept(this);
        return col != null ? col + " IS NULL" : null;
      }
      case IS_NOT_NULL: {
        String col = operands.get(0).accept(this);
        return col != null ? col + " IS NOT NULL" : null;
      }

      // LIKE
      case LIKE: {
        String col     = operands.get(0).accept(this);
        String pattern = operands.get(1).accept(this);
        if (col == null || pattern == null) return null;
        if (operands.size() == 3) return null; // ESCAPE not supported in Spanner LIKE
        return col + " LIKE " + pattern;
      }

      // BETWEEN … AND …  (Calcite expands BETWEEN to >= AND <=, but keep for safety)
      case BETWEEN: {
        String col  = operands.get(0).accept(this);
        String low  = operands.get(1).accept(this);
        String high = operands.get(2).accept(this);
        if (col == null || low == null || high == null) return null;
        return col + " BETWEEN " + low + " AND " + high;
      }

      // IN (list)
      case IN: {
        String col = operands.get(0).accept(this);
        if (col == null) return null;
        List<String> values = operands.subList(1, operands.size()).stream()
            .map(op -> op.accept(this))
            .collect(Collectors.toList());
        if (values.contains(null)) return null;
        return col + " IN (" + String.join(", ", values) + ")";
      }

      // CASE and everything else: don't push
      default:
        return null;
    }
  }

  // ─── Helpers ────────────────────────────────────────────────────────────────

  private String binary(List<RexNode> operands, String op) {
    String left  = operands.get(0).accept(this);
    String right = operands.get(1).accept(this);
    if (left == null || right == null) return null;
    return "(" + left + " " + op + " " + right + ")";
  }

  /**
   * N-ary logical operator (AND/OR). In partial-pushdown mode, AND allows
   * pushing the pushable terms and dropping the rest (returns only what can
   * be pushed). For OR, all terms must be pushable or we push nothing.
   */
  private String nary(List<RexNode> operands, String op, boolean isTopLevel) {
    if ("AND".equals(op)) {
      // Partial pushdown: push any convertible terms, ignore the rest
      List<String> parts = operands.stream()
          .map(o -> o.accept(this))
          .filter(s -> s != null)
          .collect(Collectors.toList());
      if (parts.isEmpty()) return null;
      return parts.size() == 1 ? parts.get(0)
          : "(" + String.join(" AND ", parts) + ")";
    } else {
      // OR: all arms must be pushable
      List<String> parts = operands.stream()
          .map(o -> o.accept(this))
          .collect(Collectors.toList());
      if (parts.contains(null)) return null;
      return "(" + String.join(" OR ", parts) + ")";
    }
  }
}
