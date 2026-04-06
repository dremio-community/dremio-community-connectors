package com.dremio.plugins.cassandra.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * A single filter predicate extracted from Dremio's logical plan and pushed
 * down into a CassandraScanSpec.
 *
 * Serialized as JSON so it survives SubScan distribution to executor nodes.
 *
 * Supports two shapes:
 *   - Single-value: EQ, GT, GTE, LT, LTE — carry one {@code literal} string.
 *   - Multi-value:  IN — carry a {@code literals} list.
 *
 * The RecordReader validates which predicates are safe to push to CQL:
 *   - EQ and IN on ALL partition key columns → direct partition lookup.
 *   - Range predicates (GT/GTE/LT/LTE) on clustering columns → server-side CQL filter.
 *   - Everything else → handled by Dremio's residual filter (always correct).
 */
public class CassandraPredicate {

  public enum Op {
    EQ("="), GT(">"), GTE(">="), LT("<"), LTE("<="), IN("IN");

    private final String cql;

    Op(String cql) {
      this.cql = cql;
    }

    public String toCql() {
      return cql;
    }

    /** Flip the operator for reversed operand order (e.g. literal op col → col flipped-op literal). */
    public Op flip() {
      switch (this) {
        case GT:  return LT;
        case GTE: return LTE;
        case LT:  return GT;
        case LTE: return GTE;
        default:  return this; // EQ and IN are symmetric
      }
    }
  }

  private final String column;
  private final Op op;

  /**
   * Single literal value — used for EQ, GT, GTE, LT, LTE.
   * Null when op is IN.
   */
  private final String literal;

  /**
   * Multiple literal values — used for IN only.
   * Null when op is not IN.
   */
  private final List<String> literals;

  /** True if CQL literals must be single-quoted (text, uuid, timestamp, etc.). */
  private final boolean literalIsString;

  /**
   * Primary constructor — used by Jackson for deserialization.
   * Handles both single-value and IN predicates.
   * The {@code literal} field is absent/null in IN predicates; {@code literals} is
   * absent/null in single-value predicates — both default cleanly via Jackson.
   */
  @JsonCreator
  public CassandraPredicate(
      @JsonProperty("column")          String       column,
      @JsonProperty("op")              Op           op,
      @JsonProperty("literal")         String       literal,
      @JsonProperty("literals")        List<String> literals,
      @JsonProperty("literalIsString") boolean      literalIsString) {
    this.column          = column;
    this.op              = op;
    this.literal         = literal;
    this.literals        = (literals != null)
        ? Collections.unmodifiableList(literals) : null;
    this.literalIsString = literalIsString;
  }

  /** Convenience constructor for EQ / GT / GTE / LT / LTE (single literal). */
  public CassandraPredicate(String column, Op op, String literal, boolean literalIsString) {
    this(column, op, literal, null, literalIsString);
  }

  /** Factory method for IN predicates (multiple literals). */
  public static CassandraPredicate in(String column, List<String> literals,
                                      boolean literalIsString) {
    return new CassandraPredicate(column, Op.IN, null, literals, literalIsString);
  }

  @JsonProperty("column")
  public String getColumn() { return column; }

  @JsonProperty("op")
  public Op getOp() { return op; }

  @JsonProperty("literal")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getLiteral() { return literal; }

  @JsonProperty("literals")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getLiterals() { return literals; }

  @JsonProperty("literalIsString")
  public boolean isLiteralIsString() { return literalIsString; }

  /**
   * Returns a CQL WHERE-clause fragment for this predicate.
   *
   * Single-value: {@code "user_id" = 'alice'}  or  {@code "age" >= 21}
   * IN:           {@code "status" IN ('active', 'pending')}
   */
  public String toCqlFragment() {
    String quotedCol = "\"" + column + "\"";

    if (op == Op.IN) {
      StringBuilder sb = new StringBuilder(quotedCol).append(" IN (");
      for (int i = 0; i < literals.size(); i++) {
        if (i > 0) sb.append(", ");
        if (literalIsString) {
          sb.append("'").append(literals.get(i).replace("'", "''")).append("'");
        } else {
          sb.append(literals.get(i));
        }
      }
      sb.append(")");
      return sb.toString();
    }

    // Single-value operators
    String cqlLiteral = literalIsString
        ? "'" + literal.replace("'", "''") + "'"
        : literal;
    return quotedCol + " " + op.toCql() + " " + cqlLiteral;
  }

  @Override
  public String toString() {
    return toCqlFragment();
  }
}
