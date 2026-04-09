package com.dremio.plugins.dynamodb.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

/**
 * A simple predicate pushed down from Dremio's filter layer into the DynamoDB scan.
 *
 * Supports EQ, GT, GTE, LT, LTE, and IN operators.
 * Used to build DynamoDB FilterExpression strings and ExpressionAttributeValues.
 *
 * Note: FilterExpression in DynamoDB Scan is evaluated server-side but does NOT
 * reduce read capacity units consumed — the full table is still scanned. However
 * it does reduce network transfer. For Query-path predicates (partition key EQ),
 * DynamoDBRecordReader uses KeyConditionExpression instead, which IS efficient.
 */
public class DynamoDBPredicate {

  public enum Op {
    EQ, GT, GTE, LT, LTE, IN;

    public Op flip() {
      switch (this) {
        case GT:  return LT;
        case GTE: return LTE;
        case LT:  return GT;
        case LTE: return GTE;
        default:  return this;
      }
    }

    public String toDynamoOp() {
      switch (this) {
        case EQ:  return "=";
        case GT:  return ">";
        case GTE: return ">=";
        case LT:  return "<";
        case LTE: return "<=";
        default:  return "=";
      }
    }
  }

  private final String column;
  private final Op op;
  private final String value;       // single value (EQ/GT/GTE/LT/LTE)
  private final List<String> inValues; // multi-value (IN)
  private final boolean isString;   // whether value(s) should be treated as DynamoDB S type

  @JsonCreator
  public DynamoDBPredicate(
      @JsonProperty("column")   String       column,
      @JsonProperty("op")       Op           op,
      @JsonProperty("value")    String       value,
      @JsonProperty("inValues") List<String> inValues,
      @JsonProperty("isString") boolean      isString) {
    this.column   = column;
    this.op       = op;
    this.value    = value;
    this.inValues = inValues;
    this.isString = isString;
  }

  public static DynamoDBPredicate single(String column, Op op, String value, boolean isString) {
    return new DynamoDBPredicate(column, op, value, null, isString);
  }

  public static DynamoDBPredicate in(String column, List<String> values, boolean isString) {
    return new DynamoDBPredicate(column, Op.IN, null, values, isString);
  }

  @JsonProperty("column")   public String       getColumn()   { return column; }
  @JsonProperty("op")       public Op           getOp()       { return op; }
  @JsonProperty("value")    public String       getValue()    { return value; }
  @JsonProperty("inValues") public List<String> getInValues() { return inValues; }
  @JsonProperty("isString") public boolean      isString()    { return isString; }

  public boolean isIn()    { return op == Op.IN; }
  public boolean isEq()    { return op == Op.EQ; }

  /**
   * Returns the DynamoDB FilterExpression fragment for this predicate.
   * Uses placeholder names (#attrN) and values (:valN) keyed by {@code index}.
   *
   * Example (index=0, EQ): "#attr0 = :val0"
   * Example (index=1, IN): "#attr1 IN (:val1_0, :val1_1)"
   */
  public String toFilterFragment(int index) {
    String attrRef = "#attr" + index;
    if (op == Op.IN && inValues != null) {
      StringBuilder sb = new StringBuilder(attrRef).append(" IN (");
      for (int i = 0; i < inValues.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append(":val").append(index).append("_").append(i);
      }
      sb.append(")");
      return sb.toString();
    }
    return attrRef + " " + op.toDynamoOp() + " :val" + index;
  }

  /**
   * Adds this predicate's ExpressionAttributeNames and ExpressionAttributeValues
   * entries to the provided maps.
   */
  public void addToExpressionMaps(int index,
                                   java.util.Map<String, String> nameMap,
                                   java.util.Map<String, software.amazon.awssdk.services.dynamodb.model.AttributeValue> valueMap) {
    nameMap.put("#attr" + index, column);

    if (op == Op.IN && inValues != null) {
      for (int i = 0; i < inValues.size(); i++) {
        valueMap.put(":val" + index + "_" + i, toAttributeValue(inValues.get(i)));
      }
    } else if (value != null) {
      valueMap.put(":val" + index, toAttributeValue(value));
    }
  }

  private software.amazon.awssdk.services.dynamodb.model.AttributeValue toAttributeValue(String val) {
    if (isString) {
      return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().s(val).build();
    } else {
      // Try number first, fall back to string
      return software.amazon.awssdk.services.dynamodb.model.AttributeValue.builder().n(val).build();
    }
  }

  @Override
  public String toString() {
    if (op == Op.IN) return column + " IN " + inValues;
    return column + " " + op + " " + value;
  }
}
