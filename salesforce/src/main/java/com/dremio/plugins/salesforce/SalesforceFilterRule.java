/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.salesforce;

import com.dremio.exec.planner.logical.FilterRel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.planner.logical.RelOptHelper;
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
 * Pushes simple column-comparison-literal predicates from a logical FilterRel into the
 * SOQL WHERE clause of a {@link SalesforceScanDrel}.
 *
 * <p>Supported predicates: =, !=, &lt;, &gt;, &lt;=, &gt;=, IS NULL, IS NOT NULL, AND, OR.
 *
 * <p>The original FilterRel is always kept as a residual for correctness — Dremio
 * re-applies it after the scan.
 */
public class SalesforceFilterRule extends RelOptRule {

    public static final SalesforceFilterRule INSTANCE = new SalesforceFilterRule();

    private static final Logger logger = LoggerFactory.getLogger(SalesforceFilterRule.class);

    private SalesforceFilterRule() {
        super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
                        RelOptHelper.any(SalesforceScanDrel.class)),
                "SalesforceFilterRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FilterRel filter = call.rel(0);
        SalesforceScanDrel scan = call.rel(1);

        RelDataType rowType = scan.getRowType();
        List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());

        List<String> pushed = new ArrayList<>();
        for (RexNode conjunct : conjuncts) {
            String soqlPred = tryConvertToSoql(conjunct, rowType);
            if (soqlPred != null) {
                pushed.add(soqlPred);
            }
        }

        if (pushed.isEmpty()) {
            return;
        }

        String whereClause = String.join(" AND ", pushed);
        SalesforceScanSpec newSpec = scan.getScanSpec().withWhereClause(whereClause);

        SalesforceScanDrel newScan = new SalesforceScanDrel(
                scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getPluginId(),
                scan.getTableMetadata(), scan.getProjectedColumns(),
                scan.getCostAdjustmentFactor(), scan.getHints(), newSpec);

        // Keep residual filter — Dremio will re-apply it for correctness
        RelNode residual = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));
        logger.debug("SalesforceFilterRule: pushed {} conjuncts to SOQL WHERE", pushed.size());
        call.transformTo(residual);
    }

    // -------------------------------------------------------------------------
    // SOQL predicate conversion
    // -------------------------------------------------------------------------

    private String tryConvertToSoql(RexNode node, RelDataType rowType) {
        if (!(node instanceof RexCall)) return null;
        RexCall call = (RexCall) node;

        switch (call.getKind()) {
            case AND: {
                List<String> parts = new ArrayList<>();
                for (RexNode op : call.getOperands()) {
                    String part = tryConvertToSoql(op, rowType);
                    if (part == null) return null;  // can't push partial AND
                    parts.add(part);
                }
                return "(" + String.join(" AND ", parts) + ")";
            }
            case OR: {
                List<String> parts = new ArrayList<>();
                for (RexNode op : call.getOperands()) {
                    String part = tryConvertToSoql(op, rowType);
                    if (part == null) return null;  // can't push partial OR
                    parts.add(part);
                }
                return "(" + String.join(" OR ", parts) + ")";
            }
            case EQUALS:                return convertComparison(call, "=", rowType);
            case NOT_EQUALS:            return convertComparison(call, "!=", rowType);
            case LESS_THAN:             return convertComparison(call, "<", rowType);
            case GREATER_THAN:          return convertComparison(call, ">", rowType);
            case LESS_THAN_OR_EQUAL:    return convertComparison(call, "<=", rowType);
            case GREATER_THAN_OR_EQUAL: return convertComparison(call, ">=", rowType);
            case IS_NULL: {
                String col = columnName(call.getOperands().get(0), rowType);
                return col != null ? col + " = null" : null;
            }
            case IS_NOT_NULL: {
                String col = columnName(call.getOperands().get(0), rowType);
                return col != null ? col + " != null" : null;
            }
            default:
                return null;
        }
    }

    private String convertComparison(RexCall call, String op, RelDataType rowType) {
        if (call.getOperands().size() != 2) return null;
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        String leftStr = operandToSoql(left, rowType);
        String rightStr = operandToSoql(right, rowType);
        if (leftStr == null || rightStr == null) return null;

        return leftStr + " " + op + " " + rightStr;
    }

    private String operandToSoql(RexNode node, RelDataType rowType) {
        if (node instanceof RexInputRef) {
            return columnName(node, rowType);
        }
        if (node instanceof RexLiteral) {
            return literalToSoql((RexLiteral) node);
        }
        return null;
    }

    private String columnName(RexNode node, RelDataType rowType) {
        if (!(node instanceof RexInputRef)) return null;
        int index = ((RexInputRef) node).getIndex();
        List<String> names = rowType.getFieldNames();
        if (index < 0 || index >= names.size()) return null;
        return names.get(index);
    }

    private String literalToSoql(RexLiteral lit) {
        if (lit.isNull()) return "null";

        SqlTypeName typeName = lit.getType().getSqlTypeName();
        switch (typeName) {
            case CHAR:
            case VARCHAR: {
                Object val = lit.getValue();
                String str = (val instanceof NlsString) ? ((NlsString) val).getValue()
                        : (val != null ? val.toString() : null);
                if (str == null) return "null";
                return "'" + str.replace("\\", "\\\\").replace("'", "\\'") + "'";
            }
            case BOOLEAN: {
                Object val = lit.getValue();
                return val != null ? val.toString() : "null";
            }
            case INTEGER:
            case BIGINT:
            case SMALLINT:
            case TINYINT:
            case DECIMAL:
            case FLOAT:
            case DOUBLE:
            case REAL: {
                Object val = lit.getValue();
                if (val instanceof BigDecimal) return ((BigDecimal) val).toPlainString();
                return val != null ? val.toString() : "null";
            }
            case DATE: {
                // Calcite stores dates as Integer (days since epoch)
                Object val = lit.getValue();
                if (val instanceof Integer) {
                    long epochDay = (Integer) val;
                    java.time.LocalDate date = java.time.LocalDate.ofEpochDay(epochDay);
                    return date.toString();
                }
                return val != null ? val.toString() : "null";
            }
            case TIMESTAMP: {
                Object val = lit.getValue();
                if (val instanceof Long) {
                    java.time.Instant instant = java.time.Instant.ofEpochMilli((Long) val);
                    return instant.toString();
                }
                return val != null ? "'" + val + "'" : "null";
            }
            default: {
                Object val = lit.getValue2();
                if (val == null) return "null";
                return "'" + val.toString().replace("'", "\\'") + "'";
            }
        }
    }
}
