/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.dataverse;

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
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Pushes simple predicates from a logical FilterRel into the OData $filter query parameter.
 *
 * <p>Supported: =, !=, &lt;, &gt;, &lt;=, &gt;=, IS NULL, IS NOT NULL, AND, OR.
 * OData operators: eq, ne, lt, gt, le, ge, and, or.
 *
 * <p>The original FilterRel is always kept as residual — Dremio re-applies it for correctness.
 */
public class DataverseFilterRule extends RelOptRule {

    public static final DataverseFilterRule INSTANCE = new DataverseFilterRule();

    private static final Logger logger = LoggerFactory.getLogger(DataverseFilterRule.class);

    private DataverseFilterRule() {
        super(RelOptHelper.some(FilterRel.class, Rel.LOGICAL,
                        RelOptHelper.any(DataverseScanDrel.class)),
                "DataverseFilterRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        FilterRel filter = call.rel(0);
        DataverseScanDrel scan = call.rel(1);

        RelDataType rowType = scan.getRowType();
        List<RexNode> conjuncts = RelOptUtil.conjunctions(filter.getCondition());

        List<String> pushed = new ArrayList<>();
        for (RexNode conjunct : conjuncts) {
            String pred = tryConvertToOData(conjunct, rowType);
            if (pred != null) pushed.add(pred);
        }

        if (pushed.isEmpty()) return;

        String odataFilter = String.join(" and ", pushed);
        DataverseScanSpec newSpec = scan.getScanSpec().withFilter(odataFilter);

        DataverseScanDrel newScan = new DataverseScanDrel(
                scan.getCluster(), scan.getTraitSet(), scan.getTable(), scan.getPluginId(),
                scan.getTableMetadata(), scan.getProjectedColumns(),
                scan.getCostAdjustmentFactor(), scan.getHints(), newSpec);

        RelNode residual = filter.copy(filter.getTraitSet(), List.of((RelNode) newScan));
        logger.debug("DataverseFilterRule: pushed {} conjuncts to OData $filter", pushed.size());
        call.transformTo(residual);
    }

    // -------------------------------------------------------------------------
    // OData predicate conversion
    // -------------------------------------------------------------------------

    private String tryConvertToOData(RexNode node, RelDataType rowType) {
        if (!(node instanceof RexCall)) return null;
        RexCall call = (RexCall) node;

        switch (call.getKind()) {
            case AND: {
                List<String> parts = new ArrayList<>();
                for (RexNode op : call.getOperands()) {
                    String part = tryConvertToOData(op, rowType);
                    if (part == null) return null;
                    parts.add(part);
                }
                return "(" + String.join(" and ", parts) + ")";
            }
            case OR: {
                List<String> parts = new ArrayList<>();
                for (RexNode op : call.getOperands()) {
                    String part = tryConvertToOData(op, rowType);
                    if (part == null) return null;
                    parts.add(part);
                }
                return "(" + String.join(" or ", parts) + ")";
            }
            case EQUALS:                return convertComparison(call, "eq", rowType);
            case NOT_EQUALS:            return convertComparison(call, "ne", rowType);
            case LESS_THAN:             return convertComparison(call, "lt", rowType);
            case GREATER_THAN:          return convertComparison(call, "gt", rowType);
            case LESS_THAN_OR_EQUAL:    return convertComparison(call, "le", rowType);
            case GREATER_THAN_OR_EQUAL: return convertComparison(call, "ge", rowType);
            case IS_NULL: {
                String col = columnName(call.getOperands().get(0), rowType);
                return col != null ? col + " eq null" : null;
            }
            case IS_NOT_NULL: {
                String col = columnName(call.getOperands().get(0), rowType);
                return col != null ? col + " ne null" : null;
            }
            default:
                return null;
        }
    }

    private String convertComparison(RexCall call, String op, RelDataType rowType) {
        if (call.getOperands().size() != 2) return null;
        String left = operandToOData(call.getOperands().get(0), rowType);
        String right = operandToOData(call.getOperands().get(1), rowType);
        if (left == null || right == null) return null;
        return left + " " + op + " " + right;
    }

    private String operandToOData(RexNode node, RelDataType rowType) {
        if (node instanceof RexInputRef) return columnName(node, rowType);
        if (node instanceof RexLiteral) return literalToOData((RexLiteral) node);
        return null;
    }

    private String columnName(RexNode node, RelDataType rowType) {
        if (!(node instanceof RexInputRef)) return null;
        int index = ((RexInputRef) node).getIndex();
        List<String> names = rowType.getFieldNames();
        if (index < 0 || index >= names.size()) return null;
        return names.get(index);
    }

    private String literalToOData(RexLiteral lit) {
        if (lit.isNull()) return "null";

        SqlTypeName typeName = lit.getType().getSqlTypeName();
        switch (typeName) {
            case CHAR:
            case VARCHAR: {
                Object val = lit.getValue();
                String str = (val instanceof NlsString) ? ((NlsString) val).getValue()
                        : (val != null ? val.toString() : null);
                if (str == null) return "null";
                // OData strings: single-quoted, escape single quotes by doubling them
                return "'" + str.replace("'", "''") + "'";
            }
            case BOOLEAN: {
                Object val = lit.getValue();
                return val != null ? val.toString().toLowerCase() : "null";
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
                Object val = lit.getValue();
                if (val instanceof Integer) {
                    java.time.LocalDate date = java.time.LocalDate.ofEpochDay((Integer) val);
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
                return val != null ? val.toString() : "null";
            }
            default: {
                Object val = lit.getValue2();
                if (val == null) return "null";
                return "'" + val.toString().replace("'", "''") + "'";
            }
        }
    }
}
