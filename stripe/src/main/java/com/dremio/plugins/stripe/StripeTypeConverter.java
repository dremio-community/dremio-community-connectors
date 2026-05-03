/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.stripe;

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-table schema definitions and JSON → Arrow value extraction for Stripe.
 *
 * <p>Each table has a fixed schema (Stripe's API is stable and versioned).
 * Nested fields (e.g. recurring.interval in prices) are exposed as flat columns
 * using underscore-joined names (e.g. recurring_interval).
 */
public final class StripeTypeConverter {

    private StripeTypeConverter() {}

    // -------------------------------------------------------------------------
    // Column descriptors
    // -------------------------------------------------------------------------

    /** Describes one column: Arrow type + how to extract its value from a Stripe JSON record. */
    public static class StripeColumn {
        public final String name;
        public final ArrowType arrowType;
        /** Dot-path into the JSON record, e.g. "recurring.interval". */
        public final String jsonPath;

        public StripeColumn(String name, ArrowType arrowType, String jsonPath) {
            this.name = name;
            this.arrowType = arrowType;
            this.jsonPath = jsonPath;
        }

        public StripeColumn(String name, ArrowType arrowType) {
            this(name, arrowType, name);
        }
    }

    // -------------------------------------------------------------------------
    // Per-table column lists
    // -------------------------------------------------------------------------

    private static final List<StripeColumn> CHARGES_COLS = Arrays.asList(
            new StripeColumn("id",               ArrowType.Utf8.INSTANCE),
            new StripeColumn("amount",            new ArrowType.Int(64, true)),
            new StripeColumn("amount_captured",   new ArrowType.Int(64, true)),
            new StripeColumn("amount_refunded",   new ArrowType.Int(64, true)),
            new StripeColumn("currency",          ArrowType.Utf8.INSTANCE),
            new StripeColumn("customer",          ArrowType.Utf8.INSTANCE),
            new StripeColumn("description",       ArrowType.Utf8.INSTANCE),
            new StripeColumn("receipt_email",     ArrowType.Utf8.INSTANCE),
            new StripeColumn("status",            ArrowType.Utf8.INSTANCE),
            new StripeColumn("paid",              ArrowType.Bool.INSTANCE),
            new StripeColumn("refunded",          ArrowType.Bool.INSTANCE),
            new StripeColumn("created",           new ArrowType.Int(64, true)),
            new StripeColumn("payment_method_type", ArrowType.Utf8.INSTANCE,
                    "payment_method_details.type")
    );

    private static final List<StripeColumn> CUSTOMERS_COLS = Arrays.asList(
            new StripeColumn("id",          ArrowType.Utf8.INSTANCE),
            new StripeColumn("email",       ArrowType.Utf8.INSTANCE),
            new StripeColumn("name",        ArrowType.Utf8.INSTANCE),
            new StripeColumn("phone",       ArrowType.Utf8.INSTANCE),
            new StripeColumn("description", ArrowType.Utf8.INSTANCE),
            new StripeColumn("currency",    ArrowType.Utf8.INSTANCE),
            new StripeColumn("balance",     new ArrowType.Int(64, true)),
            new StripeColumn("delinquent",  ArrowType.Bool.INSTANCE),
            new StripeColumn("created",     new ArrowType.Int(64, true))
    );

    private static final List<StripeColumn> SUBSCRIPTIONS_COLS = Arrays.asList(
            new StripeColumn("id",                   ArrowType.Utf8.INSTANCE),
            new StripeColumn("customer",             ArrowType.Utf8.INSTANCE),
            new StripeColumn("status",               ArrowType.Utf8.INSTANCE),
            new StripeColumn("current_period_start", new ArrowType.Int(64, true)),
            new StripeColumn("current_period_end",   new ArrowType.Int(64, true)),
            new StripeColumn("created",              new ArrowType.Int(64, true)),
            new StripeColumn("canceled_at",          new ArrowType.Int(64, true)),
            new StripeColumn("trial_end",            new ArrowType.Int(64, true)),
            new StripeColumn("cancel_at_period_end", ArrowType.Bool.INSTANCE)
    );

    private static final List<StripeColumn> INVOICES_COLS = Arrays.asList(
            new StripeColumn("id",               ArrowType.Utf8.INSTANCE),
            new StripeColumn("customer",         ArrowType.Utf8.INSTANCE),
            new StripeColumn("subscription",     ArrowType.Utf8.INSTANCE),
            new StripeColumn("status",           ArrowType.Utf8.INSTANCE),
            new StripeColumn("currency",         ArrowType.Utf8.INSTANCE),
            new StripeColumn("amount_due",       new ArrowType.Int(64, true)),
            new StripeColumn("amount_paid",      new ArrowType.Int(64, true)),
            new StripeColumn("amount_remaining", new ArrowType.Int(64, true)),
            new StripeColumn("paid",             ArrowType.Bool.INSTANCE),
            new StripeColumn("created",          new ArrowType.Int(64, true)),
            new StripeColumn("due_date",         new ArrowType.Int(64, true))
    );

    private static final List<StripeColumn> PAYMENT_INTENTS_COLS = Arrays.asList(
            new StripeColumn("id",                   ArrowType.Utf8.INSTANCE),
            new StripeColumn("amount",               new ArrowType.Int(64, true)),
            new StripeColumn("amount_received",      new ArrowType.Int(64, true)),
            new StripeColumn("currency",             ArrowType.Utf8.INSTANCE),
            new StripeColumn("customer",             ArrowType.Utf8.INSTANCE),
            new StripeColumn("description",          ArrowType.Utf8.INSTANCE),
            new StripeColumn("status",               ArrowType.Utf8.INSTANCE),
            new StripeColumn("created",              new ArrowType.Int(64, true)),
            new StripeColumn("payment_method_types", ArrowType.Utf8.INSTANCE)
    );

    private static final List<StripeColumn> PRODUCTS_COLS = Arrays.asList(
            new StripeColumn("id",          ArrowType.Utf8.INSTANCE),
            new StripeColumn("name",        ArrowType.Utf8.INSTANCE),
            new StripeColumn("description", ArrowType.Utf8.INSTANCE),
            new StripeColumn("type",        ArrowType.Utf8.INSTANCE),
            new StripeColumn("active",      ArrowType.Bool.INSTANCE),
            new StripeColumn("created",     new ArrowType.Int(64, true)),
            new StripeColumn("updated",     new ArrowType.Int(64, true))
    );

    private static final List<StripeColumn> PRICES_COLS = Arrays.asList(
            new StripeColumn("id",                     ArrowType.Utf8.INSTANCE),
            new StripeColumn("product",                ArrowType.Utf8.INSTANCE),
            new StripeColumn("currency",               ArrowType.Utf8.INSTANCE),
            new StripeColumn("unit_amount",            new ArrowType.Int(64, true)),
            new StripeColumn("type",                   ArrowType.Utf8.INSTANCE),
            new StripeColumn("active",                 ArrowType.Bool.INSTANCE),
            new StripeColumn("created",                new ArrowType.Int(64, true)),
            new StripeColumn("recurring_interval",     ArrowType.Utf8.INSTANCE, "recurring.interval"),
            new StripeColumn("recurring_interval_count", new ArrowType.Int(64, true), "recurring.interval_count")
    );

    private static final List<StripeColumn> REFUNDS_COLS = Arrays.asList(
            new StripeColumn("id",             ArrowType.Utf8.INSTANCE),
            new StripeColumn("amount",         new ArrowType.Int(64, true)),
            new StripeColumn("currency",       ArrowType.Utf8.INSTANCE),
            new StripeColumn("charge",         ArrowType.Utf8.INSTANCE),
            new StripeColumn("payment_intent", ArrowType.Utf8.INSTANCE),
            new StripeColumn("status",         ArrowType.Utf8.INSTANCE),
            new StripeColumn("reason",         ArrowType.Utf8.INSTANCE),
            new StripeColumn("created",        new ArrowType.Int(64, true))
    );

    private static final List<StripeColumn> BALANCE_TRANSACTIONS_COLS = Arrays.asList(
            new StripeColumn("id",          ArrowType.Utf8.INSTANCE),
            new StripeColumn("amount",      new ArrowType.Int(64, true)),
            new StripeColumn("currency",    ArrowType.Utf8.INSTANCE),
            new StripeColumn("fee",         new ArrowType.Int(64, true)),
            new StripeColumn("net",         new ArrowType.Int(64, true)),
            new StripeColumn("type",        ArrowType.Utf8.INSTANCE),
            new StripeColumn("status",      ArrowType.Utf8.INSTANCE),
            new StripeColumn("description", ArrowType.Utf8.INSTANCE),
            new StripeColumn("source",      ArrowType.Utf8.INSTANCE),
            new StripeColumn("created",     new ArrowType.Int(64, true))
    );

    private static final Map<String, List<StripeColumn>> TABLE_COLUMNS;
    static {
        Map<String, List<StripeColumn>> m = new LinkedHashMap<>();
        m.put("charges",              CHARGES_COLS);
        m.put("customers",            CUSTOMERS_COLS);
        m.put("subscriptions",        SUBSCRIPTIONS_COLS);
        m.put("invoices",             INVOICES_COLS);
        m.put("payment_intents",      PAYMENT_INTENTS_COLS);
        m.put("products",             PRODUCTS_COLS);
        m.put("prices",               PRICES_COLS);
        m.put("refunds",              REFUNDS_COLS);
        m.put("balance_transactions", BALANCE_TRANSACTIONS_COLS);
        TABLE_COLUMNS = Collections.unmodifiableMap(m);
    }

    // -------------------------------------------------------------------------
    // Public API
    // -------------------------------------------------------------------------

    public static List<StripeColumn> columnsFor(String table) {
        return TABLE_COLUMNS.getOrDefault(table, Collections.emptyList());
    }

    public static BatchSchema schemaFor(String table) {
        List<StripeColumn> cols = columnsFor(table);
        List<Field> fields = new ArrayList<>(cols.size());
        for (StripeColumn col : cols) {
            fields.add(new Field(col.name, new FieldType(true, col.arrowType, null),
                    Collections.emptyList()));
        }
        return BatchSchema.newBuilder().addFields(fields).build();
    }

    /**
     * Extracts the value for a column from a Stripe JSON record.
     * Handles dot-path traversal (e.g. "recurring.interval") and array-to-string
     * (e.g. "payment_method_types" → "card,sepa_debit").
     */
    public static String extractValue(JsonNode record, StripeColumn col) {
        JsonNode node = traversePath(record, col.jsonPath);
        if (node == null || node.isNull() || node.isMissingNode()) return null;

        if (node.isArray()) {
            // Join array elements as comma-separated string
            List<String> parts = new ArrayList<>();
            for (JsonNode elem : node) {
                parts.add(elem.isTextual() ? elem.asText() : elem.toString());
            }
            return String.join(",", parts);
        }
        if (node.isTextual())  return node.asText();
        if (node.isBoolean())  return String.valueOf(node.asBoolean());
        if (node.isNumber())   return node.asText();
        if (node.isObject())   return node.toString();
        return node.asText();
    }

    private static JsonNode traversePath(JsonNode root, String dotPath) {
        String[] parts = dotPath.split("\\.");
        JsonNode current = root;
        for (String part : parts) {
            if (current == null || current.isNull() || current.isMissingNode()) return null;
            current = current.get(part);
        }
        return current;
    }
}
