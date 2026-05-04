/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.pinecone;

import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Schema inference and type resolution for Pinecone vector data.
 *
 * <p>Fixed columns always present:
 * <ul>
 *   <li>{@code _id} (Utf8) — the vector ID</li>
 *   <li>{@code values} (Utf8) — JSON array of floats as string</li>
 * </ul>
 *
 * <p>Metadata fields are dynamically inferred from sampled vectors.
 * Type inference rules:
 * <ul>
 *   <li>All non-null values parse as Long → BigInt</li>
 *   <li>All non-null values parse as Double (not Long) → Float8</li>
 *   <li>Only "true" / "false" (case-insensitive) → Bool</li>
 *   <li>Otherwise → Utf8</li>
 * </ul>
 * Metadata fields are sorted alphabetically after the fixed columns.
 */
public final class PineconeTypeConverter {

    private static final Logger logger = LoggerFactory.getLogger(PineconeTypeConverter.class);

    private PineconeTypeConverter() {}

    // -------------------------------------------------------------------------
    // Column descriptor
    // -------------------------------------------------------------------------

    public static class PineconeColumn {
        public final String name;
        public final ArrowType arrowType;

        public PineconeColumn(String name, ArrowType arrowType) {
            this.name = name;
            this.arrowType = arrowType;
        }
    }

    // -------------------------------------------------------------------------
    // Schema inference
    // -------------------------------------------------------------------------

    /**
     * Infers a schema from a sample of vectors.
     * Returns fixed columns (_id, values) followed by sorted metadata fields.
     */
    public static List<PineconeColumn> inferColumns(List<JsonNode> sampleVectors) {
        // Collect all metadata field values across sampled vectors
        // field name → list of observed string values (nulls excluded)
        Map<String, List<String>> fieldValues = new TreeMap<>(); // TreeMap = sorted

        for (JsonNode vec : sampleVectors) {
            JsonNode metadata = vec.path("metadata");
            if (metadata.isObject()) {
                Iterator<Map.Entry<String, JsonNode>> fields = metadata.fields();
                while (fields.hasNext()) {
                    Map.Entry<String, JsonNode> entry = fields.next();
                    String fieldName = entry.getKey();
                    JsonNode val = entry.getValue();
                    if (!val.isNull() && !val.isMissingNode()) {
                        fieldValues.computeIfAbsent(fieldName, k -> new ArrayList<>())
                                   .add(nodeToString(val));
                    }
                }
            }
        }

        List<PineconeColumn> columns = new ArrayList<>();

        // Fixed columns first
        columns.add(new PineconeColumn("_id",    ArrowType.Utf8.INSTANCE));
        columns.add(new PineconeColumn("values", ArrowType.Utf8.INSTANCE));

        // Inferred metadata columns (sorted by field name due to TreeMap)
        for (Map.Entry<String, List<String>> entry : fieldValues.entrySet()) {
            ArrowType type = inferType(entry.getValue());
            columns.add(new PineconeColumn(entry.getKey(), type));
            logger.debug("Inferred column '{}' as {}", entry.getKey(), type);
        }

        return columns;
    }

    /**
     * Builds a BatchSchema from the inferred columns.
     */
    public static BatchSchema buildSchema(List<PineconeColumn> columns) {
        List<Field> fields = new ArrayList<>(columns.size());
        for (PineconeColumn col : columns) {
            fields.add(new Field(col.name, new FieldType(true, col.arrowType, null),
                    Collections.emptyList()));
        }
        return BatchSchema.newBuilder().addFields(fields).build();
    }

    // -------------------------------------------------------------------------
    // Value extraction
    // -------------------------------------------------------------------------

    /**
     * Extracts the raw string value for a metadata field from a vector JsonNode.
     * Returns null if the field is absent or null.
     */
    public static String extractMetadataValue(JsonNode vector, String fieldName) {
        JsonNode metadata = vector.path("metadata");
        if (metadata.isNull() || metadata.isMissingNode()) return null;
        JsonNode val = metadata.path(fieldName);
        if (val.isNull() || val.isMissingNode()) return null;
        return nodeToString(val);
    }

    /**
     * Extracts the values array as a JSON string, e.g. "[0.1,0.2,0.3]".
     */
    public static String extractValuesJson(JsonNode vector) {
        JsonNode values = vector.path("values");
        if (values.isNull() || values.isMissingNode()) return "[]";
        if (values.isArray()) return values.toString();
        return values.asText("[]");
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Infers the Arrow type from a list of string values observed for a field.
     */
    private static ArrowType inferType(List<String> values) {
        if (values == null || values.isEmpty()) return ArrowType.Utf8.INSTANCE;

        boolean canBeLong   = true;
        boolean canBeDouble = true;
        boolean canBeBool   = true;

        for (String v : values) {
            if (canBeLong) {
                try { Long.parseLong(v); }
                catch (NumberFormatException e) { canBeLong = false; }
            }
            if (canBeDouble) {
                try { Double.parseDouble(v); }
                catch (NumberFormatException e) { canBeDouble = false; }
            }
            if (canBeBool) {
                String lower = v.toLowerCase();
                if (!lower.equals("true") && !lower.equals("false")) {
                    canBeBool = false;
                }
            }
        }

        if (canBeLong)   return new ArrowType.Int(64, true);
        if (canBeDouble) return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
        if (canBeBool)   return ArrowType.Bool.INSTANCE;
        return ArrowType.Utf8.INSTANCE;
    }

    private static String nodeToString(JsonNode val) {
        if (val.isTextual())  return val.asText();
        if (val.isBoolean())  return String.valueOf(val.asBoolean());
        if (val.isNumber())   return val.asText();
        if (val.isArray())    return val.toString();
        if (val.isObject())   return val.toString();
        return val.asText();
    }
}
