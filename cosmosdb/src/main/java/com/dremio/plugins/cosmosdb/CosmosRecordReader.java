/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.cosmosdb;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.cosmosdb.CosmosConnection.CosmosPage;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads Cosmos DB documents into Arrow vectors.
 *
 * <p>Fetches pages via continuation tokens. Flattened field names
 * (e.g. "contact_email") are resolved by navigating nested maps.
 * Uses mutator.getVector() only — never addField().
 */
public class CosmosRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(CosmosRecordReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int BATCH_SIZE = 100;

    private final CosmosConnection connection;
    private final CosmosScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;

    // Projected columns
    private List<String> columnNames;
    private List<ValueVector> vectors;
    private List<ArrowType> columnTypes;

    // Pagination state
    private CosmosPage currentPage;
    private int pageOffset;
    private String continuationToken;
    private boolean exhausted;
    private boolean firstCall;

    public CosmosRecordReader(OperatorContext context, CosmosConnection connection,
                               CosmosScanSpec spec, com.dremio.exec.record.BatchSchema schema) {
        super(context, null);
        this.connection = connection;
        this.spec = spec;
        this.schema = schema;
    }

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        columnNames = new ArrayList<>();
        vectors = new ArrayList<>();
        columnTypes = new ArrayList<>();

        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue;
            columnNames.add(field.getName());
            vectors.add(v);
            columnTypes.add(field.getType());
        }

        exhausted = false;
        firstCall = true;
        pageOffset = 0;
        continuationToken = null;
        currentPage = null;

        logger.debug("CosmosRecordReader setup: container={} columns={}", spec.getContainerName(), columnNames.size());
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            // Fetch first page or advance when current page is fully consumed
            if (firstCall) {
                firstCall = false;
                currentPage = connection.queryPage(spec.getContainerName(), spec.getSql(), null);
                continuationToken = currentPage.continuationToken;
                pageOffset = 0;
            } else if (pageOffset >= currentPage.docs.size()) {
                if (!currentPage.hasMore()) {
                    exhausted = true;
                    return 0;
                }
                currentPage = connection.queryPage(spec.getContainerName(), spec.getSql(), continuationToken);
                continuationToken = currentPage.continuationToken;
                pageOffset = 0;
            }

            if (currentPage.docs.isEmpty()) {
                exhausted = true;
                return 0;
            }

            int batchSize = Math.min(BATCH_SIZE, currentPage.docs.size() - pageOffset);
            if (batchSize <= 0) {
                exhausted = true;
                return 0;
            }

            int count = 0;
            for (int i = 0; i < batchSize; i++) {
                Map<String, Object> flat = flattenDoc(currentPage.docs.get(pageOffset + i));
                writeRecord(flat, count);
                count++;
            }
            pageOffset += batchSize;

            for (ValueVector v : vectors) v.setValueCount(count);

            if (pageOffset >= currentPage.docs.size() && !currentPage.hasMore()) {
                exhausted = true;
            }
            return count;

        } catch (IOException e) {
            throw new RuntimeException("Failed to read from Cosmos DB: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("CosmosRecordReader closed for container '{}'", spec.getContainerName());
    }

    // ── Record writing ────────────────────────────────────────────────────────

    private void writeRecord(Map<String, Object> flat, int idx) {
        for (int i = 0; i < columnNames.size(); i++) {
            String col = columnNames.get(i);
            ValueVector vector = vectors.get(i);
            Object value = flat.get(col);
            if (value == null) continue;
            try {
                writeValue(vector, columnTypes.get(i), value, idx);
            } catch (Exception e) {
                logger.warn("Failed to write column '{}' value '{}': {}", col, value, e.getMessage());
            }
        }
    }

    /**
     * Flattens a Cosmos document one level deep.
     * {"contact": {"email": "a@b.com"}} → {"contact_email": "a@b.com"}
     * Arrays → serialized as JSON string under the original key.
     */
    @SuppressWarnings("unchecked")
    private Map<String, Object> flattenDoc(Map<String, Object> doc) {
        java.util.LinkedHashMap<String, Object> flat = new java.util.LinkedHashMap<>();
        for (Map.Entry<String, Object> e : doc.entrySet()) {
            String key = e.getKey();
            Object val = e.getValue();
            if (val instanceof Map) {
                Map<String, Object> nested = (Map<String, Object>) val;
                for (Map.Entry<String, Object> ne : nested.entrySet()) {
                    String nestedKey = key + "_" + ne.getKey();
                    Object nestedVal = ne.getValue();
                    if (nestedVal instanceof Map || nestedVal instanceof List) {
                        try {
                            flat.put(nestedKey, MAPPER.writeValueAsString(nestedVal));
                        } catch (Exception ex) {
                            flat.put(nestedKey, nestedVal.toString());
                        }
                    } else {
                        flat.put(nestedKey, nestedVal);
                    }
                }
            } else if (val instanceof List) {
                try {
                    flat.put(key, MAPPER.writeValueAsString(val));
                } catch (Exception ex) {
                    flat.put(key, val.toString());
                }
            } else {
                flat.put(key, val);
            }
        }
        return flat;
    }

    @SuppressWarnings("unchecked")
    private void writeValue(ValueVector vector, ArrowType type, Object value, int idx) {
        if (type instanceof ArrowType.Utf8) {
            String str;
            if (value instanceof List || value instanceof Map) {
                try { str = MAPPER.writeValueAsString(value); } catch (Exception e) { str = value.toString(); }
            } else {
                str = value.toString();
            }
            ((VarCharVector) vector).setSafe(idx, str.getBytes(StandardCharsets.UTF_8));

        } else if (type instanceof ArrowType.Bool) {
            boolean b = (value instanceof Boolean) ? (Boolean) value : Boolean.parseBoolean(value.toString());
            ((BitVector) vector).setSafe(idx, b ? 1 : 0);

        } else if (type instanceof ArrowType.Int) {
            long v = (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
            ((BigIntVector) vector).setSafe(idx, v);

        } else if (type instanceof ArrowType.FloatingPoint) {
            double d = (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
            ((Float8Vector) vector).setSafe(idx, d);

        } else if (type instanceof ArrowType.Timestamp) {
            String s = value.toString();
            Instant instant;
            try {
                instant = Instant.parse(s);
            } catch (Exception e) {
                try {
                    instant = OffsetDateTime.parse(s).toInstant();
                } catch (Exception e2) {
                    logger.warn("Cannot parse timestamp '{}': {}", s, e2.getMessage());
                    return;
                }
            }
            ((TimeStampMilliVector) vector).setSafe(idx, instant.toEpochMilli());

        } else {
            ((VarCharVector) vector).setSafe(idx, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
