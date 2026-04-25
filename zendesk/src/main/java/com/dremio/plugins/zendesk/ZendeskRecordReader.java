/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.zendesk;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.zendesk.ZendeskConnection.ZendeskField;
import com.dremio.plugins.zendesk.ZendeskConnection.ZendeskPage;
import com.dremio.plugins.zendesk.ZendeskConnection.ZendeskTable;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
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
 * Reads records from Zendesk REST API pages into Arrow vectors.
 *
 * <p>IMPORTANT: Do NOT call addField() in setup(). Look up pre-allocated vectors
 * via mutator.getVector() only — same pattern as the Salesforce connector.
 *
 * <p>Handles cursor-based pagination transparently across next() calls.
 * Supports dot-notation field paths for nested JSON extraction (e.g. "via.channel").
 */
public class ZendeskRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(ZendeskRecordReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int BATCH_SIZE = 100;

    private final ZendeskConnection connection;
    private final ZendeskScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final ZendeskTable table;

    // Projected fields + vectors (populated in setup)
    private List<ZendeskField> projectedZdFields;
    private List<ValueVector> vectors;

    // Pagination state
    private ZendeskPage currentPage;
    private int pageOffset;
    private boolean exhausted;
    private boolean firstCall;
    private String nextUrl;

    public ZendeskRecordReader(
            OperatorContext context,
            ZendeskConnection connection,
            ZendeskScanSpec spec,
            com.dremio.exec.record.BatchSchema schema) {
        super(context, null);
        this.connection = connection;
        this.spec = spec;
        this.schema = schema;
        this.table = ZendeskConnection.getTable(spec.getTableName());
    }

    // -------------------------------------------------------------------------
    // AbstractRecordReader overrides
    // -------------------------------------------------------------------------

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        projectedZdFields = new ArrayList<>();
        vectors = new ArrayList<>();

        // Build a name→ZendeskField map for fast lookup
        Map<String, ZendeskField> zdFieldByName = new java.util.LinkedHashMap<>();
        for (ZendeskField zf : table.fields) {
            zdFieldByName.put(zf.name, zf);
        }

        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue;
            ZendeskField zf = zdFieldByName.get(field.getName());
            if (zf == null) continue;
            projectedZdFields.add(zf);
            vectors.add(v);
        }

        exhausted = false;
        firstCall = true;
        pageOffset = 0;
        currentPage = null;
        nextUrl = table.endpoint;

        logger.debug("ZendeskRecordReader setup: table={}", spec.getTableName());
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            if (firstCall) {
                firstCall = false;
                logger.debug("Fetching first page: {}", nextUrl);
                currentPage = connection.fetchPage(nextUrl);
                pageOffset = 0;
                nextUrl = currentPage.nextUrl;
            }

            // Advance to next page if current is consumed
            if (currentPage == null || pageOffset >= currentPage.records.size()) {
                if (!currentPage.hasMore || nextUrl == null) {
                    exhausted = true;
                    return 0;
                }
                logger.debug("Fetching next page: {}", nextUrl);
                currentPage = connection.fetchPage(nextUrl);
                pageOffset = 0;
                nextUrl = currentPage.nextUrl;
            }

            if (currentPage.records == null || currentPage.records.isEmpty()) {
                exhausted = true;
                return 0;
            }

            int batchSize = Math.min(BATCH_SIZE, currentPage.records.size() - pageOffset);
            if (batchSize <= 0) {
                exhausted = true;
                return 0;
            }

            int count = 0;
            for (int i = 0; i < batchSize; i++) {
                Map<String, Object> record = currentPage.records.get(pageOffset + i);
                writeRecord(record, count);
                count++;
            }
            pageOffset += batchSize;

            for (ValueVector v : vectors) {
                v.setValueCount(count);
            }

            if (pageOffset >= currentPage.records.size() && !currentPage.hasMore) {
                exhausted = true;
            }

            return count;

        } catch (IOException e) {
            logger.error("Error reading from Zendesk", e);
            throw new RuntimeException("Failed to read from Zendesk: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("ZendeskRecordReader closed for table '{}'", spec.getTableName());
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeRecord(Map<String, Object> record, int idx) {
        for (int i = 0; i < projectedZdFields.size(); i++) {
            ZendeskField zf = projectedZdFields.get(i);
            ValueVector vector = vectors.get(i);
            Object value = extractValue(record, zf.jsonPath);
            if (value == null) continue;
            try {
                writeValue(vector, zf.type, value, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' value '{}': {}", zf.name, value, e.getMessage());
            }
        }
    }

    /**
     * Extracts a value from a record using a dot-notation path (e.g. "via.channel"
     * navigates record["via"]["channel"]).
     */
    @SuppressWarnings("unchecked")
    private Object extractValue(Map<String, Object> record, String path) {
        String[] parts = path.split("\\.", 2);
        Object val = record.get(parts[0]);
        if (val == null || parts.length == 1) return val;
        if (val instanceof Map) {
            return extractValue((Map<String, Object>) val, parts[1]);
        }
        return null;
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
            int bitWidth = ((ArrowType.Int) type).getBitWidth();
            if (bitWidth == 32) {
                int v = (value instanceof Number) ? ((Number) value).intValue() : Integer.parseInt(value.toString());
                ((IntVector) vector).setSafe(idx, v);
            } else {
                long v = (value instanceof Number) ? ((Number) value).longValue() : Long.parseLong(value.toString());
                ((BigIntVector) vector).setSafe(idx, v);
            }

        } else if (type instanceof ArrowType.FloatingPoint) {
            double d = (value instanceof Number) ? ((Number) value).doubleValue() : Double.parseDouble(value.toString());
            ((Float8Vector) vector).setSafe(idx, d);

        } else if (type instanceof ArrowType.Timestamp) {
            // Zendesk timestamps: ISO-8601 with timezone, e.g. "2024-01-15T10:30:00Z"
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
            // Fallback: write as string
            ((VarCharVector) vector).setSafe(idx, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
