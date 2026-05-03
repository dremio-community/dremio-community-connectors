/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.hubspot;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.hubspot.HubSpotSubScan.HubSpotScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class HubSpotRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(HubSpotRecordReader.class);

    private static final java.util.Set<String> BUILTIN_NAMES = new java.util.HashSet<>(
            java.util.Arrays.asList("id", "createdAt", "updatedAt", "archived"));

    private final HubSpotConnection connection;
    private final HubSpotScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final HubSpotConf conf;

    private List<Field>       projectedFields;
    private List<ValueVector> vectors;

    // Pagination state
    private HubSpotConnection.HubSpotPage currentPage;
    private int    pageOffset;
    private boolean exhausted;
    private boolean firstCall;

    public HubSpotRecordReader(
            OperatorContext context,
            HubSpotConnection connection,
            HubSpotScanSpec spec,
            com.dremio.exec.record.BatchSchema schema,
            HubSpotConf conf) {
        super(context, null);
        this.connection = connection;
        this.spec = spec;
        this.schema = schema;
        this.conf = conf;
    }

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        projectedFields = new ArrayList<>(schema.getFieldCount());
        vectors = new ArrayList<>(schema.getFieldCount());

        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue;
            projectedFields.add(field);
            vectors.add(v);
        }

        exhausted = false;
        firstCall = true;
        pageOffset = 0;
        currentPage = null;
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            if (firstCall) {
                firstCall = false;
                currentPage = fetchFirstPage();
                pageOffset = 0;
            }

            if (currentPage == null || currentPage.records == null || currentPage.records.isEmpty()) {
                exhausted = true;
                return 0;
            }

            if (pageOffset >= currentPage.records.size()) {
                if (currentPage.nextCursor == null) {
                    exhausted = true;
                    return 0;
                }
                currentPage = fetchNextPage(currentPage.nextCursor);
                pageOffset = 0;
                if (currentPage == null || currentPage.records == null || currentPage.records.isEmpty()) {
                    exhausted = true;
                    return 0;
                }
            }

            int batchSize = Math.min(conf.pageSize, currentPage.records.size() - pageOffset);
            if (batchSize <= 0) {
                exhausted = true;
                return 0;
            }

            int count = 0;
            for (int i = 0; i < batchSize; i++) {
                JsonNode record = currentPage.records.get(pageOffset + i);
                writeRecord(record, count);
                count++;
            }
            pageOffset += batchSize;

            for (ValueVector v : vectors) {
                v.setValueCount(count);
            }

            if (pageOffset >= currentPage.records.size() && currentPage.nextCursor == null) {
                exhausted = true;
            }

            return count;

        } catch (IOException e) {
            logger.error("Error reading from HubSpot", e);
            throw new RuntimeException("Failed to read from HubSpot: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("HubSpotRecordReader closed for {}", spec.getObjectType());
    }

    // -------------------------------------------------------------------------
    // Fetch helpers
    // -------------------------------------------------------------------------

    private HubSpotConnection.HubSpotPage fetchFirstPage() throws IOException {
        if (HubSpotConnection.OWNERS_OBJECT.equals(spec.getObjectType())) {
            return connection.fetchOwnersPage(null);
        }
        return connection.fetchPage(spec.getObjectType(), spec.getPropertyNames(), null);
    }

    private HubSpotConnection.HubSpotPage fetchNextPage(String cursor) throws IOException {
        if (HubSpotConnection.OWNERS_OBJECT.equals(spec.getObjectType())) {
            return connection.fetchOwnersPage(cursor);
        }
        return connection.fetchPage(spec.getObjectType(), spec.getPropertyNames(), cursor);
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeRecord(JsonNode record, int idx) {
        boolean isOwner = HubSpotConnection.OWNERS_OBJECT.equals(spec.getObjectType());
        JsonNode properties = isOwner ? null : record.get("properties");

        for (int i = 0; i < projectedFields.size(); i++) {
            Field field = projectedFields.get(i);
            ValueVector vector = vectors.get(i);
            String fieldName = field.getName();

            String rawValue = null;

            if (BUILTIN_NAMES.contains(fieldName) || isOwner) {
                // Top-level field
                JsonNode node = record.get(fieldName);
                if (node != null && !node.isNull()) {
                    rawValue = node.isTextual() ? node.asText() : node.toString();
                }
            } else if (properties != null) {
                // Dynamic property nested under "properties"
                JsonNode node = properties.get(fieldName);
                if (node != null && !node.isNull() && !node.asText("").isEmpty()) {
                    rawValue = node.asText();
                }
            }

            if (rawValue == null) continue;

            try {
                writeValue(vector, field, rawValue, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' value '{}': {}",
                        fieldName, rawValue, e.getMessage());
            }
        }
    }

    private void writeValue(ValueVector vector, Field field, String raw, int idx) {
        ArrowType type = field.getType();

        if (type instanceof ArrowType.Bool) {
            ((BitVector) vector).setSafe(idx, Boolean.parseBoolean(raw) ? 1 : 0);
        } else if (type instanceof ArrowType.FloatingPoint) {
            try {
                ((Float8Vector) vector).setSafe(idx, Double.parseDouble(raw));
            } catch (NumberFormatException e) {
                // leave unset
            }
        } else {
            ((VarCharVector) vector).setSafe(idx, raw.getBytes(StandardCharsets.UTF_8));
        }
    }
}
