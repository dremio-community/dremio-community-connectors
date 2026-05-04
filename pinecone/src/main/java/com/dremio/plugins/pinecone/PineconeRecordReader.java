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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.pinecone.PineconeSubScan.PineconeScanSpec;
import com.dremio.plugins.pinecone.PineconeTypeConverter.PineconeColumn;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PineconeRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(PineconeRecordReader.class);

    private final PineconeConnection connection;
    private final PineconeScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final PineconeConf conf;

    private List<PineconeColumn> projectedColumns;
    private List<ValueVector>    vectors;

    // Pagination state
    private List<String> currentIds;
    private String       nextToken;
    private boolean      exhausted;
    private boolean      firstCall;

    public PineconeRecordReader(
            OperatorContext context,
            PineconeConnection connection,
            PineconeScanSpec spec,
            com.dremio.exec.record.BatchSchema schema,
            PineconeConf conf) {
        super(context, null);
        this.connection = connection;
        this.spec       = spec;
        this.schema     = schema;
        this.conf       = conf;
    }

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        // Re-infer the schema from samples to get PineconeColumn list
        List<PineconeColumn> allCols;
        try {
            List<JsonNode> sample = connection.sampleVectors(
                    spec.getHost(), conf.namespace, conf.sampleSize, spec.getTable());
            allCols = PineconeTypeConverter.inferColumns(sample);
        } catch (IOException e) {
            logger.warn("Schema inference failed during setup for '{}': {}",
                    spec.getTable(), e.getMessage());
            allCols = PineconeTypeConverter.inferColumns(new ArrayList<>());
        }

        projectedColumns = new ArrayList<>();
        vectors          = new ArrayList<>();

        for (PineconeColumn col : allCols) {
            ValueVector v = mutator.getVector(col.name);
            if (v == null) continue;
            projectedColumns.add(col);
            vectors.add(v);
        }

        exhausted  = false;
        firstCall  = true;
        currentIds = null;
        nextToken  = null;
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            // First call: fetch initial page of IDs
            if (firstCall) {
                firstCall  = false;
                currentIds = connection.listVectorIds(spec.getHost(), conf.namespace,
                        conf.pageSize, null, spec.getTable());
                nextToken  = connection.fetchNextToken(spec.getHost(), conf.namespace,
                        conf.pageSize, null, spec.getTable());

                if (currentIds == null || currentIds.isEmpty()) {
                    exhausted = true;
                    return 0;
                }
            }

            if (currentIds == null || currentIds.isEmpty()) {
                exhausted = true;
                return 0;
            }

            // Fetch full vector data for the current page of IDs
            Map<String, JsonNode> vectorMap = connection.fetchVectors(
                    spec.getHost(), currentIds, conf.namespace, spec.getTable());

            int count = 0;
            for (String id : currentIds) {
                JsonNode vec = vectorMap.get(id);
                if (vec == null) {
                    // Write just the _id with nulls for other fields
                    vec = new com.fasterxml.jackson.databind.node.ObjectNode(
                            com.fasterxml.jackson.databind.node.JsonNodeFactory.instance);
                    ((com.fasterxml.jackson.databind.node.ObjectNode) vec).put("id", id);
                }
                writeVector(id, vec, count);
                count++;
            }

            for (ValueVector v : vectors) {
                v.setValueCount(count);
            }

            // Advance to next page of IDs
            if (nextToken != null && !nextToken.isEmpty()) {
                currentIds = connection.listVectorIds(spec.getHost(), conf.namespace,
                        conf.pageSize, nextToken, spec.getTable());
                nextToken  = connection.fetchNextToken(spec.getHost(), conf.namespace,
                        conf.pageSize, nextToken, spec.getTable());
                if (currentIds == null || currentIds.isEmpty()) {
                    exhausted = true;
                }
            } else {
                exhausted  = true;
                currentIds = null;
            }

            return count;

        } catch (IOException e) {
            logger.error("Error reading from Pinecone index '{}'", spec.getTable(), e);
            throw new RuntimeException("Failed to read from Pinecone: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("PineconeRecordReader closed for index '{}'", spec.getTable());
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeVector(String id, JsonNode vec, int idx) {
        for (int i = 0; i < projectedColumns.size(); i++) {
            PineconeColumn col    = projectedColumns.get(i);
            ValueVector    vector = vectors.get(i);

            try {
                String rawValue;
                if ("_id".equals(col.name)) {
                    rawValue = id;
                } else if ("values".equals(col.name)) {
                    rawValue = PineconeTypeConverter.extractValuesJson(vec);
                } else {
                    rawValue = PineconeTypeConverter.extractMetadataValue(vec, col.name);
                }

                if (rawValue == null || rawValue.isEmpty()) continue;
                writeValue(vector, col, rawValue, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' for vector '{}': {}",
                        col.name, id, e.getMessage());
            }
        }
    }

    private void writeValue(ValueVector vector, PineconeColumn col, String raw, int idx) {
        ArrowType type = col.arrowType;

        if (type instanceof ArrowType.Bool) {
            ((BitVector) vector).setSafe(idx, Boolean.parseBoolean(raw) ? 1 : 0);
        } else if (type instanceof ArrowType.Int) {
            try {
                ((BigIntVector) vector).setSafe(idx, Long.parseLong(raw));
            } catch (NumberFormatException e) {
                // leave unset for null/non-numeric values
            }
        } else if (type instanceof ArrowType.FloatingPoint) {
            try {
                ((Float8Vector) vector).setSafe(idx, Double.parseDouble(raw));
            } catch (NumberFormatException e) {
                // leave unset
            }
        } else {
            // Utf8
            ((VarCharVector) vector).setSafe(idx, raw.getBytes(StandardCharsets.UTF_8));
        }
    }

}
