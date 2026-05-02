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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateDayVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads records from Dataverse via OData v4, writing to Arrow vectors.
 *
 * <p>IMPORTANT: We do NOT call addField() in setup(). Dremio pre-allocates vectors before
 * calling setup(). Calling addField() on pre-existing fields causes a schema-change
 * false-positive. Instead we look up each vector by name via output.getVector().
 *
 * <p>Handles OData pagination (@odata.nextLink) transparently across next() calls.
 */
public class DataverseRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(DataverseRecordReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DataverseConnection connection;
    private final DataverseScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final DataverseConf conf;

    private List<Field> projectedFields;
    private List<ValueVector> vectors;

    private DataverseConnection.DataverseQueryResult currentPage;
    private int pageOffset;
    private boolean exhausted;
    private boolean firstCall;

    public DataverseRecordReader(
            OperatorContext context,
            DataverseConnection connection,
            DataverseScanSpec spec,
            com.dremio.exec.record.BatchSchema schema,
            DataverseConf conf) {
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

        logger.debug("DataverseRecordReader setup: entity={} url={}", spec.getEntityLogicalName(), spec.getQueryUrl());
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            if (firstCall) {
                firstCall = false;
                logger.debug("Querying Dataverse: {}", spec.getQueryUrl());
                currentPage = connection.query(spec.getQueryUrl());
                pageOffset = 0;
            }

            // Advance to next page if current is exhausted
            if (currentPage == null || pageOffset >= currentPage.records.size()) {
                if (currentPage == null || currentPage.nextLink == null) {
                    exhausted = true;
                    return 0;
                }
                logger.debug("Fetching next Dataverse page via @odata.nextLink");
                currentPage = connection.query(currentPage.nextLink);
                pageOffset = 0;
            }

            if (currentPage.records == null || currentPage.records.isEmpty()) {
                exhausted = true;
                return 0;
            }

            int batchSize = Math.min(conf.recordsPerPage, currentPage.records.size() - pageOffset);
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

            if (pageOffset >= currentPage.records.size() && currentPage.nextLink == null) {
                exhausted = true;
            }

            return count;

        } catch (IOException e) {
            logger.error("Error reading from Dataverse", e);
            throw new RuntimeException("Failed to read from Dataverse: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("DataverseRecordReader closed for {}", spec.getEntityLogicalName());
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeRecord(Map<String, Object> record, int idx) {
        for (int i = 0; i < projectedFields.size(); i++) {
            Field field = projectedFields.get(i);
            ValueVector vector = vectors.get(i);
            Object value = record.get(field.getName());
            if (value == null) continue;
            try {
                writeValue(vector, field, value, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' value '{}': {}", field.getName(), value, e.getMessage());
            }
        }
    }

    private void writeValue(ValueVector vector, Field field, Object value, int idx) {
        ArrowType arrowType = field.getType();

        if (arrowType instanceof ArrowType.Utf8) {
            String str;
            if (value instanceof Map || value instanceof List) {
                try {
                    str = MAPPER.writeValueAsString(value);
                } catch (Exception e) {
                    str = value.toString();
                }
            } else {
                str = value.toString();
            }
            ((VarCharVector) vector).setSafe(idx, str.getBytes(StandardCharsets.UTF_8));

        } else if (arrowType instanceof ArrowType.Bool) {
            boolean b = (value instanceof Boolean) ? (Boolean) value
                    : Boolean.parseBoolean(value.toString());
            ((BitVector) vector).setSafe(idx, b ? 1 : 0);

        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 32) {
                int intVal = (value instanceof Number) ? ((Number) value).intValue()
                        : Integer.parseInt(value.toString());
                ((IntVector) vector).setSafe(idx, intVal);
            } else {
                long longVal = (value instanceof Number) ? ((Number) value).longValue()
                        : Long.parseLong(value.toString());
                ((BigIntVector) vector).setSafe(idx, longVal);
            }

        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            double dbl = (value instanceof Number) ? ((Number) value).doubleValue()
                    : Double.parseDouble(value.toString());
            ((Float8Vector) vector).setSafe(idx, dbl);

        } else if (arrowType instanceof ArrowType.Date) {
            // Dataverse date format: "2024-01-15"
            String dateStr = value.toString();
            if (dateStr.contains("T")) {
                dateStr = dateStr.substring(0, dateStr.indexOf('T'));
            }
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            ((DateDayVector) vector).setSafe(idx, (int) date.toEpochDay());

        } else if (arrowType instanceof ArrowType.Timestamp) {
            // Dataverse datetime: ISO 8601, e.g. "2024-01-15T10:30:00Z"
            String dtStr = value.toString();
            Instant instant;
            try {
                instant = Instant.parse(dtStr);
            } catch (Exception e) {
                try {
                    java.time.OffsetDateTime odt = java.time.OffsetDateTime.parse(dtStr);
                    instant = odt.toInstant();
                } catch (Exception e2) {
                    logger.warn("Could not parse datetime '{}': {}", dtStr, e2.getMessage());
                    return;
                }
            }
            ((TimeStampMilliTZVector) vector).setSafe(idx, instant.toEpochMilli());

        } else {
            // Fallback: stringify
            ((VarCharVector) vector).setSafe(idx, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
