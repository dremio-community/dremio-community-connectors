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
import org.apache.arrow.vector.TimeMilliVector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Reads records from Salesforce via SOQL, writing to Arrow vectors.
 *
 * <p>IMPORTANT: We do NOT call addField() in setup(). Dremio pre-allocates vectors before
 * calling setup(). Calling addField() on pre-existing fields causes a schema-change
 * false-positive. Instead we look up each vector by name via output.getVector().
 *
 * <p>Handles pagination (queryMore) transparently across next() calls.
 */
public class SalesforceRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceRecordReader.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final SalesforceConnection connection;
    private final SalesforceScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final SalesforceConf conf;

    // Projected fields and corresponding vectors (populated in setup)
    private List<Field> projectedFields;
    private List<ValueVector> vectors;

    // Pagination state
    private SalesforceConnection.SalesforceQueryResult currentPage;
    private int pageOffset;       // index within currentPage.records
    private boolean exhausted;    // true when all pages consumed
    private boolean firstCall;    // true before first query executed

    public SalesforceRecordReader(
            OperatorContext context,
            SalesforceConnection connection,
            SalesforceScanSpec spec,
            com.dremio.exec.record.BatchSchema schema,
            SalesforceConf conf) {
        super(context, null);
        this.connection = connection;
        this.spec = spec;
        this.schema = schema;
        this.conf = conf;
    }

    // -------------------------------------------------------------------------
    // AbstractRecordReader overrides
    // -------------------------------------------------------------------------

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        projectedFields = new ArrayList<>(schema.getFieldCount());
        vectors = new ArrayList<>(schema.getFieldCount());

        // Look up pre-allocated vectors — do NOT call addField()
        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue; // not projected (e.g. aggregation-only query)
            projectedFields.add(field);
            vectors.add(v);
        }

        exhausted = false;
        firstCall = true;
        pageOffset = 0;
        currentPage = null;

        logger.debug("SalesforceRecordReader setup: object={} split={}/{} soql={}",
                spec.getObjectName(), spec.getSplitIndex(), spec.getSplitCount(),
                spec.getSoqlQuery());
    }

    @Override
    public int next() {
        if (exhausted) {
            return 0;
        }

        try {
            // Fetch first page on first call
            if (firstCall) {
                firstCall = false;
                logger.debug("Executing SOQL: {}", spec.getSoqlQuery());
                currentPage = connection.query(spec.getSoqlQuery());
                pageOffset = 0;
            }

            // If we've consumed the current page, fetch the next one
            if (currentPage == null || pageOffset >= currentPage.records.size()) {
                if (currentPage == null || currentPage.done || currentPage.nextRecordsUrl == null) {
                    exhausted = true;
                    return 0;
                }
                logger.debug("Fetching next Salesforce page via queryMore");
                currentPage = connection.queryMore(currentPage.nextRecordsUrl);
                pageOffset = 0;
            }

            // Nothing on current page
            if (currentPage.records == null || currentPage.records.isEmpty()) {
                exhausted = true;
                return 0;
            }

            // Write up to recordsPerPage records
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

            // Set value count on all vectors
            for (ValueVector v : vectors) {
                v.setValueCount(count);
            }

            // Mark exhausted if this page is done and no more pages
            if (pageOffset >= currentPage.records.size() && currentPage.done) {
                exhausted = true;
            }

            return count;

        } catch (IOException e) {
            logger.error("Error reading from Salesforce", e);
            throw new RuntimeException("Failed to read from Salesforce: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        // Connection is shared / owned by the plugin; nothing to close here
        logger.debug("SalesforceRecordReader closed for {}", spec.getObjectName());
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeRecord(Map<String, Object> record, int idx) {
        for (int i = 0; i < projectedFields.size(); i++) {
            Field field = projectedFields.get(i);
            ValueVector vector = vectors.get(i);
            Object value = record.get(field.getName());

            if (value == null) {
                // Leave slot unset — Arrow default is null
                continue;
            }

            try {
                writeValue(vector, field, value, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' value '{}': {}",
                        field.getName(), value, e.getMessage());
                // Leave slot as null on error
            }
        }
    }

    private void writeValue(ValueVector vector, Field field, Object value, int idx) {
        ArrowType arrowType = field.getType();

        if (arrowType instanceof ArrowType.Utf8) {
            String str;
            if (value instanceof Map || value instanceof List) {
                // address / location / compound — JSON-stringify
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
            boolean b;
            if (value instanceof Boolean) {
                b = (Boolean) value;
            } else {
                b = Boolean.parseBoolean(value.toString());
            }
            ((BitVector) vector).setSafe(idx, b ? 1 : 0);

        } else if (arrowType instanceof ArrowType.Int) {
            ArrowType.Int intType = (ArrowType.Int) arrowType;
            if (intType.getBitWidth() == 32) {
                int intVal;
                if (value instanceof Number) {
                    intVal = ((Number) value).intValue();
                } else {
                    intVal = Integer.parseInt(value.toString());
                }
                ((IntVector) vector).setSafe(idx, intVal);
            } else {
                // 64-bit
                long longVal;
                if (value instanceof Number) {
                    longVal = ((Number) value).longValue();
                } else {
                    longVal = Long.parseLong(value.toString());
                }
                ((BigIntVector) vector).setSafe(idx, longVal);
            }

        } else if (arrowType instanceof ArrowType.FloatingPoint) {
            double dbl;
            if (value instanceof Number) {
                dbl = ((Number) value).doubleValue();
            } else {
                dbl = Double.parseDouble(value.toString());
            }
            ((Float8Vector) vector).setSafe(idx, dbl);

        } else if (arrowType instanceof ArrowType.Date) {
            // Salesforce date format: "2024-01-15"
            String dateStr = value.toString();
            LocalDate date = LocalDate.parse(dateStr, DateTimeFormatter.ISO_LOCAL_DATE);
            int epochDay = (int) date.toEpochDay();
            ((DateDayVector) vector).setSafe(idx, epochDay);

        } else if (arrowType instanceof ArrowType.Timestamp) {
            // Salesforce datetime format: "2024-01-15T10:30:00.000+0000" or "...Z"
            String dtStr = value.toString();
            // Normalize +0000 → Z for ISO-8601 parsing
            if (dtStr.endsWith("+0000")) {
                dtStr = dtStr.substring(0, dtStr.length() - 5) + "Z";
            }
            Instant instant;
            try {
                instant = Instant.parse(dtStr);
            } catch (Exception e) {
                // Try with offset pattern
                java.time.OffsetDateTime odt = java.time.OffsetDateTime.parse(dtStr);
                instant = odt.toInstant();
            }
            ((TimeStampMilliTZVector) vector).setSafe(idx, instant.toEpochMilli());

        } else if (arrowType instanceof ArrowType.Time) {
            // Salesforce time format: "10:30:00.000Z"
            String timeStr = value.toString();
            if (timeStr.endsWith("Z")) {
                timeStr = timeStr.substring(0, timeStr.length() - 1);
            }
            LocalTime lt;
            if (timeStr.contains(".")) {
                // May have variable fractional digits — normalize to .SSS
                int dotIdx = timeStr.lastIndexOf('.');
                String frac = timeStr.substring(dotIdx + 1);
                while (frac.length() < 3) frac += "0";
                frac = frac.substring(0, 3);
                timeStr = timeStr.substring(0, dotIdx + 1) + frac;
                lt = LocalTime.parse(timeStr, DateTimeFormatter.ofPattern("HH:mm:ss.SSS"));
            } else {
                lt = LocalTime.parse(timeStr, DateTimeFormatter.ISO_LOCAL_TIME);
            }
            int millis = (int) (lt.toNanoOfDay() / 1_000_000L);
            ((TimeMilliVector) vector).setSafe(idx, millis);

        } else if (arrowType instanceof ArrowType.Binary) {
            // base64-encoded binary
            byte[] bytes = Base64.getDecoder().decode(value.toString());
            ((VarBinaryVector) vector).setSafe(idx, bytes);

        } else {
            // Fallback: write as string
            ((VarCharVector) vector).setSafe(idx, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}
