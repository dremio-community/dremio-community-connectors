/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.influxdb;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.influxdb.InfluxDBSubScan.InfluxDBScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
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
 * Reads InfluxDB measurement rows into Arrow vectors.
 *
 * <p>Pages through results using SQL LIMIT/OFFSET. Each call to {@link #next()}
 * fetches one page and writes it into the output vectors.
 * Uses {@code mutator.getVector()} only — never {@code addField()}.
 */
public class InfluxDBRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBRecordReader.class);

    private final InfluxDBConnection connection;
    private final InfluxDBScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;

    private List<String> columnNames;
    private List<ValueVector> vectors;
    private List<ArrowType> columnTypes;

    private int currentOffset;
    private boolean exhausted;

    public InfluxDBRecordReader(OperatorContext context, InfluxDBConnection connection,
                                InfluxDBScanSpec spec, com.dremio.exec.record.BatchSchema schema) {
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

        currentOffset = 0;
        exhausted = false;
        logger.debug("InfluxDBRecordReader setup: measurement={} columns={}", spec.getMeasurement(), columnNames.size());
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        List<Map<String, Object>> rows;
        try {
            rows = connection.queryPage(spec.getMeasurement(), currentOffset);
        } catch (IOException e) {
            throw new RuntimeException("Failed to query InfluxDB measurement '"
                    + spec.getMeasurement() + "': " + e.getMessage(), e);
        }

        if (rows.isEmpty()) {
            exhausted = true;
            return 0;
        }

        int count = 0;
        for (Map<String, Object> row : rows) {
            writeRecord(row, count);
            count++;
        }

        for (ValueVector v : vectors) v.setValueCount(count);

        currentOffset += rows.size();
        if (rows.size() < connection.getPageSize()) {
            exhausted = true;
        }

        return count;
    }

    @Override
    public void close() throws Exception {
        logger.debug("InfluxDBRecordReader closed for measurement '{}'", spec.getMeasurement());
    }

    // ── Record writing ────────────────────────────────────────────────────────

    private void writeRecord(Map<String, Object> row, int idx) {
        for (int i = 0; i < columnNames.size(); i++) {
            Object value = row.get(columnNames.get(i));
            if (value == null) continue;
            try {
                writeValue(vectors.get(i), columnTypes.get(i), value, idx);
            } catch (Exception e) {
                logger.warn("Failed to write column '{}' value '{}': {}",
                        columnNames.get(i), value, e.getMessage());
            }
        }
    }

    private void writeValue(ValueVector vector, ArrowType type, Object value, int idx) {
        if (type instanceof ArrowType.Timestamp) {
            // InfluxDB returns timestamps as RFC3339 strings
            Instant instant = parseTimestamp(value.toString());
            if (instant != null) {
                ((TimeStampMilliVector) vector).setSafe(idx, instant.toEpochMilli());
            }

        } else if (type instanceof ArrowType.Int) {
            long v = (value instanceof Number)
                    ? ((Number) value).longValue()
                    : Long.parseLong(value.toString());
            ((BigIntVector) vector).setSafe(idx, v);

        } else if (type instanceof ArrowType.FloatingPoint) {
            double d = (value instanceof Number)
                    ? ((Number) value).doubleValue()
                    : Double.parseDouble(value.toString());
            ((Float8Vector) vector).setSafe(idx, d);

        } else if (type instanceof ArrowType.Bool) {
            boolean b = (value instanceof Boolean)
                    ? (Boolean) value
                    : Boolean.parseBoolean(value.toString());
            ((BitVector) vector).setSafe(idx, b ? 1 : 0);

        } else {
            // Utf8 — tags, string fields, and fallback
            ((VarCharVector) vector).setSafe(idx, value.toString().getBytes(StandardCharsets.UTF_8));
        }
    }

    private Instant parseTimestamp(String s) {
        try {
            return Instant.parse(s);
        } catch (Exception e) {
            try {
                return OffsetDateTime.parse(s).toInstant();
            } catch (Exception e2) {
                // Try parsing as epoch nanoseconds (InfluxDB may return integer nanos)
                try {
                    long nanos = Long.parseLong(s);
                    return Instant.ofEpochSecond(nanos / 1_000_000_000L, nanos % 1_000_000_000L);
                } catch (Exception e3) {
                    logger.warn("Cannot parse InfluxDB timestamp '{}': {}", s, e2.getMessage());
                    return null;
                }
            }
        }
    }
}
