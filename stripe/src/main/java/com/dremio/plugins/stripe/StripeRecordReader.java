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

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.stripe.StripeSubScan.StripeScanSpec;
import com.dremio.plugins.stripe.StripeTypeConverter.StripeColumn;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
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

public class StripeRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(StripeRecordReader.class);

    private final StripeConnection connection;
    private final StripeScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final StripeConf conf;

    private List<StripeColumn> projectedColumns;
    private List<ValueVector>  vectors;

    // Pagination state
    private StripeConnection.StripePage currentPage;
    private int     pageOffset;
    private boolean exhausted;
    private boolean firstCall;

    public StripeRecordReader(
            OperatorContext context,
            StripeConnection connection,
            StripeScanSpec spec,
            com.dremio.exec.record.BatchSchema schema,
            StripeConf conf) {
        super(context, null);
        this.connection = connection;
        this.spec       = spec;
        this.schema     = schema;
        this.conf       = conf;
    }

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        List<StripeColumn> allCols = StripeTypeConverter.columnsFor(spec.getTable());

        projectedColumns = new ArrayList<>();
        vectors          = new ArrayList<>();

        for (StripeColumn col : allCols) {
            ValueVector v = mutator.getVector(col.name);
            if (v == null) continue;
            projectedColumns.add(col);
            vectors.add(v);
        }

        exhausted  = false;
        firstCall  = true;
        pageOffset = 0;
        currentPage = null;
    }

    @Override
    public int next() {
        if (exhausted) return 0;

        try {
            if (firstCall) {
                firstCall = false;
                currentPage = connection.fetchPage(spec.getTable(), null);
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
                currentPage = connection.fetchPage(spec.getTable(), currentPage.nextCursor);
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
            logger.error("Error reading from Stripe table '{}'", spec.getTable(), e);
            throw new RuntimeException("Failed to read from Stripe: " + e.getMessage(), e);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("StripeRecordReader closed for table '{}'", spec.getTable());
    }

    // -------------------------------------------------------------------------
    // Record writing
    // -------------------------------------------------------------------------

    private void writeRecord(JsonNode record, int idx) {
        for (int i = 0; i < projectedColumns.size(); i++) {
            StripeColumn col    = projectedColumns.get(i);
            ValueVector  vector = vectors.get(i);

            String rawValue = StripeTypeConverter.extractValue(record, col);
            if (rawValue == null || rawValue.isEmpty()) continue;

            try {
                writeValue(vector, col, rawValue, idx);
            } catch (Exception e) {
                logger.warn("Failed to write field '{}' value '{}': {}",
                        col.name, rawValue, e.getMessage());
            }
        }
    }

    private void writeValue(ValueVector vector, StripeColumn col, String raw, int idx) {
        ArrowType type = col.arrowType;

        if (type instanceof ArrowType.Bool) {
            ((BitVector) vector).setSafe(idx, Boolean.parseBoolean(raw) ? 1 : 0);
        } else if (type instanceof ArrowType.Int) {
            try {
                ((BigIntVector) vector).setSafe(idx, Long.parseLong(raw));
            } catch (NumberFormatException e) {
                // leave unset for null/non-numeric values
            }
        } else {
            // Utf8
            ((VarCharVector) vector).setSafe(idx, raw.getBytes(StandardCharsets.UTF_8));
        }
    }
}
