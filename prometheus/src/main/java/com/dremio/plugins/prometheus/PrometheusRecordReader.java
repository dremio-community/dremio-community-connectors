/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.prometheus;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.prometheus.PrometheusSubScan.PrometheusScanSpec;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class PrometheusRecordReader extends AbstractRecordReader {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusRecordReader.class);

    private final PrometheusConnection connection;
    private final PrometheusScanSpec spec;
    private final com.dremio.exec.record.BatchSchema schema;
    private final PrometheusConf conf;

    private List<Field> projectedFields;
    private List<ValueVector> vectors;

    private List<JsonNode> allRecords;
    private int allRecordsIdx;
    private boolean exhausted;

    public PrometheusRecordReader(OperatorContext context, PrometheusConnection connection,
            PrometheusScanSpec spec, com.dremio.exec.record.BatchSchema schema, PrometheusConf conf) {
        super(context, null);
        this.connection = connection;
        this.spec       = spec;
        this.schema     = schema;
        this.conf       = conf;
    }

    @Override
    public void setup(OutputMutator mutator) throws ExecutionSetupException {
        projectedFields = new ArrayList<>(schema.getFieldCount());
        vectors         = new ArrayList<>(schema.getFieldCount());
        for (Field field : schema.getFields()) {
            ValueVector v = mutator.getVector(field.getName());
            if (v == null) continue;
            projectedFields.add(field);
            vectors.add(v);
        }
        exhausted      = false;
        allRecords     = null;
        allRecordsIdx  = 0;
    }

    @Override
    public int next() {
        if (exhausted) return 0;
        try {
            if (allRecords == null) {
                allRecords    = fetchAll(spec.getTableName());
                allRecordsIdx = 0;
            }
            if (allRecordsIdx >= allRecords.size()) {
                exhausted = true;
                return 0;
            }
            int batchSize = Math.min(conf.stepSeconds > 0 ? 4096 : 4096,
                                     allRecords.size() - allRecordsIdx);
            int count = writeBatch(allRecords, allRecordsIdx, batchSize, spec.getTableName());
            allRecordsIdx += batchSize;
            if (allRecordsIdx >= allRecords.size()) exhausted = true;
            return count;
        } catch (IOException e) {
            logger.error("Error reading from Prometheus table {}", spec.getTableName(), e);
            throw new RuntimeException("Failed to read Prometheus table " + spec.getTableName()
                    + ": " + e.getMessage(), e);
        }
    }

    private List<JsonNode> fetchAll(String tableName) throws IOException {
        switch (tableName) {
            case "metrics": return connection.fetchMetrics();
            case "targets": return connection.fetchTargets();
            case "alerts":  return connection.fetchAlerts();
            case "rules":   return connection.fetchRules();
            case "labels":  return connection.fetchLabels();
            case "samples": return connection.fetchSamples();
            default: throw new IllegalArgumentException("Unknown Prometheus table: " + tableName);
        }
    }

    @Override
    public void close() throws Exception {
        logger.debug("PrometheusRecordReader closed for {}", spec.getTableName());
    }

    // -------------------------------------------------------------------------
    // Field writing
    // -------------------------------------------------------------------------

    private int writeBatch(List<JsonNode> records, int offset, int size, String tableName) {
        int count = 0;
        for (int i = offset; i < offset + size; i++) {
            writeRecord(records.get(i), tableName, count);
            count++;
        }
        for (ValueVector v : vectors) v.setValueCount(count);
        return count;
    }

    private void writeRecord(JsonNode record, String tableName, int idx) {
        switch (tableName) {
            case "metrics": writeMetric(record, idx);  break;
            case "targets": writeTarget(record, idx);  break;
            case "alerts":  writeAlert(record, idx);   break;
            case "rules":   writeRule(record, idx);    break;
            case "labels":  writeLabel(record, idx);   break;
            case "samples": writeSample(record, idx);  break;
        }
    }

    private void writeMetric(JsonNode rec, int idx) {
        set(idx, "metric_name", rec.path("metricName").asText(null));
        set(idx, "type",        rec.path("type").asText(null));
        set(idx, "help",        rec.path("help").asText(null));
        set(idx, "unit",        rec.path("unit").asText(null));
    }

    private void writeTarget(JsonNode rec, int idx) {
        set(idx, "job",                          rec.path("job").asText(null));
        set(idx, "instance",                     rec.path("instance").asText(null));
        set(idx, "health",                       rec.path("health").asText(null));
        set(idx, "last_scrape",                  rec.path("lastScrape").asText(null));
        setFloat(idx, "last_scrape_duration_seconds", rec.path("lastScrapeDurationSeconds"));
        set(idx, "last_error",                   rec.path("lastError").asText(null));
        set(idx, "labels",                       rec.path("labels").asText(null));
        set(idx, "scrape_url",                   rec.path("scrapeUrl").asText(null));
    }

    private void writeAlert(JsonNode rec, int idx) {
        set(idx, "alert_name",          rec.path("alertName").asText(null));
        set(idx, "state",               rec.path("state").asText(null));
        setFloat(idx, "metric_value",   rec.path("value"));
        set(idx, "labels",              rec.path("labels").asText(null));
        set(idx, "annotations",         rec.path("annotations").asText(null));
        set(idx, "active_at",           rec.path("activeAt").asText(null));
        set(idx, "generator_url",       rec.path("generatorUrl").asText(null));
    }

    private void writeRule(JsonNode rec, int idx) {
        set(idx, "group_name",                 rec.path("groupName").asText(null));
        set(idx, "rule_name",                  rec.path("ruleName").asText(null));
        set(idx, "rule_type",                  rec.path("ruleType").asText(null));
        set(idx, "query",                      rec.path("query").asText(null));
        set(idx, "health",                     rec.path("health").asText(null));
        set(idx, "last_evaluation",            rec.path("lastEvaluation").asText(null));
        setFloat(idx, "evaluation_time_seconds", rec.path("evaluationTimeSeconds"));
        setFloat(idx, "duration_seconds",      rec.path("durationSeconds"));
        set(idx, "labels",                     rec.path("labels").asText(null));
        set(idx, "annotations",                rec.path("annotations").asText(null));
    }

    private void writeLabel(JsonNode rec, int idx) {
        set(idx, "label_name", rec.path("labelName").asText(null));
    }

    private void writeSample(JsonNode rec, int idx) {
        set(idx, "metric_name",         rec.path("metricName").asText(null));
        set(idx, "labels",              rec.path("labels").asText(null));
        set(idx, "sample_time",         rec.path("timestamp").asText(null));
        setFloat(idx, "sample_value",   rec.path("value"));
    }

    // -------------------------------------------------------------------------
    // Vector write helpers
    // -------------------------------------------------------------------------

    private void set(int idx, String name, String value) {
        if (value == null || value.isEmpty()) return;
        int pos = fieldIndex(name);
        if (pos < 0) return;
        ValueVector v = vectors.get(pos);
        if (v instanceof VarCharVector) {
            ((VarCharVector) v).setSafe(idx, value.getBytes(StandardCharsets.UTF_8));
        }
    }

    private void setFloat(int idx, String name, JsonNode node) {
        if (node == null || node.isNull() || node.isMissingNode()) return;
        double d = node.asDouble(Double.NaN);
        if (Double.isNaN(d)) return;
        int pos = fieldIndex(name);
        if (pos < 0) return;
        ValueVector v = vectors.get(pos);
        if (v instanceof Float8Vector) {
            ((Float8Vector) v).setSafe(idx, d);
        }
    }

    private int fieldIndex(String name) {
        for (int i = 0; i < projectedFields.size(); i++) {
            if (projectedFields.get(i).getName().equals(name)) return i;
        }
        return -1;
    }
}
