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

import com.dremio.exec.record.BatchSchema;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public enum PrometheusTableDef {

    METRICS("metrics"),
    TARGETS("targets"),
    ALERTS("alerts"),
    RULES("rules"),
    LABELS("labels"),
    SAMPLES("samples");

    public final String tableName;

    PrometheusTableDef(String tableName) {
        this.tableName = tableName;
    }

    public static final List<String> ALL_TABLES;
    static {
        List<String> names = new ArrayList<>();
        for (PrometheusTableDef t : values()) names.add(t.tableName);
        ALL_TABLES = Collections.unmodifiableList(names);
    }

    public static Optional<PrometheusTableDef> fromName(String name) {
        for (PrometheusTableDef t : values()) {
            if (t.tableName.equals(name)) return Optional.of(t);
        }
        return Optional.empty();
    }

    public BatchSchema getSchema() {
        switch (this) {
            case METRICS: return metricsSchema();
            case TARGETS: return targetsSchema();
            case ALERTS:  return alertsSchema();
            case RULES:   return rulesSchema();
            case LABELS:  return labelsSchema();
            case SAMPLES: return samplesSchema();
            default: throw new IllegalArgumentException("Unknown table: " + tableName);
        }
    }

    // -------------------------------------------------------------------------
    // Schema definitions
    // -------------------------------------------------------------------------

    private static BatchSchema metricsSchema() {
        return schema(
            utf8("metric_name"),
            utf8("type"),
            utf8("help"),
            utf8("unit")
        );
    }

    private static BatchSchema targetsSchema() {
        return schema(
            utf8("job"),
            utf8("instance"),
            utf8("health"),
            utf8("last_scrape"),
            float8("last_scrape_duration_seconds"),
            utf8("last_error"),
            utf8("labels"),
            utf8("scrape_url")
        );
    }

    private static BatchSchema alertsSchema() {
        return schema(
            utf8("alert_name"),
            utf8("state"),
            float8("metric_value"),
            utf8("labels"),
            utf8("annotations"),
            utf8("active_at"),
            utf8("generator_url")
        );
    }

    private static BatchSchema rulesSchema() {
        return schema(
            utf8("group_name"),
            utf8("rule_name"),
            utf8("rule_type"),
            utf8("query"),
            utf8("health"),
            utf8("last_evaluation"),
            float8("evaluation_time_seconds"),
            float8("duration_seconds"),
            utf8("labels"),
            utf8("annotations")
        );
    }

    private static BatchSchema labelsSchema() {
        return schema(
            utf8("label_name")
        );
    }

    private static BatchSchema samplesSchema() {
        return schema(
            utf8("metric_name"),
            utf8("labels"),
            utf8("sample_time"),
            float8("sample_value")
        );
    }

    // -------------------------------------------------------------------------
    // Arrow field helpers
    // -------------------------------------------------------------------------

    private static BatchSchema schema(Field... fields) {
        return BatchSchema.newBuilder().addFields(Arrays.asList(fields)).build();
    }

    static Field utf8(String name) {
        return new Field(name, new FieldType(true, ArrowType.Utf8.INSTANCE, null), Collections.emptyList());
    }

    static Field float8(String name) {
        return new Field(name, new FieldType(true,
                new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE), null), Collections.emptyList());
    }
}
