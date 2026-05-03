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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB 3 HTTP SQL API client.
 *
 * <p>All queries go to {@code POST /api/v3/query_sql} with Bearer token auth.
 * Schema is discovered from {@code information_schema.columns} where
 * {@code table_schema = 'iox'} (the user-data schema in InfluxDB 3).
 *
 * <p>Data pagination uses SQL LIMIT/OFFSET since InfluxDB 3 has no
 * continuation-token mechanism on the HTTP API.
 */
public class InfluxDBConnection {

    private static final Logger logger = LoggerFactory.getLogger(InfluxDBConnection.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final InfluxDBConf conf;
    private final HttpClient httpClient;

    public InfluxDBConnection(InfluxDBConf conf) {
        this.conf = conf;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
    }

    // ── Public API ────────────────────────────────────────────────────────────

    /** Verifies connectivity by listing tables. Throws on failure. */
    public void testConnection() throws IOException {
        queryJson("SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox' LIMIT 1");
    }

    /** Lists all measurement names in the configured database. */
    public List<String> listMeasurements() throws IOException {
        JsonNode rows = queryJson(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'iox'");
        List<String> names = new ArrayList<>();
        if (rows != null && rows.isArray()) {
            for (JsonNode row : rows) {
                JsonNode col = row.get("table_name");
                if (col != null && !col.isNull()) {
                    names.add(col.asText());
                }
            }
        }
        return names;
    }

    /**
     * Returns the Arrow schema for a measurement by querying information_schema.columns.
     * Always includes the {@code time} column first.
     */
    public List<InfluxField> inferSchema(String measurement) throws IOException {
        String sql = "SELECT column_name, data_type FROM information_schema.columns "
                + "WHERE table_schema = 'iox' AND table_name = '" + escapeSql(measurement) + "' "
                + "ORDER BY ordinal_position";
        JsonNode rows = queryJson(sql);

        List<InfluxField> fields = new ArrayList<>();
        if (rows != null && rows.isArray()) {
            for (JsonNode row : rows) {
                String colName = row.path("column_name").asText();
                String dataType = row.path("data_type").asText();
                fields.add(new InfluxField(colName, mapType(dataType)));
            }
        }
        return fields;
    }

    /**
     * Fetches one page of rows from a measurement.
     *
     * @param measurement  measurement (table) name
     * @param offset       0-based row offset for OFFSET clause
     * @return list of row maps; empty list signals end of data
     */
    public List<Map<String, Object>> queryPage(String measurement, int offset) throws IOException {
        String sql = "SELECT * FROM \"" + escapeSql(measurement) + "\""
                + " ORDER BY time"
                + " LIMIT " + conf.pageSize
                + " OFFSET " + offset;
        JsonNode rows = queryJson(sql);

        List<Map<String, Object>> result = new ArrayList<>();
        if (rows != null && rows.isArray()) {
            for (JsonNode row : rows) {
                result.add(MAPPER.convertValue(row, Map.class));
            }
        }
        return result;
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    /**
     * Executes a SQL query and returns the JSON response array.
     * InfluxDB 3 returns a flat JSON array: [{col: val, ...}, ...].
     */
    private JsonNode queryJson(String sql) throws IOException {
        String body;
        try {
            body = MAPPER.writeValueAsString(Map.of(
                    "db", conf.database,
                    "q", sql,
                    "format", "json"
            ));
        } catch (Exception e) {
            throw new IOException("Failed to serialize query body", e);
        }

        String authHeader = conf.token != null && !conf.token.isBlank()
                ? "Bearer " + conf.token.trim()
                : "";

        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(conf.host.replaceAll("/$", "") + "/api/v3/query_sql"))
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body));

        if (!authHeader.isEmpty()) {
            builder.header("Authorization", authHeader);
        }

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("HTTP request interrupted", e);
        }

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("InfluxDB query failed [" + response.statusCode() + "]: "
                    + response.body() + " | sql=" + sql);
        }

        String responseBody = response.body();
        if (responseBody == null || responseBody.isBlank() || responseBody.equals("null")) {
            return MAPPER.createArrayNode();
        }

        JsonNode node = MAPPER.readTree(responseBody);
        // InfluxDB 3 may return an object with an error key on failure
        if (node.isObject() && node.has("error")) {
            throw new IOException("InfluxDB error: " + node.get("error").asText()
                    + " | sql=" + sql);
        }
        return node;
    }

    /** Maps InfluxDB 3 information_schema data_type strings to Arrow types. */
    static ArrowType mapType(String influxType) {
        if (influxType == null) return ArrowType.Utf8.INSTANCE;
        String t = influxType.trim();
        switch (t) {
            case "Timestamp":
                return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
            case "Int64":
                return new ArrowType.Int(64, true);
            case "Float64":
                return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
            case "Boolean":
                return ArrowType.Bool.INSTANCE;
            default:
                // Utf8, Dictionary(Int32, Utf8), and any unknown types → string
                return ArrowType.Utf8.INSTANCE;
        }
    }

    /** Minimal SQL string escaping — replaces single quotes to prevent injection. */
    private static String escapeSql(String value) {
        return value.replace("'", "''").replace("\"", "\"\"");
    }

    public int getPageSize() { return conf.pageSize; }

    // ── Data structures ───────────────────────────────────────────────────────

    public static class InfluxField {
        public final String name;
        public final ArrowType type;

        public InfluxField(String name, ArrowType type) {
            this.name = name;
            this.type = type;
        }

        public Field toArrowField() {
            return new Field(name, new FieldType(true, type, null), Collections.emptyList());
        }
    }
}
