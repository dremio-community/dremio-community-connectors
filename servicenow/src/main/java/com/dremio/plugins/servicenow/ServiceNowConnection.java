/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.servicenow;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ServiceNow REST API client using HTTP Basic Auth.
 */
public class ServiceNowConnection {

    private static final Logger logger = LoggerFactory.getLogger(ServiceNowConnection.class);

    private final ServiceNowConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final String baseUrl;
    private final String authHeader;

    // -------------------------------------------------------------------------
    // Static table registry
    // -------------------------------------------------------------------------

    public static class ServiceNowTable {
        public final String name;
        public final String endpoint;
        public final String recordKey;
        public final List<ServiceNowField> fields;

        public ServiceNowTable(String name, String endpoint, String recordKey, List<ServiceNowField> fields) {
            this.name = name;
            this.endpoint = endpoint;
            this.recordKey = recordKey;
            this.fields = fields;
        }
    }

    public static class ServiceNowField {
        public final String name;
        public final String jsonPath;
        public final ArrowType type;

        public ServiceNowField(String name, String jsonPath, ArrowType type) {
            this.name = name;
            this.jsonPath = jsonPath;
            this.type = type;
        }

        public ServiceNowField(String name, ArrowType type) {
            this(name, name, type);
        }

        public Field toArrowField() {
            return new Field(name, new FieldType(true, type, null), Collections.emptyList());
        }
    }

    private static final Map<String, ServiceNowTable> TABLES = buildTables();

    private static Map<String, ServiceNowTable> buildTables() {
        Map<String, ServiceNowTable> map = new LinkedHashMap<>();

        // ── incident ──────────────────────────────────────────────────────────
        map.put("incident", new ServiceNowTable("incident",
                "/api/now/table/incident?sysparm_limit=100",
                "result",
                Arrays.asList(
                        new ServiceNowField("sys_id", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("number", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("short_description", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("state", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("priority", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("caller_id", "caller_id.value", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("assigned_to", "assigned_to.value", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_created_on", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_updated_on", ArrowType.Utf8.INSTANCE)
                )));

        // ── task ─────────────────────────────────────────────────────────────
        map.put("task", new ServiceNowTable("task",
                "/api/now/table/task?sysparm_limit=100",
                "result",
                Arrays.asList(
                        new ServiceNowField("sys_id", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("number", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("short_description", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("state", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("priority", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("assigned_to", "assigned_to.value", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_class_name", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_created_on", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_updated_on", ArrowType.Utf8.INSTANCE)
                )));

        // ── sys_user ─────────────────────────────────────────────────────
        map.put("sys_user", new ServiceNowTable("sys_user",
                "/api/now/table/sys_user?sysparm_limit=100",
                "result",
                Arrays.asList(
                        new ServiceNowField("sys_id", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("user_name", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("name", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("first_name", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("last_name", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("email", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("active", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_created_on", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_updated_on", ArrowType.Utf8.INSTANCE)
                )));

        // ── problem ────────────────────────────────────────────────────────────
        map.put("problem", new ServiceNowTable("problem",
                "/api/now/table/problem?sysparm_limit=100",
                "result",
                Arrays.asList(
                        new ServiceNowField("sys_id", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("number", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("short_description", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("state", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("priority", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("assigned_to", "assigned_to.value", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_created_on", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_updated_on", ArrowType.Utf8.INSTANCE)
                )));

        // ── change_request ────────────────────────────────────────────────────
        map.put("change_request", new ServiceNowTable("change_request",
                "/api/now/table/change_request?sysparm_limit=100",
                "result",
                Arrays.asList(
                        new ServiceNowField("sys_id", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("number", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("short_description", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("type", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("state", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("priority", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("assigned_to", "assigned_to.value", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_created_on", ArrowType.Utf8.INSTANCE),
                        new ServiceNowField("sys_updated_on", ArrowType.Utf8.INSTANCE)
                )));

        return Collections.unmodifiableMap(map);
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public ServiceNowConnection(ServiceNowConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
        
        // Ensure no trailing slash
        String url = conf.instanceUrl;
        if (url != null && url.endsWith("/")) {
            url = url.substring(0, url.length() - 1);
        }
        this.baseUrl = url;

        String credentials = conf.username + ":" + conf.password;
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                credentials.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    // -------------------------------------------------------------------------
    // Table registry
    // -------------------------------------------------------------------------

    public static Map<String, ServiceNowTable> getTables() {
        return TABLES;
    }

    public static ServiceNowTable getTable(String name) {
        ServiceNowTable t = TABLES.get(name.toLowerCase());
        if (t == null) {
            throw new IllegalArgumentException("Unknown ServiceNow table: " + name);
        }
        return t;
    }

    // -------------------------------------------------------------------------
    // API
    // -------------------------------------------------------------------------

    /** Validates connectivity by fetching 1 incident. */
    public void testConnection() throws IOException {
        String url = baseUrl + "/api/now/table/incident?sysparm_limit=1";
        executeRequest(url);
        logger.info("ServiceNow connection verified for '{}'", conf.instanceUrl);
    }

    /**
     * Fetches the first page of records for the given table.
     */
    public ServiceNowPage fetchPage(String url) throws IOException {
        String fullUrl = url.startsWith("http") ? url : baseUrl + url;
        HttpResponse<String> response = executeRequest(fullUrl);
        return parsePage(response.body(), response.headers().firstValue("Link").orElse(null));
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private HttpResponse<String> executeRequest(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during ServiceNow HTTP request", e);
        }

        if (response.statusCode() == 429) {
            throw new IOException("ServiceNow rate limit hit (HTTP 429). Retry after: "
                    + response.headers().firstValue("Retry-After").orElse("unknown") + "s");
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("ServiceNow API error (HTTP " + response.statusCode() + ") at "
                    + url + ": " + response.body());
        }
        return response;
    }

    private ServiceNowPage parsePage(String json, String linkHeader) throws IOException {
        JsonNode root = mapper.readTree(json);

        List<Map<String, Object>> records = new ArrayList<>();
        if (root.has("result")) {
            JsonNode arr = root.get("result");
            if (arr != null && arr.isArray()) {
                for (JsonNode rec : arr) {
                    records.add(mapper.convertValue(rec, Map.class));
                }
            }
        }

        // Parse Link header for pagination, e.g., <https://...>;rel="next"
        String nextUrl = null;
        if (linkHeader != null && !linkHeader.isEmpty()) {
            Pattern pattern = Pattern.compile("<([^>]+)>;rel=\"next\"");
            Matcher matcher = pattern.matcher(linkHeader);
            if (matcher.find()) {
                nextUrl = matcher.group(1);
            }
        }

        ServiceNowPage page = new ServiceNowPage();
        page.records = records;
        page.nextUrl = nextUrl;
        page.hasMore = (nextUrl != null && !nextUrl.isBlank());
        
        // If results are less than requested limit (assuming 100), it's the last page
        if (records.isEmpty()) {
            page.hasMore = false;
        }

        return page;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class ServiceNowPage {
        public List<Map<String, Object>> records;
        public String nextUrl;
        public boolean hasMore;
    }
}
