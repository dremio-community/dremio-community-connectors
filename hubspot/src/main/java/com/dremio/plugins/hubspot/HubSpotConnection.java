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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Collections;
import java.util.List;

/**
 * HTTP client for the HubSpot CRM REST API v3.
 *
 * <p>Auth: Bearer token (Private App access token). No OAuth dance required.
 */
public class HubSpotConnection {

    private static final Logger logger = LoggerFactory.getLogger(HubSpotConnection.class);

    /**
     * CRM object types exposed as Dremio tables.
     * These all use the standard /crm/v3/objects/{type} endpoint + Properties API.
     */
    public static final List<String> STANDARD_OBJECTS = Collections.unmodifiableList(Arrays.asList(
            "contacts", "companies", "deals", "tickets",
            "products", "line_items",
            "calls", "emails", "meetings", "notes", "tasks"
    ));

    /** owners uses a different endpoint (/crm/v3/owners) with a fixed schema. */
    public static final String OWNERS_OBJECT = "owners";

    private final HubSpotConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;

    public HubSpotConnection(HubSpotConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
    }

    // -------------------------------------------------------------------------
    // Auth / health check
    // -------------------------------------------------------------------------

    /**
     * Verifies that the access token is valid by hitting a lightweight endpoint.
     */
    public void verifyToken() throws IOException {
        String url = conf.baseUrl.replaceAll("/$", "")
                + "/crm/v3/objects/contacts?limit=1";
        JsonNode root = getJson(url);
        // If we get here without exception, the token is valid
        logger.info("HubSpot token verified; API reachable at {}", conf.baseUrl);
    }

    // -------------------------------------------------------------------------
    // Schema: properties API
    // -------------------------------------------------------------------------

    /**
     * Returns all property definitions for a CRM object type.
     * Each HubSpotProperty carries name + type (string, number, bool, date, datetime, enumeration).
     */
    public List<HubSpotProperty> getProperties(String objectType) throws IOException {
        String url = conf.baseUrl.replaceAll("/$", "")
                + "/crm/v3/properties/" + objectType;
        JsonNode root = getJson(url);
        JsonNode results = root.get("results");

        List<HubSpotProperty> props = new ArrayList<>();
        if (results != null && results.isArray()) {
            for (JsonNode p : results) {
                String name = p.path("name").asText(null);
                String type = p.path("type").asText("string");
                boolean hidden = p.path("hidden").asBoolean(false);
                boolean calculated = p.path("calculated").asBoolean(false);
                if (name != null && !hidden) {
                    props.add(new HubSpotProperty(name, type, calculated));
                }
            }
        }
        return props;
    }

    // -------------------------------------------------------------------------
    // Data: Search API (POST) — handles cursor pagination
    // -------------------------------------------------------------------------

    /**
     * Fetches a page of records for a standard CRM object type.
     *
     * <p>Uses the Search API (POST) because it:
     * (a) returns a {@code total} field for row-count estimation,
     * (b) accepts a JSON body (no URL length limits for large property lists),
     * (c) supports future filter pushdown.
     *
     * @param objectType  CRM object type, e.g. "contacts"
     * @param properties  list of property names to include in the response
     * @param after       pagination cursor from previous response, or null for first page
     */
    public HubSpotPage fetchPage(String objectType, List<String> properties, String after)
            throws IOException {
        String url = conf.baseUrl.replaceAll("/$", "")
                + "/crm/v3/objects/" + objectType + "/search";

        ObjectNode body = mapper.createObjectNode();
        ArrayNode propsArray = body.putArray("properties");
        for (String p : properties) {
            propsArray.add(p);
        }
        body.put("limit", Math.min(conf.pageSize, 100));
        if (after != null) {
            body.put("after", after);
        }

        String bodyStr = mapper.writeValueAsString(body);
        JsonNode root = postJson(url, bodyStr);
        return parseStandardPage(root);
    }

    /**
     * Fetches a page of owners (special endpoint with fixed schema).
     */
    public HubSpotPage fetchOwnersPage(String after) throws IOException {
        StringBuilder url = new StringBuilder(conf.baseUrl.replaceAll("/$", ""))
                .append("/crm/v3/owners?limit=")
                .append(Math.min(conf.pageSize, 100));
        if (after != null) {
            url.append("&after=").append(after);
        }
        if (conf.includeArchived) {
            url.append("&archived=true");
        }

        JsonNode root = getJson(url.toString());
        return parseStandardPage(root);
    }

    // -------------------------------------------------------------------------
    // Row count estimation
    // -------------------------------------------------------------------------

    /**
     * Returns an estimated row count for a CRM object by fetching the first page
     * (Search API always returns {@code total} in the response).
     */
    public long estimateCount(String objectType) throws IOException {
        if (OWNERS_OBJECT.equals(objectType)) {
            // owners endpoint doesn't return total; default
            return 500L;
        }
        try {
            String url = conf.baseUrl.replaceAll("/$", "")
                    + "/crm/v3/objects/" + objectType + "/search";
            ObjectNode body = mapper.createObjectNode();
            body.put("limit", 1);
            JsonNode root = postJson(url, mapper.writeValueAsString(body));
            return root.path("total").asLong(1000L);
        } catch (Exception e) {
            logger.warn("Could not estimate count for {}: {}", objectType, e.getMessage());
            return 1000L;
        }
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private JsonNode getJson(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + conf.accessToken)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();
        return execute(request, url);
    }

    private JsonNode postJson(String url, String jsonBody) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + conf.accessToken)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        return execute(request, url);
    }

    private JsonNode execute(HttpRequest request, String url) throws IOException {
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during HubSpot API request to " + url, e);
        }

        if (response.statusCode() == 401) {
            throw new IOException("HubSpot authentication failed (401) — check your Private App token");
        }
        if (response.statusCode() == 403) {
            throw new IOException("HubSpot permission denied (403) — ensure the token has CRM read scopes for: " + url);
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("HubSpot API error (HTTP " + response.statusCode() + ") at " + url
                    + ": " + response.body());
        }

        return mapper.readTree(response.body());
    }

    // -------------------------------------------------------------------------
    // Response parsing
    // -------------------------------------------------------------------------

    private HubSpotPage parseStandardPage(JsonNode root) {
        HubSpotPage page = new HubSpotPage();
        page.total = root.path("total").asLong(-1);

        JsonNode results = root.get("results");
        page.records = new ArrayList<>();
        if (results != null && results.isArray()) {
            for (JsonNode rec : results) {
                page.records.add(rec);
            }
        }

        JsonNode paging = root.get("paging");
        if (paging != null && !paging.isNull()) {
            JsonNode next = paging.get("next");
            if (next != null && !next.isNull()) {
                page.nextCursor = next.path("after").asText(null);
            }
        }
        return page;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class HubSpotPage {
        /** Total matching records (from Search API; -1 if not available). */
        public long total;
        /** Records in this page, each as a raw JsonNode. */
        public List<JsonNode> records;
        /** Cursor for the next page; null if this is the last page. */
        public String nextCursor;
    }

    public static class HubSpotProperty {
        public final String name;
        /** HubSpot type: string, number, bool, date, datetime, enumeration, phone_number. */
        public final String type;
        public final boolean calculated;

        public HubSpotProperty(String name, String type, boolean calculated) {
            this.name = name;
            this.type = type;
            this.calculated = calculated;
        }
    }
}
