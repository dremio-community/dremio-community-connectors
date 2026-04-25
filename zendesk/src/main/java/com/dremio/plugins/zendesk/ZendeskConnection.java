/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.zendesk;

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
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Zendesk REST API client using HTTP Basic Auth (email/token).
 *
 * <p>Each logical "table" maps to a Zendesk API endpoint with a static schema.
 * Pagination uses Zendesk cursor-based pagination (links.next).
 */
public class ZendeskConnection {

    private static final Logger logger = LoggerFactory.getLogger(ZendeskConnection.class);

    private final ZendeskConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final String baseUrl;
    private final String authHeader;

    // -------------------------------------------------------------------------
    // Static table registry
    // -------------------------------------------------------------------------

    /**
     * Describes one Zendesk "table" — its API endpoint, the JSON array key in the
     * response, and its static Arrow schema.
     */
    public static class ZendeskTable {
        public final String name;
        public final String endpoint;   // e.g. "/api/v2/tickets.json"
        public final String recordKey;  // JSON key holding the array, e.g. "tickets"
        public final List<ZendeskField> fields;

        public ZendeskTable(String name, String endpoint, String recordKey, List<ZendeskField> fields) {
            this.name = name;
            this.endpoint = endpoint;
            this.recordKey = recordKey;
            this.fields = fields;
        }
    }

    /** Describes one field: its name in the Arrow schema, the path to extract from JSON, and Arrow type. */
    public static class ZendeskField {
        public final String name;       // Arrow column name
        public final String jsonPath;   // dot-separated path, e.g. "via.channel"
        public final ArrowType type;

        public ZendeskField(String name, String jsonPath, ArrowType type) {
            this.name = name;
            this.jsonPath = jsonPath;
            this.type = type;
        }

        /** Convenience: name == jsonPath (top-level field). */
        public ZendeskField(String name, ArrowType type) {
            this(name, name, type);
        }

        public Field toArrowField() {
            return new Field(name, new FieldType(true, type, null), Collections.emptyList());
        }
    }

    private static final Map<String, ZendeskTable> TABLES = buildTables();

    private static Map<String, ZendeskTable> buildTables() {
        Map<String, ZendeskTable> map = new LinkedHashMap<>();

        // ── tickets ──────────────────────────────────────────────────────────
        map.put("tickets", new ZendeskTable("tickets",
                "/api/v2/tickets.json?page[size]=100&sort=id",
                "tickets",
                Arrays.asList(
                        new ZendeskField("id",              new ArrowType.Int(64, true)),
                        new ZendeskField("url",             ArrowType.Utf8.INSTANCE),
                        new ZendeskField("external_id",     ArrowType.Utf8.INSTANCE),
                        new ZendeskField("type",            ArrowType.Utf8.INSTANCE),
                        new ZendeskField("subject",         ArrowType.Utf8.INSTANCE),
                        new ZendeskField("raw_subject",     ArrowType.Utf8.INSTANCE),
                        new ZendeskField("description",     ArrowType.Utf8.INSTANCE),
                        new ZendeskField("priority",        ArrowType.Utf8.INSTANCE),
                        new ZendeskField("status",          ArrowType.Utf8.INSTANCE),
                        new ZendeskField("requester_id",    new ArrowType.Int(64, true)),
                        new ZendeskField("submitter_id",    new ArrowType.Int(64, true)),
                        new ZendeskField("assignee_id",     new ArrowType.Int(64, true)),
                        new ZendeskField("organization_id", new ArrowType.Int(64, true)),
                        new ZendeskField("group_id",        new ArrowType.Int(64, true)),
                        new ZendeskField("brand_id",        new ArrowType.Int(64, true)),
                        new ZendeskField("has_incidents",   ArrowType.Bool.INSTANCE),
                        new ZendeskField("is_public",       ArrowType.Bool.INSTANCE),
                        new ZendeskField("via_channel",     "via.channel", ArrowType.Utf8.INSTANCE),
                        new ZendeskField("tags",            ArrowType.Utf8.INSTANCE), // JSON array → string
                        new ZendeskField("created_at",      new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("updated_at",      new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("due_at",          new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        // ── users ─────────────────────────────────────────────────────────────
        map.put("users", new ZendeskTable("users",
                "/api/v2/users.json?page[size]=100",
                "users",
                Arrays.asList(
                        new ZendeskField("id",                  new ArrowType.Int(64, true)),
                        new ZendeskField("url",                 ArrowType.Utf8.INSTANCE),
                        new ZendeskField("name",                ArrowType.Utf8.INSTANCE),
                        new ZendeskField("email",               ArrowType.Utf8.INSTANCE),
                        new ZendeskField("alias",               ArrowType.Utf8.INSTANCE),
                        new ZendeskField("active",              ArrowType.Bool.INSTANCE),
                        new ZendeskField("verified",            ArrowType.Bool.INSTANCE),
                        new ZendeskField("shared",              ArrowType.Bool.INSTANCE),
                        new ZendeskField("locale",              ArrowType.Utf8.INSTANCE),
                        new ZendeskField("locale_id",           new ArrowType.Int(64, true)),
                        new ZendeskField("time_zone",           ArrowType.Utf8.INSTANCE),
                        new ZendeskField("last_login_at",       new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("phone",               ArrowType.Utf8.INSTANCE),
                        new ZendeskField("role",                ArrowType.Utf8.INSTANCE),
                        new ZendeskField("organization_id",     new ArrowType.Int(64, true)),
                        new ZendeskField("default_group_id",    new ArrowType.Int(64, true)),
                        new ZendeskField("moderator",           ArrowType.Bool.INSTANCE),
                        new ZendeskField("notes",               ArrowType.Utf8.INSTANCE),
                        new ZendeskField("suspended",           ArrowType.Bool.INSTANCE),
                        new ZendeskField("ticket_restriction",  ArrowType.Utf8.INSTANCE),
                        new ZendeskField("tags",                ArrowType.Utf8.INSTANCE),
                        new ZendeskField("created_at",          new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("updated_at",          new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        // ── organizations ─────────────────────────────────────────────────────
        map.put("organizations", new ZendeskTable("organizations",
                "/api/v2/organizations.json?page[size]=100",
                "organizations",
                Arrays.asList(
                        new ZendeskField("id",              new ArrowType.Int(64, true)),
                        new ZendeskField("url",             ArrowType.Utf8.INSTANCE),
                        new ZendeskField("external_id",     ArrowType.Utf8.INSTANCE),
                        new ZendeskField("name",            ArrowType.Utf8.INSTANCE),
                        new ZendeskField("domain_names",    ArrowType.Utf8.INSTANCE), // JSON array
                        new ZendeskField("group_id",        new ArrowType.Int(64, true)),
                        new ZendeskField("shared_tickets",  ArrowType.Bool.INSTANCE),
                        new ZendeskField("shared_comments", ArrowType.Bool.INSTANCE),
                        new ZendeskField("notes",           ArrowType.Utf8.INSTANCE),
                        new ZendeskField("details",         ArrowType.Utf8.INSTANCE),
                        new ZendeskField("tags",            ArrowType.Utf8.INSTANCE),
                        new ZendeskField("created_at",      new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("updated_at",      new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        // ── groups ────────────────────────────────────────────────────────────
        map.put("groups", new ZendeskTable("groups",
                "/api/v2/groups.json?page[size]=100",
                "groups",
                Arrays.asList(
                        new ZendeskField("id",          new ArrowType.Int(64, true)),
                        new ZendeskField("url",         ArrowType.Utf8.INSTANCE),
                        new ZendeskField("name",        ArrowType.Utf8.INSTANCE),
                        new ZendeskField("description", ArrowType.Utf8.INSTANCE),
                        new ZendeskField("is_public",   "default", ArrowType.Bool.INSTANCE),
                        new ZendeskField("deleted",     ArrowType.Bool.INSTANCE),
                        new ZendeskField("created_at",  new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("updated_at",  new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        // ── ticket_metrics ────────────────────────────────────────────────────
        map.put("ticket_metrics", new ZendeskTable("ticket_metrics",
                "/api/v2/ticket_metrics.json?page[size]=100",
                "ticket_metrics",
                Arrays.asList(
                        new ZendeskField("id",                              new ArrowType.Int(64, true)),
                        new ZendeskField("ticket_id",                       new ArrowType.Int(64, true)),
                        new ZendeskField("reopens",                         new ArrowType.Int(32, true)),
                        new ZendeskField("replies",                         new ArrowType.Int(32, true)),
                        new ZendeskField("assignee_stations",               new ArrowType.Int(32, true)),
                        new ZendeskField("group_stations",                  new ArrowType.Int(32, true)),
                        new ZendeskField("first_resolution_time_calendar",  "first_resolution_time_in_minutes.calendar", new ArrowType.Int(32, true)),
                        new ZendeskField("first_resolution_time_business",  "first_resolution_time_in_minutes.business", new ArrowType.Int(32, true)),
                        new ZendeskField("full_resolution_time_calendar",   "full_resolution_time_in_minutes.calendar",  new ArrowType.Int(32, true)),
                        new ZendeskField("full_resolution_time_business",   "full_resolution_time_in_minutes.business",  new ArrowType.Int(32, true)),
                        new ZendeskField("agent_wait_time_calendar",        "agent_wait_time_in_minutes.calendar",       new ArrowType.Int(32, true)),
                        new ZendeskField("agent_wait_time_business",        "agent_wait_time_in_minutes.business",       new ArrowType.Int(32, true)),
                        new ZendeskField("requester_wait_time_calendar",    "requester_wait_time_in_minutes.calendar",   new ArrowType.Int(32, true)),
                        new ZendeskField("requester_wait_time_business",    "requester_wait_time_in_minutes.business",   new ArrowType.Int(32, true)),
                        new ZendeskField("on_hold_time_calendar",           "on_hold_time_in_minutes.calendar",          new ArrowType.Int(32, true)),
                        new ZendeskField("on_hold_time_business",           "on_hold_time_in_minutes.business",          new ArrowType.Int(32, true)),
                        new ZendeskField("first_reply_time_calendar",       "first_reply_time_in_minutes.calendar",      new ArrowType.Int(32, true)),
                        new ZendeskField("first_reply_time_business",       "first_reply_time_in_minutes.business",      new ArrowType.Int(32, true)),
                        new ZendeskField("created_at",                      new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("updated_at",                      new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("solved_at",                       new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("initially_assigned_at",           new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("assigned_at",                     new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("latest_comment_added_at",         new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        // ── satisfaction_ratings ──────────────────────────────────────────────
        map.put("satisfaction_ratings", new ZendeskTable("satisfaction_ratings",
                "/api/v2/satisfaction_ratings.json?page[size]=100",
                "satisfaction_ratings",
                Arrays.asList(
                        new ZendeskField("id",           new ArrowType.Int(64, true)),
                        new ZendeskField("url",          ArrowType.Utf8.INSTANCE),
                        new ZendeskField("assignee_id",  new ArrowType.Int(64, true)),
                        new ZendeskField("group_id",     new ArrowType.Int(64, true)),
                        new ZendeskField("requester_id", new ArrowType.Int(64, true)),
                        new ZendeskField("ticket_id",    new ArrowType.Int(64, true)),
                        new ZendeskField("score",        ArrowType.Utf8.INSTANCE),
                        new ZendeskField("comment",      ArrowType.Utf8.INSTANCE),
                        new ZendeskField("reason",       ArrowType.Utf8.INSTANCE),
                        new ZendeskField("reason_code",  new ArrowType.Int(32, true)),
                        new ZendeskField("created_at",   new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new ZendeskField("updated_at",   new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        return Collections.unmodifiableMap(map);
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public ZendeskConnection(ZendeskConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
        this.baseUrl = "https://" + conf.subdomain + ".zendesk.com";
        // Basic Auth: "{email}/token:{apiToken}" base64-encoded
        String credentials = conf.email + "/token:" + conf.apiToken;
        this.authHeader = "Basic " + Base64.getEncoder().encodeToString(
                credentials.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    // -------------------------------------------------------------------------
    // Table registry
    // -------------------------------------------------------------------------

    public static Map<String, ZendeskTable> getTables() {
        return TABLES;
    }

    public static ZendeskTable getTable(String name) {
        ZendeskTable t = TABLES.get(name.toLowerCase());
        if (t == null) {
            throw new IllegalArgumentException("Unknown Zendesk table: " + name);
        }
        return t;
    }

    // -------------------------------------------------------------------------
    // API
    // -------------------------------------------------------------------------

    /** Validates connectivity by fetching the current user (works for all agent roles). */
    public void testConnection() throws IOException {
        getJson(baseUrl + "/api/v2/users/me.json");
        logger.info("Zendesk connection verified for subdomain '{}'", conf.subdomain);
    }

    /**
     * Fetches the first page of records for the given table.
     * Returns a ZendeskPage with records + optional nextUrl.
     */
    public ZendeskPage fetchPage(String url) throws IOException {
        String json = getJson(url.startsWith("http") ? url : baseUrl + url);
        return parsePage(json);
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private String getJson(String url) throws IOException {
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
            throw new IOException("Interrupted during Zendesk HTTP request", e);
        }

        if (response.statusCode() == 429) {
            // Rate limited — log and throw; caller can retry
            throw new IOException("Zendesk rate limit hit (HTTP 429). Retry after: "
                    + response.headers().firstValue("Retry-After").orElse("unknown") + "s");
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Zendesk API error (HTTP " + response.statusCode() + ") at "
                    + url + ": " + response.body());
        }
        return response.body();
    }

    private ZendeskPage parsePage(String json) throws IOException {
        JsonNode root = mapper.readTree(json);

        // Find the records array — try known keys or first array field
        List<Map<String, Object>> records = new ArrayList<>();
        for (ZendeskTable table : TABLES.values()) {
            if (root.has(table.recordKey)) {
                JsonNode arr = root.get(table.recordKey);
                if (arr != null && arr.isArray()) {
                    for (JsonNode rec : arr) {
                        records.add(mapper.convertValue(rec, Map.class));
                    }
                    break;
                }
            }
        }

        // Cursor pagination: links.next or next_page
        String nextUrl = null;
        if (root.has("links") && root.get("links").has("next")
                && !root.get("links").get("next").isNull()) {
            nextUrl = root.get("links").get("next").asText(null);
        } else if (root.has("next_page") && !root.get("next_page").isNull()) {
            nextUrl = root.get("next_page").asText(null);
        }

        boolean hasMore = false;
        if (root.has("meta") && root.get("meta").has("has_more")) {
            hasMore = root.get("meta").get("has_more").asBoolean(false);
        } else if (nextUrl != null && !nextUrl.isBlank()) {
            hasMore = true;
        }

        ZendeskPage page = new ZendeskPage();
        page.records = records;
        page.nextUrl = hasMore ? nextUrl : null;
        page.hasMore = hasMore;
        return page;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class ZendeskPage {
        public List<Map<String, Object>> records;
        public String nextUrl;
        public boolean hasMore;
    }
}
