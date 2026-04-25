/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.cosmosdb;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Cosmos DB REST API client with HMAC-SHA256 master key auth.
 *
 * <p>Each container in the configured database is treated as a table.
 * Schema is inferred by sampling up to {@code schemaSampleSize} documents.
 *
 * <p>Auth: <a href="https://learn.microsoft.com/en-us/rest/api/cosmos-db/access-control-on-cosmosdb-resources">
 *   Cosmos DB resource token</a> — HMAC-SHA256 over (verb, resourceType, resourceLink, date).
 *
 * <p>Pagination: responses include {@code x-ms-continuation} header; pass it back
 * as a request header to fetch the next page.
 */
public class CosmosConnection {

    private static final Logger logger = LoggerFactory.getLogger(CosmosConnection.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final DateTimeFormatter RFC1123 =
            DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withLocale(java.util.Locale.US);

    private final CosmosConf conf;
    private final HttpClient httpClient;

    public CosmosConnection(CosmosConf conf) {
        this.conf = conf;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
    }

    // ── Public schema / metadata API ─────────────────────────────────────────

    /** Lists all container names in the configured database. */
    public List<String> listContainers() throws IOException {
        String url = conf.endpoint + "/dbs/" + conf.database + "/colls";
        JsonNode resp = getJson(url, "get", "colls", "dbs/" + conf.database);
        List<String> names = new ArrayList<>();
        JsonNode colls = resp.get("DocumentCollections");
        if (colls != null && colls.isArray()) {
            for (JsonNode c : colls) {
                JsonNode id = c.get("id");
                if (id != null) names.add(id.asText());
            }
        }
        return names;
    }

    /** Returns the partition key path for a container, e.g. "/status". */
    public String getPartitionKeyPath(String container) throws IOException {
        String url = conf.endpoint + "/dbs/" + conf.database + "/colls/" + container;
        JsonNode resp = getJson(url, "get", "colls", "dbs/" + conf.database + "/colls/" + container);
        JsonNode pk = resp.path("partitionKey").path("paths");
        if (pk.isArray() && pk.size() > 0) {
            return pk.get(0).asText();
        }
        return "/id";
    }

    /**
     * Infers schema by sampling documents. Returns ordered field list.
     * Nested objects are flattened one level deep (a.b → "a_b" column).
     * Arrays and deeper nesting are serialized as JSON strings.
     */
    public List<CosmosField> inferSchema(String container) throws IOException {
        String resourceLink = "dbs/" + conf.database + "/colls/" + container;
        String url = conf.endpoint + "/" + resourceLink + "/docs";

        String query = "SELECT TOP " + conf.schemaSampleSize + " * FROM c";
        List<Map<String, Object>> docs = queryDocs(url, resourceLink, query, null);

        // Collect all field paths and infer types
        Map<String, ArrowType> fieldTypes = new LinkedHashMap<>();
        for (Map<String, Object> doc : docs) {
            collectFields("", doc, fieldTypes);
        }

        // Remove internal Cosmos system fields (_rid, _self, _etag, _attachments, _ts)
        fieldTypes.remove("_rid");
        fieldTypes.remove("_self");
        fieldTypes.remove("_etag");
        fieldTypes.remove("_attachments");
        fieldTypes.remove("_ts");

        List<CosmosField> fields = new ArrayList<>();
        for (Map.Entry<String, ArrowType> e : fieldTypes.entrySet()) {
            fields.add(new CosmosField(e.getKey(), e.getValue()));
        }
        return fields;
    }

    /**
     * Queries documents from a container. Returns a page of results plus
     * an optional continuation token for the next page.
     */
    public CosmosPage queryPage(String container, String soql, String continuationToken)
            throws IOException {
        String resourceLink = "dbs/" + conf.database + "/colls/" + container;
        String url = conf.endpoint + "/" + resourceLink + "/docs";

        HttpRequest.Builder builder = buildRequest("POST", url, "docs", resourceLink)
                .header("Content-Type", "application/query+json")
                .header("x-ms-documentdb-isquery", "true")
                .header("x-ms-documentdb-query-enablecrosspartitionquery", "true")
                .header("x-ms-max-item-count", String.valueOf(conf.pageSize));

        if (continuationToken != null) {
            builder.header("x-ms-continuation", continuationToken);
        }

        String body = MAPPER.writeValueAsString(
                Map.of("query", soql, "parameters", List.of()));
        builder.POST(HttpRequest.BodyPublishers.ofString(body));

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Cosmos DB query failed [" + response.statusCode() + "]: " + response.body());
        }

        JsonNode json = MAPPER.readTree(response.body());
        List<Map<String, Object>> docs = new ArrayList<>();
        JsonNode docArray = json.get("Documents");
        if (docArray != null && docArray.isArray()) {
            for (JsonNode d : docArray) {
                docs.add(MAPPER.convertValue(d, Map.class));
            }
        }

        String nextToken = response.headers().firstValue("x-ms-continuation").orElse(null);
        return new CosmosPage(docs, nextToken);
    }

    /** Pings the account endpoint to verify connectivity. */
    public void testConnection() throws IOException {
        String url = conf.endpoint + "/dbs/" + conf.database;
        getJson(url, "get", "dbs", "dbs/" + conf.database);
    }

    // ── Data structures ───────────────────────────────────────────────────────

    public static class CosmosField {
        public final String name;
        public final ArrowType type;

        public CosmosField(String name, ArrowType type) {
            this.name = name;
            this.type = type;
        }

        public Field toArrowField() {
            return new Field(name, new FieldType(true, type, null), Collections.emptyList());
        }
    }

    public static class CosmosPage {
        public final List<Map<String, Object>> docs;
        public final String continuationToken; // null if this is the last page

        public CosmosPage(List<Map<String, Object>> docs, String continuationToken) {
            this.docs = docs;
            this.continuationToken = continuationToken;
        }

        public boolean hasMore() {
            return continuationToken != null;
        }
    }

    // ── Internal helpers ──────────────────────────────────────────────────────

    private JsonNode getJson(String url, String verb, String resourceType, String resourceLink)
            throws IOException {
        HttpRequest request = buildRequest(verb, url, resourceType, resourceLink)
                .GET()
                .build();
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Cosmos DB request failed [" + response.statusCode() + "]: " + response.body());
        }
        return MAPPER.readTree(response.body());
    }

    private List<Map<String, Object>> queryDocs(String url, String resourceLink,
            String query, String continuationToken) throws IOException {
        HttpRequest.Builder builder = buildRequest("POST", url, "docs", resourceLink)
                .header("Content-Type", "application/query+json")
                .header("x-ms-documentdb-isquery", "true")
                .header("x-ms-documentdb-query-enablecrosspartitionquery", "true")
                .header("x-ms-max-item-count", String.valueOf(conf.schemaSampleSize));

        if (continuationToken != null) {
            builder.header("x-ms-continuation", continuationToken);
        }

        String body = MAPPER.writeValueAsString(
                Map.of("query", query, "parameters", List.of()));
        builder.POST(HttpRequest.BodyPublishers.ofString(body));

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Request interrupted", e);
        }

        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Schema inference query failed [" + response.statusCode() + "]: " + response.body());
        }

        JsonNode json = MAPPER.readTree(response.body());
        List<Map<String, Object>> docs = new ArrayList<>();
        JsonNode docArray = json.get("Documents");
        if (docArray != null && docArray.isArray()) {
            for (JsonNode d : docArray) {
                docs.add(MAPPER.convertValue(d, Map.class));
            }
        }
        return docs;
    }

    @SuppressWarnings("unchecked")
    private void collectFields(String prefix, Map<String, Object> obj,
            Map<String, ArrowType> fieldTypes) {
        for (Map.Entry<String, Object> entry : obj.entrySet()) {
            String key = prefix.isEmpty() ? entry.getKey() : prefix + "_" + entry.getKey();
            Object val = entry.getValue();

            if (val == null) {
                // Don't infer type from null — leave existing type or skip
                fieldTypes.putIfAbsent(key, ArrowType.Utf8.INSTANCE);
            } else if (val instanceof Map) {
                // Flatten one level
                collectFields(key, (Map<String, Object>) val, fieldTypes);
            } else if (val instanceof List) {
                // Arrays → JSON string
                fieldTypes.put(key, ArrowType.Utf8.INSTANCE);
            } else if (val instanceof Boolean) {
                fieldTypes.merge(key, ArrowType.Bool.INSTANCE, (a, b) -> a);
            } else if (val instanceof Integer) {
                fieldTypes.merge(key, new ArrowType.Int(64, true), (a, b) ->
                        (a instanceof ArrowType.FloatingPoint) ? a : b);
            } else if (val instanceof Long) {
                fieldTypes.merge(key, new ArrowType.Int(64, true), (a, b) ->
                        (a instanceof ArrowType.FloatingPoint) ? a : b);
            } else if (val instanceof Double || val instanceof Float) {
                // Float wins over Int if we've seen both
                fieldTypes.put(key, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
            } else if (val instanceof String) {
                String s = (String) val;
                // Detect ISO timestamps (heuristic: contains 'T' and 'Z' or '+')
                if (s.length() >= 20 && s.contains("T") && (s.endsWith("Z") || s.contains("+"))) {
                    fieldTypes.merge(key, new ArrowType.Timestamp(TimeUnit.MILLISECOND, null),
                            (a, b) -> a); // keep first inference
                } else {
                    fieldTypes.merge(key, ArrowType.Utf8.INSTANCE, (a, b) -> ArrowType.Utf8.INSTANCE);
                }
            } else {
                fieldTypes.putIfAbsent(key, ArrowType.Utf8.INSTANCE);
            }
        }
    }

    /**
     * Builds an authenticated HTTP request.
     *
     * <p>Auth header: {@code type=master&ver=1.0&sig=<base64(HMAC-SHA256)>}
     * Signing payload: {@code verb\nresourceType\nresourceLink\ndate\n\n} (all lowercase).
     * If masterKey is blank (local emulator), the Authorization header is still sent
     * but the emulator ignores it.
     */
    private HttpRequest.Builder buildRequest(String verb, String url,
            String resourceType, String resourceLink) {
        String date = ZonedDateTime.now(ZoneOffset.UTC).format(RFC1123);
        String authHeader = buildAuthHeader(verb.toLowerCase(), resourceType.toLowerCase(),
                resourceLink, date.toLowerCase());

        return HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .header("x-ms-date", date)
                .header("x-ms-version", "2018-12-31")
                .header("Authorization", authHeader)
                .header("Accept", "application/json");
    }

    private String buildAuthHeader(String verb, String resourceType,
            String resourceLink, String date) {
        String payload = verb + "\n" + resourceType + "\n" + resourceLink + "\n" + date + "\n\n";
        String sig = "";
        if (conf.masterKey != null && !conf.masterKey.isBlank()) {
            try {
                byte[] keyBytes = Base64.getDecoder().decode(conf.masterKey.trim());
                Mac mac = Mac.getInstance("HmacSHA256");
                mac.init(new SecretKeySpec(keyBytes, "HmacSHA256"));
                byte[] hashBytes = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
                sig = Base64.getEncoder().encodeToString(hashBytes);
            } catch (Exception e) {
                logger.warn("Failed to compute Cosmos DB auth signature: {}", e.getMessage());
            }
        }
        String token = "type=master&ver=1.0&sig=" + sig;
        return URLEncoder.encode(token, StandardCharsets.UTF_8);
    }
}
