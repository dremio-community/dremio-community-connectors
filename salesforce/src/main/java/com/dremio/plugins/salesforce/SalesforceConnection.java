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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

/**
 * Manages OAuth2 authentication and REST API calls to Salesforce.
 * Thread-safe: authenticate() is synchronized.
 */
public class SalesforceConnection {

    private static final Logger logger = LoggerFactory.getLogger(SalesforceConnection.class);

    private final SalesforceConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;

    private volatile String accessToken;
    private volatile String instanceUrl;

    public SalesforceConnection(SalesforceConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
    }

    // -------------------------------------------------------------------------
    // Authentication
    // -------------------------------------------------------------------------

    /**
     * Performs OAuth2 password-grant flow.
     * Stores access_token and instance_url for subsequent calls.
     */
    public synchronized void authenticate() throws IOException {
        logger.info("Authenticating to Salesforce at {}", conf.loginUrl);

        String passwordWithToken = conf.password + (conf.securityToken != null ? conf.securityToken : "");

        StringJoiner body = new StringJoiner("&");
        body.add("grant_type=password");
        body.add("client_id=" + URLEncoder.encode(conf.clientId, StandardCharsets.UTF_8));
        body.add("client_secret=" + URLEncoder.encode(conf.clientSecret, StandardCharsets.UTF_8));
        body.add("username=" + URLEncoder.encode(conf.username, StandardCharsets.UTF_8));
        body.add("password=" + URLEncoder.encode(passwordWithToken, StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(conf.loginUrl + "/services/oauth2/token"))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Salesforce authentication", e);
        }

        if (response.statusCode() != 200) {
            throw new IOException("Salesforce authentication failed (HTTP " + response.statusCode() + "): " + response.body());
        }

        JsonNode json = mapper.readTree(response.body());
        if (json.has("error")) {
            throw new IOException("Salesforce OAuth error: " + json.get("error").asText() + " - " + json.path("error_description").asText());
        }

        this.accessToken = json.get("access_token").asText();
        this.instanceUrl = json.get("instance_url").asText();
        logger.info("Authenticated to Salesforce instance: {}", instanceUrl);
    }

    /**
     * Ensures we have a valid access token. Authenticates lazily on first call.
     */
    public void ensureAuthenticated() throws IOException {
        if (accessToken == null) {
            authenticate();
        }
    }

    // -------------------------------------------------------------------------
    // API helpers
    // -------------------------------------------------------------------------

    private HttpResponse<String> getWithAuth(String url) throws IOException {
        ensureAuthenticated();
        return executeGet(url);
    }

    private HttpResponse<String> executeGet(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + accessToken)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Salesforce HTTP request", e);
        }
    }

    /**
     * Executes a GET, re-authenticating once on 401.
     */
    private String getJson(String url) throws IOException {
        HttpResponse<String> response = getWithAuth(url);
        if (response.statusCode() == 401) {
            logger.warn("Got 401 from Salesforce, re-authenticating");
            authenticate();
            response = executeGet(url);
        }
        if (response.statusCode() != 200) {
            throw new IOException("Salesforce API error (HTTP " + response.statusCode() + ") at " + url + ": " + response.body());
        }
        return response.body();
    }

    // -------------------------------------------------------------------------
    // Metadata APIs
    // -------------------------------------------------------------------------

    /**
     * Returns the list of queryable SObject API names.
     */
    public List<String> listSObjects() throws IOException {
        String url = instanceUrl + "/services/data/v" + conf.apiVersion + "/sobjects/";
        String json = getJson(url);
        JsonNode root = mapper.readTree(json);
        JsonNode sobjects = root.get("sobjects");

        List<String> names = new ArrayList<>();
        if (sobjects != null && sobjects.isArray()) {
            for (JsonNode node : sobjects) {
                boolean queryable = node.path("queryable").asBoolean(false);
                if (queryable) {
                    names.add(node.get("name").asText());
                }
            }
        }
        return names;
    }

    /**
     * Returns the field descriptions for the given SObject.
     */
    public List<SalesforceField> describeSObject(String objectName) throws IOException {
        String url = instanceUrl + "/services/data/v" + conf.apiVersion + "/sobjects/" + objectName + "/describe";
        String json = getJson(url);
        JsonNode root = mapper.readTree(json);
        JsonNode fields = root.get("fields");

        List<SalesforceField> result = new ArrayList<>();
        if (fields != null && fields.isArray()) {
            for (JsonNode f : fields) {
                SalesforceField sf = new SalesforceField();
                sf.name = f.get("name").asText();
                sf.type = f.get("type").asText();
                sf.nillable = f.path("nillable").asBoolean(true);
                result.add(sf);
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Query APIs
    // -------------------------------------------------------------------------

    /**
     * Executes a SOQL query and returns the first page of results.
     */
    public SalesforceQueryResult query(String soql) throws IOException {
        String url = instanceUrl + "/services/data/v" + conf.apiVersion + "/query?q="
                + URLEncoder.encode(soql, StandardCharsets.UTF_8);
        String json = getJson(url);
        return parseQueryResult(json);
    }

    /**
     * Fetches the next page of results using the nextRecordsUrl from a prior response.
     */
    public SalesforceQueryResult queryMore(String nextRecordsUrl) throws IOException {
        String url = instanceUrl + nextRecordsUrl;
        String json = getJson(url);
        return parseQueryResult(json);
    }

    /**
     * Executes SELECT COUNT() FROM {objectName} [{whereClause}] and returns the count.
     * whereClause should include the leading " WHERE " if non-empty, or be empty string.
     */
    public int countQuery(String objectName, String whereClause) throws IOException {
        String soql = "SELECT COUNT() FROM " + objectName + whereClause;
        String url = instanceUrl + "/services/data/v" + conf.apiVersion + "/query?q="
                + URLEncoder.encode(soql, StandardCharsets.UTF_8);
        String json = getJson(url);
        JsonNode root = mapper.readTree(json);
        return root.path("totalSize").asInt(0);
    }

    @SuppressWarnings("unchecked")
    private SalesforceQueryResult parseQueryResult(String json) throws IOException {
        JsonNode root = mapper.readTree(json);

        SalesforceQueryResult result = new SalesforceQueryResult();
        result.totalSize = root.path("totalSize").asInt(0);
        result.done = root.path("done").asBoolean(true);
        result.nextRecordsUrl = root.has("nextRecordsUrl") ? root.get("nextRecordsUrl").asText() : null;

        JsonNode records = root.get("records");
        result.records = new ArrayList<>();
        if (records != null && records.isArray()) {
            for (JsonNode rec : records) {
                Map<String, Object> map = mapper.convertValue(rec, Map.class);
                // Remove Salesforce metadata field
                map.remove("attributes");
                result.records.add(map);
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class SalesforceQueryResult {
        public int totalSize;
        public boolean done;
        public String nextRecordsUrl;
        public List<Map<String, Object>> records;
    }

    public static class SalesforceField {
        public String name;
        public String type;
        public boolean nillable;
    }
}
