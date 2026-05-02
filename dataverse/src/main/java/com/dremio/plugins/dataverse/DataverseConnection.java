/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.dataverse;

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
 * Manages Azure AD OAuth2 authentication and OData v4 REST API calls to Microsoft Dataverse.
 * Thread-safe: authenticate() is synchronized.
 */
public class DataverseConnection {

    private static final Logger logger = LoggerFactory.getLogger(DataverseConnection.class);

    private final DataverseConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;

    private volatile String accessToken;

    public DataverseConnection(DataverseConf conf) {
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
     * Performs Azure AD OAuth2 client credentials flow.
     * Token URL: https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token
     * Scope: {organizationUrl}/.default
     */
    public synchronized void authenticate() throws IOException {
        logger.info("Authenticating to Azure AD for Dataverse org: {}", conf.organizationUrl);

        String orgUrl = conf.organizationUrl.endsWith("/")
                ? conf.organizationUrl.substring(0, conf.organizationUrl.length() - 1)
                : conf.organizationUrl;
        String scope = orgUrl + "/.default";
        String tokenUrl = "https://login.microsoftonline.com/" + conf.tenantId + "/oauth2/v2.0/token";

        StringJoiner body = new StringJoiner("&");
        body.add("grant_type=client_credentials");
        body.add("client_id=" + URLEncoder.encode(conf.clientId, StandardCharsets.UTF_8));
        body.add("client_secret=" + URLEncoder.encode(conf.clientSecret, StandardCharsets.UTF_8));
        body.add("scope=" + URLEncoder.encode(scope, StandardCharsets.UTF_8));

        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(tokenUrl))
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Azure AD authentication", e);
        }

        if (response.statusCode() != 200) {
            throw new IOException("Azure AD authentication failed (HTTP " + response.statusCode()
                    + "): " + response.body());
        }

        JsonNode json = mapper.readTree(response.body());
        if (json.has("error")) {
            throw new IOException("Azure AD OAuth error: " + json.get("error").asText()
                    + " - " + json.path("error_description").asText());
        }

        this.accessToken = json.get("access_token").asText();
        logger.info("Authenticated to Dataverse: {}", orgUrl);
    }

    public void ensureAuthenticated() throws IOException {
        if (accessToken == null) {
            authenticate();
        }
    }

    // -------------------------------------------------------------------------
    // Base URL
    // -------------------------------------------------------------------------

    public String getBaseUrl() {
        String org = conf.organizationUrl;
        if (!org.endsWith("/")) org += "/";
        return org + "api/data/v" + conf.apiVersion + "/";
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private String getJson(String url) throws IOException {
        ensureAuthenticated();
        HttpResponse<String> response = executeGet(url);
        if (response.statusCode() == 401) {
            logger.warn("Got 401 from Dataverse, re-authenticating");
            authenticate();
            response = executeGet(url);
        }
        if (response.statusCode() != 200) {
            throw new IOException("Dataverse API error (HTTP " + response.statusCode()
                    + ") at " + url + ": " + response.body());
        }
        return response.body();
    }

    private HttpResponse<String> executeGet(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url.replace(" ", "%20")))
                .header("Authorization", "Bearer " + accessToken)
                .header("Accept", "application/json")
                .header("OData-MaxVersion", "4.0")
                .header("OData-Version", "4.0")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();
        try {
            return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Dataverse HTTP request", e);
        }
    }

    // -------------------------------------------------------------------------
    // Metadata APIs
    // -------------------------------------------------------------------------

    /**
     * Lists all queryable entities (tables) from Dataverse.
     * Returns a list of DataverseEntity descriptors with logical name + entity set name.
     */
    public List<DataverseEntity> listEntities() throws IOException {
        String url = getBaseUrl()
                + "EntityDefinitions?$select=LogicalName,EntitySetName,DisplayName"
                + "&$filter=IsValidForAdvancedFind eq true and IsCustomizable/Value eq true or IsValidForAdvancedFind eq true";
        // Simpler filter — just IsValidForAdvancedFind
        url = getBaseUrl()
                + "EntityDefinitions?$select=LogicalName,EntitySetName,DisplayName"
                + "&$filter=IsValidForAdvancedFind eq true";

        String json = getJson(url);
        JsonNode root = mapper.readTree(json);
        JsonNode value = root.get("value");

        List<DataverseEntity> entities = new ArrayList<>();
        if (value != null && value.isArray()) {
            for (JsonNode node : value) {
                DataverseEntity entity = new DataverseEntity();
                entity.logicalName = node.path("LogicalName").asText();
                entity.entitySetName = node.path("EntitySetName").asText();
                JsonNode displayName = node.path("DisplayName").path("UserLocalizedLabel");
                entity.displayName = displayName.isMissingNode()
                        ? entity.logicalName
                        : displayName.path("Label").asText(entity.logicalName);
                if (!entity.logicalName.isEmpty() && !entity.entitySetName.isEmpty()) {
                    entities.add(entity);
                }
            }
        }
        logger.debug("Listed {} Dataverse entities", entities.size());
        return entities;
    }

    /**
     * Returns the field descriptors for the given entity logical name.
     * Skips Virtual and ManagedProperty attribute types (not queryable).
     */
    public List<DataverseField> describeEntity(String logicalName) throws IOException {
        String url = getBaseUrl()
                + "EntityDefinitions(LogicalName='" + logicalName + "')"
                + "/Attributes?$select=LogicalName,AttributeType,DisplayName";

        String json = getJson(url);
        JsonNode root = mapper.readTree(json);
        JsonNode value = root.get("value");

        List<DataverseField> fields = new ArrayList<>();
        if (value != null && value.isArray()) {
            for (JsonNode node : value) {
                String attrType = node.path("AttributeType").asText("");
                // Skip non-queryable attribute types
                if (attrType.equals("Virtual") || attrType.equals("ManagedProperty")
                        || attrType.equals("CalendarRules") || attrType.equals("EntityNameMap")) {
                    continue;
                }
                DataverseField field = new DataverseField();
                field.logicalName = node.path("LogicalName").asText();
                field.attributeType = attrType;
                JsonNode label = node.path("DisplayName").path("UserLocalizedLabel");
                field.displayName = label.isMissingNode()
                        ? field.logicalName
                        : label.path("Label").asText(field.logicalName);
                if (!field.logicalName.isEmpty()) {
                    fields.add(field);
                }
            }
        }
        return fields;
    }

    // -------------------------------------------------------------------------
    // Query APIs
    // -------------------------------------------------------------------------

    /**
     * Executes an OData GET request and returns the first page of results.
     */
    public DataverseQueryResult query(String url) throws IOException {
        String json = getJson(url);
        return parseQueryResult(json);
    }

    @SuppressWarnings("unchecked")
    private DataverseQueryResult parseQueryResult(String json) throws IOException {
        JsonNode root = mapper.readTree(json);

        DataverseQueryResult result = new DataverseQueryResult();
        result.nextLink = root.has("@odata.nextLink") ? root.get("@odata.nextLink").asText() : null;

        JsonNode valueNode = root.get("value");
        result.records = new ArrayList<>();
        if (valueNode != null && valueNode.isArray()) {
            for (JsonNode rec : valueNode) {
                Map<String, Object> map = mapper.convertValue(rec, Map.class);
                // Remove OData metadata fields
                map.entrySet().removeIf(e -> e.getKey().startsWith("@"));
                result.records.add(map);
            }
        }
        return result;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class DataverseQueryResult {
        public String nextLink;
        public List<Map<String, Object>> records;
    }

    public static class DataverseEntity {
        public String logicalName;
        public String entitySetName;
        public String displayName;
    }

    public static class DataverseField {
        public String logicalName;
        public String attributeType;
        public String displayName;
    }
}
