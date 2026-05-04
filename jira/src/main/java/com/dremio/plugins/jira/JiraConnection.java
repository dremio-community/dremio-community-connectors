/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.jira;

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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class JiraConnection {

    private static final Logger logger = LoggerFactory.getLogger(JiraConnection.class);

    private static final String[] ISSUE_FIELDS = {
        "summary", "description", "status", "issuetype", "priority",
        "assignee", "reporter", "created", "updated", "resolutiondate", "duedate",
        "project", "labels", "components", "fixVersions", "parent",
        "timeoriginalestimate", "timespent", "customfield_10016", "comment"
    };

    private final JiraConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final String baseUrl;
    private final String authHeader;

    public JiraConnection(JiraConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.timeoutSeconds))
                .build();
        this.baseUrl = "https://" + conf.domain.replaceAll("\\.atlassian\\.net$", "") + ".atlassian.net";
        String credentials = conf.email + ":" + conf.apiToken;
        this.authHeader = "Basic " + Base64.getEncoder()
                .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
    }

    // -------------------------------------------------------------------------
    // Health check
    // -------------------------------------------------------------------------

    public void verifyConnection() throws IOException {
        getJson(baseUrl + "/rest/api/3/myself");
        logger.info("Jira connection verified for {}", baseUrl);
    }

    // -------------------------------------------------------------------------
    // Paginated tables: issues, projects, users, boards
    // -------------------------------------------------------------------------

    public JiraPage fetchPage(String tableName, int startAt) throws IOException {
        switch (tableName) {
            case "projects": return fetchProjectsPage(startAt);
            case "users":    return fetchUsersPage(startAt);
            case "boards":   return fetchBoardsPage(startAt);
            default: throw new IllegalArgumentException("Not an offset-paginated table: " + tableName);
        }
    }

    /** Fetch a page of issues using cursor-based pagination (new /search/jql endpoint). */
    public JiraPage fetchIssuesPage(String cursor) throws IOException {
        String url = baseUrl + "/rest/api/3/search/jql";
        ObjectNode body = mapper.createObjectNode();
        // Ensure JQL has a restriction — unbounded queries are rejected by the new endpoint
        String jql = conf.issueJql;
        if (!jql.toLowerCase().contains("project") && !jql.toLowerCase().contains("=")
                && !jql.toLowerCase().contains("!=") && !jql.toLowerCase().contains(" in ")) {
            jql = "project IS NOT EMPTY " + (jql.isEmpty() ? "ORDER BY created ASC" : jql);
        }
        body.put("jql", jql);
        body.put("maxResults", Math.min(conf.pageSize, 100));
        if (cursor != null) body.put("nextPageToken", cursor);
        ArrayNode fields = body.putArray("fields");
        for (String f : ISSUE_FIELDS) fields.add(f);

        JsonNode root = postJson(url, mapper.writeValueAsString(body));
        JiraPage page = new JiraPage();
        page.records = toList(root.get("issues"));
        page.isLast = root.path("isLast").asBoolean(page.records.isEmpty());
        page.nextPageToken = root.path("nextPageToken").asText(null);
        if (page.nextPageToken != null && page.nextPageToken.isEmpty()) page.nextPageToken = null;
        return page;
    }

    private JiraPage fetchProjectsPage(int startAt) throws IOException {
        int ps = Math.min(conf.pageSize, 50);
        JsonNode root = getJson(baseUrl + "/rest/api/3/project/search?startAt=" + startAt + "&maxResults=" + ps);
        JiraPage page = new JiraPage();
        page.total = root.path("total").asLong(0);
        page.startAt = startAt;
        page.records = toList(root.get("values"));
        page.isLast = root.path("isLast").asBoolean(true);
        return page;
    }

    private JiraPage fetchUsersPage(int startAt) throws IOException {
        int ps = Math.min(conf.pageSize, 50);
        JsonNode root = getJson(baseUrl + "/rest/api/3/users/search?startAt=" + startAt + "&maxResults=" + ps);
        JiraPage page = new JiraPage();
        page.records = toList(root);
        page.isLast = page.records.size() < ps;
        page.total = -1;
        page.startAt = startAt;
        return page;
    }

    private JiraPage fetchBoardsPage(int startAt) throws IOException {
        int ps = Math.min(conf.pageSize, 50);
        JsonNode root = getJson(baseUrl + "/rest/agile/1.0/board?startAt=" + startAt + "&maxResults=" + ps);
        JiraPage page = new JiraPage();
        page.total = root.path("total").asLong(0);
        page.startAt = startAt;
        page.records = toList(root.get("values"));
        page.isLast = root.path("isLast").asBoolean(true);
        return page;
    }

    // -------------------------------------------------------------------------
    // Single-fetch tables
    // -------------------------------------------------------------------------

    public List<JsonNode> fetchAll(String tableName) throws IOException {
        switch (tableName) {
            case "priorities":  return toList(getJson(baseUrl + "/rest/api/3/priority"));
            case "issue_types": return toList(getJson(baseUrl + "/rest/api/3/issuetype"));
            case "statuses":    return toList(getJson(baseUrl + "/rest/api/3/status"));
            case "fields":      return toList(getJson(baseUrl + "/rest/api/3/field"));
            case "components":  return fetchAllComponents();
            case "versions":    return fetchAllVersions();
            default: throw new IllegalArgumentException("Not a single-fetch table: " + tableName);
        }
    }

    private List<JsonNode> fetchAllComponents() throws IOException {
        List<JsonNode> all = new ArrayList<>();
        for (String key : fetchAllProjectKeys()) {
            try {
                JsonNode arr = getJson(baseUrl + "/rest/api/3/project/" + key + "/components");
                for (JsonNode c : arr) {
                    ((ObjectNode) c).put("projectKey", key);
                    all.add(c);
                }
            } catch (IOException e) {
                logger.warn("Could not fetch components for project {}: {}", key, e.getMessage());
            }
        }
        return all;
    }

    private List<JsonNode> fetchAllVersions() throws IOException {
        List<JsonNode> all = new ArrayList<>();
        for (String key : fetchAllProjectKeys()) {
            try {
                JsonNode arr = getJson(baseUrl + "/rest/api/3/project/" + key + "/versions");
                for (JsonNode v : arr) {
                    ((ObjectNode) v).put("projectKey", key);
                    all.add(v);
                }
            } catch (IOException e) {
                logger.warn("Could not fetch versions for project {}: {}", key, e.getMessage());
            }
        }
        return all;
    }

    public List<String> fetchAllProjectKeys() throws IOException {
        List<String> keys = new ArrayList<>();
        int startAt = 0;
        while (true) {
            JiraPage page = fetchProjectsPage(startAt);
            for (JsonNode p : page.records) {
                String k = p.path("key").asText(null);
                if (k != null) keys.add(k);
            }
            if (page.isLast || page.records.isEmpty()) break;
            startAt += page.records.size();
        }
        return keys;
    }

    // -------------------------------------------------------------------------
    // Row count estimation
    // -------------------------------------------------------------------------

    public long estimateCount(String tableName) {
        try {
            switch (tableName) {
                case "issues": {
                    // new /search/jql has no total; fetch first page and estimate from record count
                    JiraPage p = fetchIssuesPage(null);
                    return p.isLast ? p.records.size() : Math.max(p.records.size() * 10L, 100L);
                }
                case "projects": {
                    JsonNode root = getJson(baseUrl + "/rest/api/3/project/search?maxResults=1");
                    return root.path("total").asLong(50L);
                }
                case "boards": {
                    JsonNode root = getJson(baseUrl + "/rest/agile/1.0/board?maxResults=1");
                    return root.path("total").asLong(20L);
                }
                case "users":       return 500L;
                case "priorities":  return 10L;
                case "issue_types": return 20L;
                case "statuses":    return 30L;
                case "fields":      return 100L;
                case "components":  return 100L;
                case "versions":    return 50L;
                default:            return 100L;
            }
        } catch (Exception e) {
            logger.warn("Could not estimate row count for {}: {}", tableName, e.getMessage());
            return 1000L;
        }
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private JsonNode getJson(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.timeoutSeconds))
                .GET()
                .build();
        return execute(request, url);
    }

    private JsonNode postJson(String url, String body) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader)
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.timeoutSeconds))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return execute(request, url);
    }

    private JsonNode execute(HttpRequest request, String url) throws IOException {
        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Jira API request to " + url, e);
        }
        int code = response.statusCode();
        if (code == 401) throw new IOException("Jira authentication failed (401) — check email and API token");
        if (code == 403) throw new IOException("Jira permission denied (403) at " + url);
        if (code < 200 || code >= 300) {
            throw new IOException("Jira API error HTTP " + code + " at " + url + ": " + response.body());
        }
        return mapper.readTree(response.body());
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private List<JsonNode> toList(JsonNode node) {
        List<JsonNode> list = new ArrayList<>();
        if (node != null && node.isArray()) {
            for (JsonNode n : node) list.add(n);
        }
        return list;
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class JiraPage {
        public List<JsonNode> records;
        public long total;
        public int startAt;
        public boolean isLast;
        public String nextPageToken; // cursor for issues (new /search/jql endpoint)
    }
}
