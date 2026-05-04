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
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class PrometheusConnection {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusConnection.class);

    private final PrometheusConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final String baseUrl;
    private final String authHeader;

    public PrometheusConnection(PrometheusConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.timeoutSeconds))
                .build();
        String url = conf.url;
        if (url == null || url.isBlank()) url = "http://localhost:9090";
        this.baseUrl = url.replaceAll("/+$", "");

        if (conf.bearerToken != null && !conf.bearerToken.isBlank()) {
            this.authHeader = "Bearer " + conf.bearerToken.trim();
        } else if (conf.username != null && !conf.username.isBlank()) {
            String creds = conf.username + ":" + (conf.password != null ? conf.password : "");
            this.authHeader = "Basic " + Base64.getEncoder()
                    .encodeToString(creds.getBytes(StandardCharsets.UTF_8));
        } else {
            this.authHeader = null;
        }
    }

    // -------------------------------------------------------------------------
    // Health check
    // -------------------------------------------------------------------------

    public void verifyConnection() throws IOException {
        getJson(baseUrl + "/api/v1/labels");
        logger.info("Prometheus connection verified for {}", baseUrl);
    }

    // -------------------------------------------------------------------------
    // Table fetchers
    // -------------------------------------------------------------------------

    public List<JsonNode> fetchMetrics() throws IOException {
        JsonNode root = getJson(baseUrl + "/api/v1/metadata");
        checkStatus(root);
        JsonNode data = root.path("data");
        List<JsonNode> rows = new ArrayList<>();
        data.fields().forEachRemaining(entry -> {
            String metricName = entry.getKey();
            for (JsonNode meta : entry.getValue()) {
                PrometheusRow row = new PrometheusRow();
                row.metricName = metricName;
                row.type       = meta.path("type").asText(null);
                row.help       = meta.path("help").asText(null);
                row.unit       = meta.path("unit").asText(null);
                rows.add(mapper.valueToTree(row));
            }
        });
        return rows;
    }

    public List<JsonNode> fetchTargets() throws IOException {
        JsonNode root = getJson(baseUrl + "/api/v1/targets");
        checkStatus(root);
        List<JsonNode> rows = new ArrayList<>();
        for (JsonNode target : root.path("data").path("activeTargets")) {
            PrometheusRow row = new PrometheusRow();
            row.job                       = target.path("labels").path("job").asText(null);
            row.instance                  = target.path("labels").path("instance").asText(null);
            row.health                    = target.path("health").asText(null);
            row.lastScrape                = target.path("lastScrape").asText(null);
            row.lastScrapeDurationSeconds = target.path("lastScrapeDurationSeconds").asDouble(Double.NaN);
            row.lastError                 = target.path("lastError").asText(null);
            row.labels                    = target.path("labels").toString();
            row.scrapeUrl                 = target.path("scrapeUrl").asText(null);
            rows.add(mapper.valueToTree(row));
        }
        return rows;
    }

    public List<JsonNode> fetchAlerts() throws IOException {
        JsonNode root = getJson(baseUrl + "/api/v1/alerts");
        checkStatus(root);
        List<JsonNode> rows = new ArrayList<>();
        for (JsonNode alert : root.path("data").path("alerts")) {
            PrometheusRow row = new PrometheusRow();
            row.alertName    = alert.path("labels").path("alertname").asText(null);
            row.state        = alert.path("state").asText(null);
            row.value        = parseDouble(alert.path("value").asText(null));
            row.labels       = alert.path("labels").toString();
            row.annotations  = alert.path("annotations").toString();
            row.activeAt     = alert.path("activeAt").asText(null);
            row.generatorUrl = alert.path("generatorURL").asText(null);
            rows.add(mapper.valueToTree(row));
        }
        return rows;
    }

    public List<JsonNode> fetchRules() throws IOException {
        JsonNode root = getJson(baseUrl + "/api/v1/rules");
        checkStatus(root);
        List<JsonNode> rows = new ArrayList<>();
        for (JsonNode group : root.path("data").path("groups")) {
            String groupName = group.path("name").asText(null);
            for (JsonNode rule : group.path("rules")) {
                PrometheusRow row = new PrometheusRow();
                row.groupName             = groupName;
                row.ruleName              = rule.path("name").asText(null);
                row.ruleType              = rule.path("type").asText(null);
                row.query                 = rule.path("query").asText(
                                            rule.path("expr").asText(null));
                row.health                = rule.path("health").asText(null);
                row.lastEvaluation        = rule.path("lastEvaluation").asText(null);
                row.evaluationTimeSeconds = rule.path("evaluationTime").asDouble(Double.NaN);
                row.durationSeconds       = rule.path("duration").asDouble(Double.NaN);
                row.labels                = rule.path("labels").toString();
                row.annotations           = rule.path("annotations").toString();
                rows.add(mapper.valueToTree(row));
            }
        }
        return rows;
    }

    public List<JsonNode> fetchLabels() throws IOException {
        JsonNode root = getJson(baseUrl + "/api/v1/labels");
        checkStatus(root);
        List<JsonNode> rows = new ArrayList<>();
        for (JsonNode label : root.path("data")) {
            PrometheusRow row = new PrometheusRow();
            row.labelName = label.asText(null);
            rows.add(mapper.valueToTree(row));
        }
        return rows;
    }

    public List<JsonNode> fetchSamples() throws IOException {
        Instant end   = Instant.now();
        Instant start = end.minusSeconds((long) conf.lookbackHours * 3600L);
        int step      = Math.max(1, conf.stepSeconds);
        String query  = conf.samplesQuery != null && !conf.samplesQuery.isBlank()
                        ? conf.samplesQuery : "up";

        String url = baseUrl + "/api/v1/query_range"
                + "?query=" + URLEncoder.encode(query, StandardCharsets.UTF_8)
                + "&start=" + start.getEpochSecond()
                + "&end="   + end.getEpochSecond()
                + "&step="  + step;

        JsonNode root = getJson(url);
        checkStatus(root);

        List<JsonNode> rows = new ArrayList<>();
        for (JsonNode result : root.path("data").path("result")) {
            String metricName = result.path("metric").path("__name__").asText("unknown");
            String labels     = result.path("metric").toString();
            for (JsonNode point : result.path("values")) {
                if (!point.isArray() || point.size() < 2) continue;
                PrometheusRow row = new PrometheusRow();
                row.metricName = metricName;
                row.labels     = labels;
                row.timestamp  = Instant.ofEpochSecond(point.get(0).asLong()).toString();
                row.value      = parseDouble(point.get(1).asText(null));
                rows.add(mapper.valueToTree(row));
            }
        }
        return rows;
    }

    // -------------------------------------------------------------------------
    // Row count estimation
    // -------------------------------------------------------------------------

    public long estimateCount(String tableName) {
        try {
            switch (tableName) {
                case "metrics": {
                    JsonNode root = getJson(baseUrl + "/api/v1/metadata");
                    return root.path("data").size();
                }
                case "targets": {
                    JsonNode root = getJson(baseUrl + "/api/v1/targets");
                    return root.path("data").path("activeTargets").size();
                }
                case "alerts": {
                    JsonNode root = getJson(baseUrl + "/api/v1/alerts");
                    return root.path("data").path("alerts").size();
                }
                case "labels":  return 50L;
                case "rules":   return 20L;
                case "samples": {
                    long pointsPerSeries = (long) conf.lookbackHours * 3600L / Math.max(1, conf.stepSeconds);
                    return Math.max(1L, pointsPerSeries * 10L);
                }
                default: return 100L;
            }
        } catch (Exception e) {
            logger.warn("Could not estimate row count for {}: {}", tableName, e.getMessage());
            return 100L;
        }
    }

    // -------------------------------------------------------------------------
    // HTTP helper
    // -------------------------------------------------------------------------

    private JsonNode getJson(String url) throws IOException {
        HttpRequest.Builder builder = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.timeoutSeconds))
                .GET();
        if (authHeader != null) builder.header("Authorization", authHeader);

        HttpResponse<String> response;
        try {
            response = httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Prometheus API request to " + url, e);
        }
        int code = response.statusCode();
        if (code == 401) throw new IOException("Prometheus authentication failed (401) — check credentials");
        if (code == 403) throw new IOException("Prometheus permission denied (403) at " + url);
        if (code < 200 || code >= 300) {
            throw new IOException("Prometheus API error HTTP " + code + " at " + url + ": " + response.body());
        }
        return mapper.readTree(response.body());
    }

    private void checkStatus(JsonNode root) throws IOException {
        String status = root.path("status").asText("");
        if (!"success".equals(status)) {
            String errorType = root.path("errorType").asText("");
            String error     = root.path("error").asText("unknown error");
            throw new IOException("Prometheus API returned status=" + status
                    + (errorType.isEmpty() ? "" : " [" + errorType + "]")
                    + ": " + error);
        }
    }

    private double parseDouble(String s) {
        if (s == null || s.isBlank()) return Double.NaN;
        try { return Double.parseDouble(s); }
        catch (NumberFormatException e) { return Double.NaN; }
    }

    // -------------------------------------------------------------------------
    // Internal DTO for Jackson serialisation
    // -------------------------------------------------------------------------

    @SuppressWarnings("unused")
    public static class PrometheusRow {
        // metrics
        public String metricName;
        public String type;
        public String help;
        public String unit;
        // targets
        public String job;
        public String instance;
        public String health;
        public String lastScrape;
        public Double lastScrapeDurationSeconds;
        public String lastError;
        public String scrapeUrl;
        // alerts
        public String alertName;
        public Double value;
        public String annotations;
        public String activeAt;
        public String generatorUrl;
        // rules
        public String groupName;
        public String ruleName;
        public String ruleType;
        public String query;
        public String lastEvaluation;
        public Double evaluationTimeSeconds;
        public Double durationSeconds;
        // labels
        public String labelName;
        // shared
        public String labels;
        public String state;
        // samples
        public String timestamp;
    }
}
