/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.stripe;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * HTTP client for the Stripe REST API v1.
 *
 * <p>Auth: Bearer token (secret API key). Pagination: cursor-based via
 * {@code starting_after} parameter (the {@code id} of the last item).
 */
public class StripeConnection {

    private static final Logger logger = LoggerFactory.getLogger(StripeConnection.class);

    /** All tables exposed as Dremio datasets. */
    public static final List<String> TABLES = Collections.unmodifiableList(Arrays.asList(
            "charges",
            "customers",
            "subscriptions",
            "invoices",
            "payment_intents",
            "products",
            "prices",
            "refunds",
            "balance_transactions"
    ));

    /** Stripe API path for each table name. */
    static String apiPath(String table) {
        switch (table) {
            case "payment_intents":      return "payment_intents";
            case "balance_transactions": return "balance_transactions";
            default:                     return table;
        }
    }

    private final StripeConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;

    public StripeConnection(StripeConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
    }

    /** Verifies the API key by listing one charge. */
    public void verify() throws IOException {
        String url = base() + "/v1/charges?limit=1";
        getJson(url);
        logger.info("Stripe API key verified; connected to {}", conf.baseUrl);
    }

    /**
     * Fetches one page of records for the given table.
     *
     * @param table       table name (e.g. "charges")
     * @param startAfter  id of the last item from the previous page, or null for first page
     */
    public StripePage fetchPage(String table, String startAfter) throws IOException {
        StringBuilder url = new StringBuilder(base())
                .append("/v1/").append(apiPath(table))
                .append("?limit=").append(Math.min(conf.pageSize, 100));
        if (startAfter != null) {
            url.append("&starting_after=").append(URLEncoder.encode(startAfter, StandardCharsets.UTF_8));
        }

        JsonNode root = getJson(url.toString());
        return parsePage(root);
    }

    /** Returns a rough row count estimate for cost planning. */
    public long estimateCount(String table) {
        try {
            String url = base() + "/v1/" + apiPath(table) + "?limit=1";
            JsonNode root = getJson(url);
            // Stripe doesn't return total_count in list responses; use a fixed estimate
            // unless the first page already signals has_more=false (meaning very small table)
            boolean hasMore = root.path("has_more").asBoolean(false);
            int firstPageSize = root.path("data").size();
            if (!hasMore) return firstPageSize;
            return 1000L; // conservative estimate for cost planning
        } catch (Exception e) {
            logger.warn("Could not estimate count for {}: {}", table, e.getMessage());
            return 1000L;
        }
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private JsonNode getJson(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + conf.apiKey)
                .header("Accept", "application/json")
                .header("Stripe-Version", "2023-10-16")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Stripe API request to " + url, e);
        }

        if (response.statusCode() == 401) {
            throw new IOException("Stripe authentication failed (401) — check your API key");
        }
        if (response.statusCode() == 403) {
            throw new IOException("Stripe permission denied (403) at " + url);
        }
        if (response.statusCode() == 429) {
            // Rate limited — back off briefly and retry once
            try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during Stripe retry", ie);
            }
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Stripe API error (HTTP " + response.statusCode() + ") at "
                    + url + ": " + response.body());
        }

        return mapper.readTree(response.body());
    }

    private StripePage parsePage(JsonNode root) {
        StripePage page = new StripePage();
        page.hasMore = root.path("has_more").asBoolean(false);

        JsonNode data = root.get("data");
        page.records = new ArrayList<>();
        if (data != null && data.isArray()) {
            for (JsonNode rec : data) {
                page.records.add(rec);
            }
        }

        // Cursor for next page = id of the last item
        if (page.hasMore && !page.records.isEmpty()) {
            JsonNode last = page.records.get(page.records.size() - 1);
            page.nextCursor = last.path("id").asText(null);
        }

        return page;
    }

    private String base() {
        return conf.baseUrl.replaceAll("/$", "");
    }

    // -------------------------------------------------------------------------
    // Response type
    // -------------------------------------------------------------------------

    public static class StripePage {
        public boolean hasMore;
        public List<JsonNode> records;
        public String nextCursor; // null if no more pages
    }
}
