/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package com.dremio.plugins.pinecone;

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

/**
 * HTTP client for the Pinecone REST API.
 *
 * <p>Auth: Api-Key header. Two planes:
 * <ul>
 *   <li>Control plane ({@code controlPlaneUrl}): list indexes</li>
 *   <li>Data plane (per-index host): list vector IDs, fetch vectors</li>
 * </ul>
 */
public class PineconeConnection {

    private static final Logger logger = LoggerFactory.getLogger(PineconeConnection.class);

    /** Describes a Pinecone index returned by the control plane. */
    public static class IndexInfo {
        public final String name;
        public final String host;

        public IndexInfo(String name, String host) {
            this.name = name;
            this.host = host;
        }

        @Override
        public String toString() {
            return "IndexInfo{name='" + name + "', host='" + host + "'}";
        }
    }

    private final PineconeConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;

    public PineconeConnection(PineconeConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
    }

    /**
     * Lists all Pinecone indexes from the control plane.
     *
     * @return list of IndexInfo objects (name + host)
     */
    public List<IndexInfo> listIndexes() throws IOException {
        String url = controlBase() + "/indexes";
        JsonNode root = getJson(url);
        List<IndexInfo> indexes = new ArrayList<>();
        JsonNode arr = root.path("indexes");
        if (arr.isArray()) {
            for (JsonNode idx : arr) {
                String name = idx.path("name").asText();
                String host = idx.path("host").asText();
                // Ensure host has a scheme
                if (!host.startsWith("http://") && !host.startsWith("https://")) {
                    host = "https://" + host;
                }
                indexes.add(new IndexInfo(name, host));
            }
        }
        logger.debug("Found {} Pinecone indexes", indexes.size());
        return indexes;
    }

    /**
     * Lists vector IDs from a data-plane host.
     *
     * @param host            the index data-plane URL (e.g. https://products-xyz.svc.pinecone.io)
     * @param namespace       namespace to query (empty = default)
     * @param limit           max IDs to return
     * @param paginationToken token from a previous call, or null for first page
     * @return list of vector ID strings
     */
    public List<String> listVectorIds(String host, String namespace, int limit,
            String paginationToken) throws IOException {
        return listVectorIds(host, namespace, limit, paginationToken, null);
    }

    public List<String> listVectorIds(String host, String namespace, int limit,
            String paginationToken, String indexName) throws IOException {
        String url = buildListUrl(host, namespace, limit, paginationToken, indexName);
        JsonNode root = getJson(url);
        List<String> ids = new ArrayList<>();
        JsonNode vectors = root.path("vectors");
        if (vectors.isArray()) {
            for (JsonNode v : vectors) {
                String id = v.path("id").asText(null);
                if (id != null) ids.add(id);
            }
        }
        return ids;
    }

    /**
     * Returns the pagination token for the next page, or null if no more pages.
     */
    public String fetchNextToken(String host, String namespace, int limit,
            String paginationToken) throws IOException {
        return fetchNextToken(host, namespace, limit, paginationToken, null);
    }

    public String fetchNextToken(String host, String namespace, int limit,
            String paginationToken, String indexName) throws IOException {
        String url = buildListUrl(host, namespace, limit, paginationToken, indexName);
        JsonNode root = getJson(url);
        JsonNode pagination = root.path("pagination");
        if (pagination.isObject()) {
            JsonNode next = pagination.path("next");
            if (!next.isMissingNode() && !next.isNull()) {
                return next.asText(null);
            }
        }
        return null;
    }

    /**
     * Fetches full vector data (values + metadata) for the given IDs.
     *
     * @param host      the index data-plane URL
     * @param ids       list of vector IDs to fetch
     * @param namespace namespace to query
     * @return map of id → JsonNode (the full vector object)
     */
    public Map<String, JsonNode> fetchVectors(String host, List<String> ids,
            String namespace) throws IOException {
        return fetchVectors(host, ids, namespace, null);
    }

    public Map<String, JsonNode> fetchVectors(String host, List<String> ids,
            String namespace, String indexName) throws IOException {
        if (ids == null || ids.isEmpty()) return new HashMap<>();

        StringBuilder url = new StringBuilder(host.replaceAll("/$", ""))
                .append("/vectors/fetch?");
        for (int i = 0; i < ids.size(); i++) {
            if (i > 0) url.append("&");
            url.append("ids=").append(URLEncoder.encode(ids.get(i), StandardCharsets.UTF_8));
        }
        if (namespace != null && !namespace.isEmpty()) {
            url.append("&namespace=").append(URLEncoder.encode(namespace, StandardCharsets.UTF_8));
        } else {
            url.append("&namespace=");
        }
        if (indexName != null && !indexName.isEmpty()) {
            url.append("&index=").append(URLEncoder.encode(indexName, StandardCharsets.UTF_8));
        }

        JsonNode root = getJson(url.toString());
        Map<String, JsonNode> result = new HashMap<>();
        JsonNode vectors = root.path("vectors");
        if (vectors.isObject()) {
            vectors.fields().forEachRemaining(entry -> result.put(entry.getKey(), entry.getValue()));
        }
        return result;
    }

    /**
     * Samples up to {@code sampleSize} vectors from an index for schema inference.
     */
    public List<JsonNode> sampleVectors(String host, String namespace, int sampleSize)
            throws IOException {
        return sampleVectors(host, namespace, sampleSize, null);
    }

    public List<JsonNode> sampleVectors(String host, String namespace, int sampleSize,
            String indexName) throws IOException {
        List<String> ids = listVectorIds(host, namespace, Math.min(sampleSize, 100), null, indexName);
        if (ids.isEmpty()) return new ArrayList<>();

        if (ids.size() > sampleSize) {
            ids = ids.subList(0, sampleSize);
        }

        Map<String, JsonNode> vectorMap = fetchVectors(host, ids, namespace, indexName);
        List<JsonNode> vectors = new ArrayList<>(vectorMap.values());
        logger.debug("Sampled {} vectors from host '{}' index '{}'", vectors.size(), host, indexName);
        return vectors;
    }

    /**
     * Verifies connectivity by listing indexes. Throws if the call fails.
     */
    public void verify() throws IOException {
        List<IndexInfo> indexes = listIndexes();
        logger.info("Pinecone connection verified; found {} index(es)", indexes.size());
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private String buildListUrl(String host, String namespace, int limit, String paginationToken) {
        return buildListUrl(host, namespace, limit, paginationToken, null);
    }

    private String buildListUrl(String host, String namespace, int limit,
            String paginationToken, String indexName) {
        StringBuilder url = new StringBuilder(host.replaceAll("/$", ""))
                .append("/vectors/list?limit=").append(limit);
        if (namespace != null && !namespace.isEmpty()) {
            url.append("&namespace=").append(URLEncoder.encode(namespace, StandardCharsets.UTF_8));
        } else {
            url.append("&namespace=");
        }
        if (paginationToken != null && !paginationToken.isEmpty()) {
            url.append("&paginationToken=")
               .append(URLEncoder.encode(paginationToken, StandardCharsets.UTF_8));
        }
        if (indexName != null && !indexName.isEmpty()) {
            url.append("&index=").append(URLEncoder.encode(indexName, StandardCharsets.UTF_8));
        }
        return url.toString();
    }

    private JsonNode getJson(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Api-Key", conf.apiKey)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during Pinecone API request to " + url, e);
        }

        if (response.statusCode() == 401) {
            throw new IOException("Pinecone authentication failed (401) — check your API key");
        }
        if (response.statusCode() == 403) {
            throw new IOException("Pinecone permission denied (403) at " + url);
        }
        if (response.statusCode() == 404) {
            throw new IOException("Pinecone resource not found (404) at " + url);
        }
        if (response.statusCode() == 429) {
            // Rate limited — back off briefly and retry once
            try { Thread.sleep(1000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            try {
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted during Pinecone retry", ie);
            }
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("Pinecone API error (HTTP " + response.statusCode() + ") at "
                    + url + ": " + response.body());
        }

        return mapper.readTree(response.body());
    }

    private String controlBase() {
        return conf.controlPlaneUrl.replaceAll("/$", "");
    }
}
