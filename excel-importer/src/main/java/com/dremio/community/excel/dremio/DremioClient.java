package com.dremio.community.excel.dremio;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * Thin client for the Dremio REST API.
 * Handles authentication, SQL submission, and job polling.
 * Uses Java 11's built-in HttpClient — no extra dependencies.
 */
public class DremioClient {

    private final String baseUrl;
    private final HttpClient http;
    private final ObjectMapper mapper;
    private String token;

    // Poll interval for job status
    private static final long POLL_INTERVAL_MS = 750;
    // Maximum wait time per job
    private static final long MAX_WAIT_MS = 5 * 60 * 1000;

    public DremioClient(String host, int port) {
        this.baseUrl = "http://" + host + ":" + port;
        this.http = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        this.mapper = new ObjectMapper();
    }

    /**
     * Authenticate and store the session token.
     */
    public void login(String username, String password) throws IOException, InterruptedException {
        ObjectNode body = mapper.createObjectNode();
        body.put("userName", username);
        body.put("password", password);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/apiv2/login"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .timeout(Duration.ofSeconds(15))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new DremioException("Login failed (HTTP " + resp.statusCode() + "): " + resp.body());
        }

        JsonNode node = mapper.readTree(resp.body());
        if (!node.has("token")) {
            throw new DremioException("Login response missing token: " + resp.body());
        }
        token = node.get("token").asText();
    }

    /**
     * Submit a SQL statement and return the Dremio job ID.
     */
    public String submitSql(String sql) throws IOException, InterruptedException {
        ensureLoggedIn();

        ObjectNode body = mapper.createObjectNode();
        body.put("sql", sql);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + "/api/v3/sql"))
                .header("Content-Type", "application/json")
                .header("Authorization", "_dremio" + token)
                .POST(HttpRequest.BodyPublishers.ofString(body.toString()))
                .timeout(Duration.ofSeconds(30))
                .build();

        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new DremioException("SQL submission failed (HTTP " + resp.statusCode() + "): " + resp.body());
        }

        JsonNode node = mapper.readTree(resp.body());
        if (!node.has("id")) {
            throw new DremioException("SQL response missing job ID: " + resp.body());
        }
        return node.get("id").asText();
    }

    /**
     * Poll until the job completes, is canceled, or fails.
     * Throws DremioException on failure.
     */
    public void waitForJob(String jobId) throws IOException, InterruptedException {
        ensureLoggedIn();
        long deadline = System.currentTimeMillis() + MAX_WAIT_MS;

        while (System.currentTimeMillis() < deadline) {
            Thread.sleep(POLL_INTERVAL_MS);

            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/api/v3/job/" + jobId))
                    .header("Authorization", "_dremio" + token)
                    .GET()
                    .timeout(Duration.ofSeconds(15))
                    .build();

            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() != 200) {
                throw new DremioException("Job poll failed (HTTP " + resp.statusCode() + "): " + resp.body());
            }

            JsonNode node = mapper.readTree(resp.body());
            String state = node.has("jobState") ? node.get("jobState").asText() : "UNKNOWN";

            switch (state) {
                case "COMPLETED":
                    return;
                case "FAILED":
                case "CANCELED": {
                    String errMsg = state;
                    if (node.has("errorMessage")) errMsg += ": " + node.get("errorMessage").asText();
                    else if (node.has("cancellationInfo")) errMsg += ": " + node.get("cancellationInfo").asText();
                    throw new DremioException("Job " + errMsg);
                }
                default:
                    // RUNNING, STARTING, PLANNING, QUEUED, etc — keep polling
            }
        }
        throw new DremioException("Job " + jobId + " timed out after " + (MAX_WAIT_MS / 1000) + "s");
    }

    /**
     * Submit SQL and wait for completion. Convenience wrapper.
     */
    public void executeAndWait(String sql) throws IOException, InterruptedException {
        String jobId = submitSql(sql);
        waitForJob(jobId);
    }

    private void ensureLoggedIn() {
        if (token == null) throw new IllegalStateException("Not logged in — call login() first");
    }

    public static class DremioException extends RuntimeException {
        public DremioException(String message) { super(message); }
    }
}
