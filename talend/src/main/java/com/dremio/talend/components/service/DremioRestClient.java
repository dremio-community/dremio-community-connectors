package com.dremio.talend.components.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

/**
 * Minimal Dremio REST API client (Java 8, no external HTTP dependencies).
 * Handles SQL job submission and polling via /api/v3/sql and /api/v3/job/{id}.
 */
public class DremioRestClient {

    private final String baseUrl;
    private final String pat;

    public DremioRestClient(String host, int restPort, boolean ssl, String pat) {
        String scheme = ssl ? "https" : "http";
        this.baseUrl = scheme + "://" + host + ":" + restPort;
        this.pat = pat;
    }

    /** Submits a SQL statement and returns the Dremio job ID. */
    public String submitSql(String sql) throws IOException {
        URL url = new URL(baseUrl + "/api/v3/sql");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Authorization", "Bearer " + pat);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String body = "{\"sql\": " + toJsonString(sql) + "}";
        try (OutputStream os = conn.getOutputStream()) {
            os.write(body.getBytes(StandardCharsets.UTF_8));
        }

        int status = conn.getResponseCode();
        String response = readResponse(conn);
        if (status != 200) {
            throw new IOException("Dremio SQL submission failed (HTTP " + status + "): " + response);
        }

        String jobId = parseJsonField(response, "id");
        if (jobId == null) {
            throw new IOException("No job ID in Dremio response: " + response);
        }
        return jobId;
    }

    /**
     * Polls a Dremio job until it reaches a terminal state.
     * Returns the final jobState string ("COMPLETED", "FAILED", "CANCELED").
     * Throws IOException if the job fails/cancels or the timeout is exceeded.
     */
    public String pollJobUntilDone(String jobId, int pollIntervalMs, int timeoutSeconds)
            throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + (long) timeoutSeconds * 1000;
        while (System.currentTimeMillis() < deadline) {
            URL url = new URL(baseUrl + "/api/v3/job/" + jobId);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Authorization", "Bearer " + pat);

            String response = readResponse(conn);
            String jobState = parseJsonField(response, "jobState");

            if ("COMPLETED".equals(jobState)) {
                return jobState;
            }
            if ("FAILED".equals(jobState) || "CANCELED".equals(jobState)) {
                String msg = parseJsonField(response, "errorMessage");
                throw new IOException("Dremio job " + jobId + " " + jobState
                        + (msg != null ? ": " + msg : ""));
            }
            Thread.sleep(pollIntervalMs);
        }
        throw new IOException("Dremio job " + jobId + " timed out after " + timeoutSeconds + "s");
    }

    // -------------------------------------------------------------------------

    private String readResponse(HttpURLConnection conn) throws IOException {
        InputStream is;
        try {
            is = conn.getInputStream();
        } catch (IOException e) {
            is = conn.getErrorStream();
        }
        if (is == null) return "";
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) sb.append(line);
            return sb.toString();
        }
    }

    /** Escapes a Java string as a JSON string literal (with surrounding quotes). */
    static String toJsonString(String s) {
        return "\"" + s
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t")
                + "\"";
    }

    /** Naive single-field extractor — sufficient for the small, predictable Dremio API responses. */
    static String parseJsonField(String json, String field) {
        if (json == null) return null;
        String key = "\"" + field + "\"";
        int idx = json.indexOf(key);
        if (idx < 0) return null;
        int colon = json.indexOf(':', idx + key.length());
        if (colon < 0) return null;
        int start = colon + 1;
        while (start < json.length() && Character.isWhitespace(json.charAt(start))) start++;
        if (start >= json.length()) return null;
        if (json.charAt(start) == '"') {
            int end = json.indexOf('"', start + 1);
            return end >= 0 ? json.substring(start + 1, end) : null;
        }
        int end = start;
        while (end < json.length() && json.charAt(end) != ',' && json.charAt(end) != '}') end++;
        String val = json.substring(start, end).trim();
        return val.equals("null") ? null : val;
    }
}
