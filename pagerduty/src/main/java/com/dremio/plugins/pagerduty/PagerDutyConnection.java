package com.dremio.plugins.pagerduty;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * PagerDuty REST API client using token authentication.
 *
 * <p>Each logical "table" maps to a PagerDuty API endpoint with a static schema.
 * Pagination uses offset-based pagination.
 */
public class PagerDutyConnection {

    private static final Logger logger = LoggerFactory.getLogger(PagerDutyConnection.class);

    private final PagerDutyConf conf;
    private final ObjectMapper mapper;
    private final HttpClient httpClient;
    private final String baseUrl;
    private final String authHeader;

    // -------------------------------------------------------------------------
    // Static table registry
    // -------------------------------------------------------------------------

    public static class PagerDutyTable {
        public final String name;
        public final String endpoint;   // e.g. "/incidents?limit=100"
        public final String recordKey;  // JSON key holding the array, e.g. "incidents"
        public final List<PagerDutyField> fields;

        public PagerDutyTable(String name, String endpoint, String recordKey, List<PagerDutyField> fields) {
            this.name = name;
            this.endpoint = endpoint;
            this.recordKey = recordKey;
            this.fields = fields;
        }
    }

    public static class PagerDutyField {
        public final String name;       // Arrow column name
        public final String jsonPath;   // dot-separated path, e.g. "service.id"
        public final ArrowType type;

        public PagerDutyField(String name, String jsonPath, ArrowType type) {
            this.name = name;
            this.jsonPath = jsonPath;
            this.type = type;
        }

        public PagerDutyField(String name, ArrowType type) {
            this(name, name, type);
        }

        public Field toArrowField() {
            return new Field(name, new FieldType(true, type, null), Collections.emptyList());
        }
    }

    private static final Map<String, PagerDutyTable> TABLES = buildTables();

    private static Map<String, PagerDutyTable> buildTables() {
        Map<String, PagerDutyTable> map = new LinkedHashMap<>();

        // ── incidents ──────────────────────────────────────────────────────────
        map.put("incidents", new PagerDutyTable("incidents",
                "/incidents?limit=100&sortBy=created_at:desc",
                "incidents",
                Arrays.asList(
                        new PagerDutyField("id",                      ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("incident_number",         new ArrowType.Int(32, true)),
                        new PagerDutyField("title",                   ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("status",                  ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("urgency",                 ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("created_at",              new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new PagerDutyField("html_url",                ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("service_id",              "service.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("service_name",            "service.summary", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("assignee_id",             "assignments[0].assignee.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("assignee_name",           "assignments[0].assignee.summary", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_policy_id",     "escalation_policy.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_policy_name",   "escalation_policy.summary", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("team_ids",                "teams[*].id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("team_names",              "teams[*].summary", ArrowType.Utf8.INSTANCE)
                )));

        // ── services ─────────────────────────────────────────────────────────────
        map.put("services", new PagerDutyTable("services",
                "/services?limit=100",
                "services",
                Arrays.asList(
                        new PagerDutyField("id",                      ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("name",                    ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("description",             ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("status",                  ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("created_at",              new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new PagerDutyField("html_url",                ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_policy_id",     "escalation_policy.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_policy_name",   "escalation_policy.summary", ArrowType.Utf8.INSTANCE)
                )));

        // ── users ─────────────────────────────────────────────────────────────
        map.put("users", new PagerDutyTable("users",
                "/users?limit=100",
                "users",
                Arrays.asList(
                        new PagerDutyField("id",                      ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("name",                    ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("email",                   ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("role",                    ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("time_zone",               ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("avatar_url",              ArrowType.Utf8.INSTANCE)
                )));

        // ── oncalls ─────────────────────────────────────────────────────────────
        map.put("oncalls", new PagerDutyTable("oncalls",
                "/oncalls?limit=100",
                "oncalls",
                Arrays.asList(
                        new PagerDutyField("user_id",                 "user.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("user_name",               "user.summary", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("schedule_id",             "schedule.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("schedule_name",           "schedule.summary", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_policy_id",     "escalation_policy.id", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_policy_name",   "escalation_policy.summary", ArrowType.Utf8.INSTANCE),
                        new PagerDutyField("escalation_level",         new ArrowType.Int(32, true)),
                        new PagerDutyField("start",                   new ArrowType.Timestamp(TimeUnit.MILLISECOND, null)),
                        new PagerDutyField("end",                     new ArrowType.Timestamp(TimeUnit.MILLISECOND, null))
                )));

        return Collections.unmodifiableMap(map);
    }

    // -------------------------------------------------------------------------
    // Constructor
    // -------------------------------------------------------------------------

    public PagerDutyConnection(PagerDutyConf conf) {
        this.conf = conf;
        this.mapper = new ObjectMapper();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .build();
        this.baseUrl = conf.euRegion ? "https://api.eu.pagerduty.com" : "https://api.pagerduty.com";
        this.authHeader = conf.apiToken.startsWith("Bearer ") ? conf.apiToken : "Token token=" + conf.apiToken;
    }

    // -------------------------------------------------------------------------
    // Table registry
    // -------------------------------------------------------------------------

    public static Map<String, PagerDutyTable> getTables() {
        return TABLES;
    }

    public static PagerDutyTable getTable(String name) {
        PagerDutyTable t = TABLES.get(name.toLowerCase());
        if (t == null) {
            throw new IllegalArgumentException("Unknown PagerDuty table: " + name);
        }
        return t;
    }

    // -------------------------------------------------------------------------
    // API
    // -------------------------------------------------------------------------

    /** Validates connectivity by fetching users with a limit of 1. */
    public void testConnection() throws IOException {
        getJson(baseUrl + "/users?limit=1");
        logger.info("PagerDuty connection verified.");
    }

    /**
     * Fetches the page of records for the given URL.
     * Returns a PagerDutyPage with records + optional nextUrl.
     */
    public PagerDutyPage fetchPage(String url) throws IOException {
        String fullUrl = url.startsWith("http") ? url : baseUrl + url;
        String json = getJson(fullUrl);
        return parsePage(json, fullUrl);
    }

    // -------------------------------------------------------------------------
    // HTTP helpers
    // -------------------------------------------------------------------------

    private String getJson(String url) throws IOException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", authHeader)
                .header("Accept", "application/vnd.pagerduty+json;version=2")
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(conf.queryTimeoutSeconds))
                .GET()
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted during PagerDuty HTTP request", e);
        }

        if (response.statusCode() == 429) {
            throw new IOException("PagerDuty rate limit hit (HTTP 429).");
        }
        if (response.statusCode() < 200 || response.statusCode() >= 300) {
            throw new IOException("PagerDuty API error (HTTP " + response.statusCode() + ") at "
                    + url + ": " + response.body());
        }
        return response.body();
    }

    private PagerDutyPage parsePage(String json, String requestUrl) throws IOException {
        JsonNode root = mapper.readTree(json);

        // Find the records array
        List<Map<String, Object>> records = new ArrayList<>();
        for (PagerDutyTable table : TABLES.values()) {
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

        // Offset pagination details
        boolean more = false;
        if (root.has("more") && !root.get("more").isNull()) {
            more = root.get("more").asBoolean(false);
        }

        String nextUrl = null;
        if (more) {
            int limit = conf.pageSize;
            int offset = 0;

            if (requestUrl.contains("offset=")) {
                String offsetStr = extractQueryParam(requestUrl, "offset");
                if (offsetStr != null) {
                    offset = Integer.parseInt(offsetStr);
                }
            }
            if (requestUrl.contains("limit=")) {
                String limitStr = extractQueryParam(requestUrl, "limit");
                if (limitStr != null) {
                    limit = Integer.parseInt(limitStr);
                }
            }

            int nextOffset = offset + limit;

            // PagerDuty API restricts pagination to 10,000 records
            if (nextOffset < 10000) {
                nextUrl = updateQueryParam(requestUrl, "offset", String.valueOf(nextOffset));
            } else {
                more = false;
            }
        }

        PagerDutyPage page = new PagerDutyPage();
        page.records = records;
        page.nextUrl = more ? nextUrl : null;
        page.hasMore = more;
        return page;
    }

    private String extractQueryParam(String url, String param) {
        int start = url.indexOf(param + "=");
        if (start == -1) return null;
        start += param.length() + 1;
        int end = url.indexOf("&", start);
        if (end == -1) {
            return url.substring(start);
        }
        return url.substring(start, end);
    }

    private String updateQueryParam(String url, String param, String value) {
        if (!url.contains(param + "=")) {
            String separator = url.contains("?") ? "&" : "?";
            return url + separator + param + "=" + value;
        }
        int start = url.indexOf(param + "=");
        int valueStart = start + param.length() + 1;
        int end = url.indexOf("&", valueStart);
        if (end == -1) {
            return url.substring(0, valueStart) + value;
        }
        return url.substring(0, valueStart) + value + url.substring(end);
    }

    // -------------------------------------------------------------------------
    // Inner types
    // -------------------------------------------------------------------------

    public static class PagerDutyPage {
        public List<Map<String, Object>> records;
        public String nextUrl;
        public boolean hasMore;
    }
}
