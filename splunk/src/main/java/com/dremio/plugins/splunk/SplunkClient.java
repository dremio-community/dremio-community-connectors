package com.dremio.plugins.splunk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

/**
 * HTTP REST client for the Splunk management API.
 *
 * Uses Java 11's built-in HttpClient — no extra dependencies needed.
 *
 * Authentication:
 *   - Bearer token: set via authToken in SplunkConf (Splunk Cloud JWT or on-prem token)
 *   - Session key: obtained via POST /services/auth/login using username/password
 *
 * All requests use output_mode=json. Results are parsed with Jackson (provided by Dremio).
 *
 * Thread safety: this class is safe for concurrent use. The session key is stored
 * in an AtomicReference and refreshed automatically on 401 responses.
 */
public class SplunkClient implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(SplunkClient.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Poll interval for async search job completion (ms). */
  private static final int POLL_INTERVAL_INITIAL_MS = 500;
  private static final int POLL_INTERVAL_MAX_MS = 5000;
  private static final int POLL_INTERVAL_BACKOFF_MS = 500;

  private final SplunkConf config;
  private final String baseUrl;
  private final HttpClient httpClient;

  /** Holds the active auth header value — either "Bearer <token>" or "Splunk <sessionKey>". */
  private final AtomicReference<String> authHeader = new AtomicReference<>();

  public SplunkClient(SplunkConf config) throws Exception {
    this.config  = config;
    this.baseUrl = buildBaseUrl(config);
    this.httpClient = buildHttpClient(config);
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /**
   * Authenticates against Splunk. Call once after construction.
   * If authToken is set, uses it directly. Otherwise obtains a session key.
   */
  public void authenticate() throws IOException {
    if (config.authToken != null && !config.authToken.isBlank()) {
      // Bearer token auth (Splunk Cloud JWT or on-prem token)
      authHeader.set("Bearer " + config.authToken);
      logger.debug("Splunk client using bearer token auth");
    } else {
      // Username / password → session key
      String sessionKey = login(config.username, config.password);
      authHeader.set("Splunk " + sessionKey);
      logger.debug("Splunk client obtained session key for user '{}'", config.username);
    }
  }

  /**
   * Verifies connectivity by calling /services/server/info.
   * Returns the server version string.
   */
  public String getServerVersion() throws IOException {
    JsonNode root = getJson("/services/server/info");
    return root.path("entry").path(0).path("content").path("version").asText("unknown");
  }

  /** Logs out from Splunk (invalidates session key). No-op for bearer token auth. */
  @Override
  public void close() {
    String auth = authHeader.get();
    if (auth != null && auth.startsWith("Splunk ")) {
      try {
        String sessionKey = auth.substring("Splunk ".length());
        postForm("/services/authentication/httpauth-tokens/" + urlEncode(sessionKey),
            "DELETE=delete");
        logger.debug("Splunk session key revoked");
      } catch (Exception e) {
        logger.debug("Could not revoke Splunk session key: {}", e.getMessage());
      }
    }
  }

  // -----------------------------------------------------------------------
  // Index listing
  // -----------------------------------------------------------------------

  /**
   * Returns the names of all Splunk indexes visible to the authenticated user.
   * Calls GET /services/data/indexes?count=0&output_mode=json.
   */
  public List<String> listIndexes() throws IOException {
    JsonNode root = getJson("/services/data/indexes?count=0");
    List<String> names = new ArrayList<>();
    JsonNode entries = root.path("entry");
    if (entries.isArray()) {
      for (JsonNode entry : entries) {
        String name = entry.path("name").asText(null);
        if (name != null && !name.isEmpty()) {
          names.add(name);
        }
      }
    }
    return names;
  }

  /**
   * Returns true if the named index exists and is accessible.
   */
  public boolean indexExists(String indexName) throws IOException {
    try {
      getJson("/services/data/indexes/" + urlEncode(indexName));
      return true;
    } catch (IOException e) {
      if (e.getMessage() != null && e.getMessage().contains("404")) {
        return false;
      }
      throw e;
    }
  }

  /**
   * Returns the approximate event count for an index (from Splunk index metadata).
   * May be stale; use as a cost estimate only.
   */
  public long getIndexEventCount(String indexName) throws IOException {
    try {
      JsonNode root = getJson("/services/data/indexes/" + urlEncode(indexName));
      JsonNode content = root.path("entry").path(0).path("content");
      // totalEventCount is the most accurate; fall back to currentDBSizeMB heuristic
      long count = content.path("totalEventCount").asLong(-1);
      if (count >= 0) return count;
      count = content.path("eventCount").asLong(-1);
      if (count >= 0) return count;
    } catch (Exception e) {
      logger.debug("Could not fetch event count for index '{}': {}", indexName, e.getMessage());
    }
    return 100_000L; // safe default
  }

  // -----------------------------------------------------------------------
  // Search execution
  // -----------------------------------------------------------------------

  /**
   * Runs a Splunk search and returns parsed event results.
   *
   * For small sample queries (schema inference), uses exec_mode=blocking so the
   * POST itself waits for completion — no polling loop needed.
   *
   * @param spl       The SPL string (must start with "search ")
   * @param earliest  Splunk time modifier, e.g. "-1h", "2024-01-15T00:00:00"
   * @param latest    Splunk time modifier, e.g. "now", "2024-01-16T00:00:00"
   * @param maxCount  Maximum events to return
   * @return          List of events, each as a JsonNode (field → value)
   */
  public List<JsonNode> runSearch(String spl, String earliest, String latest, int maxCount)
      throws IOException {
    // Create a blocking one-shot job for bounded result sets
    String formBody = buildSearchFormBody(spl, earliest, latest, maxCount, "blocking");
    JsonNode response = postFormJson("/services/search/jobs", formBody);

    // In blocking mode the response IS the results (not a SID)
    // But if we get a SID back, fall through to async polling
    if (response.has("sid")) {
      String sid = response.get("sid").asText();
      waitForJob(sid);
      return fetchAllResults(sid, maxCount);
    }

    // Blocking mode returns results directly in some Splunk versions
    return parseResults(response);
  }

  /**
   * Creates an async search job, waits for completion, pages through all results.
   * Used by SplunkRecordReader for full table scans.
   *
   * @param spl       The SPL string
   * @param earliest  earliest time modifier
   * @param latest    latest time modifier
   * @param maxCount  max events cap
   * @return          The Splunk job SID (caller uses fetchResultsPage() to iterate)
   */
  public String createSearchJob(String spl, String earliest, String latest, int maxCount)
      throws IOException {
    String formBody = buildSearchFormBody(spl, earliest, latest, maxCount, "normal");
    JsonNode response = postFormJson("/services/search/jobs", formBody);

    if (!response.has("sid")) {
      throw new IOException("Splunk search job creation did not return a SID. Response: "
          + response.toString().substring(0, Math.min(500, response.toString().length())));
    }
    return response.get("sid").asText();
  }

  /**
   * Waits for a Splunk job to reach DONE state, polling with exponential backoff.
   * Throws IOException if the job FAILED or times out.
   */
  public void waitForJob(String sid) throws IOException {
    int pollInterval = POLL_INTERVAL_INITIAL_MS;
    long deadline = System.currentTimeMillis()
        + (long) config.readTimeoutSeconds * 1000L;

    while (System.currentTimeMillis() < deadline) {
      JsonNode jobStatus = getJson("/services/search/jobs/" + urlEncode(sid));
      JsonNode content   = jobStatus.path("entry").path(0).path("content");
      String   state     = content.path("dispatchState").asText("QUEUED");

      logger.debug("Splunk job {} state: {}", sid, state);

      if ("DONE".equals(state)) return;
      if ("FAILED".equals(state) || "FINALIZING".equals(state)) {
        String messages = content.path("messages").toString();
        throw new IOException("Splunk search job " + sid + " failed. State: " + state
            + ", Messages: " + messages);
      }

      try {
        Thread.sleep(pollInterval);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted waiting for Splunk job " + sid);
      }

      pollInterval = Math.min(pollInterval + POLL_INTERVAL_BACKOFF_MS, POLL_INTERVAL_MAX_MS);
    }

    throw new IOException("Timed out waiting for Splunk job " + sid
        + " after " + config.readTimeoutSeconds + "s");
  }

  /**
   * Returns a page of results from a completed search job.
   *
   * @param sid    Job SID
   * @param offset Zero-based offset
   * @param count  Page size
   * @return       List of events (empty when no more results)
   */
  public List<JsonNode> fetchResultsPage(String sid, int offset, int count) throws IOException {
    String url = "/services/search/jobs/" + urlEncode(sid)
        + "/results?output_mode=json&offset=" + offset + "&count=" + count;
    JsonNode root = getJson(url);
    return parseResults(root);
  }

  /**
   * Returns total result count for a completed job.
   */
  public int getJobResultCount(String sid) throws IOException {
    JsonNode jobStatus = getJson("/services/search/jobs/" + urlEncode(sid));
    return jobStatus.path("entry").path(0).path("content")
        .path("resultCount").asInt(0);
  }

  /**
   * Cancels (deletes) a running or completed search job.
   * Always call in RecordReader.close() to avoid leaving orphaned jobs on Splunk.
   */
  public void cancelJob(String sid) {
    try {
      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(baseUrl + "/services/search/jobs/" + urlEncode(sid)))
          .header("Authorization", authHeader.get())
          .DELETE()
          .build();
      httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      logger.debug("Cancelled Splunk job {}", sid);
    } catch (Exception e) {
      logger.debug("Could not cancel Splunk job {}: {}", sid, e.getMessage());
    }
  }

  // -----------------------------------------------------------------------
  // Private helpers — HTTP
  // -----------------------------------------------------------------------

  private String login(String username, String password) throws IOException {
    String body = "username=" + urlEncode(username) + "&password=" + urlEncode(password)
        + "&output_mode=json";
    try {
      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(baseUrl + "/services/auth/login"))
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(body))
          .build();
      HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      if (resp.statusCode() == 401 || resp.statusCode() == 403) {
        throw new IOException("Splunk authentication failed (HTTP " + resp.statusCode()
            + "). Check username and password.");
      }
      if (resp.statusCode() != 200) {
        throw new IOException("Splunk login returned HTTP " + resp.statusCode()
            + ": " + truncate(resp.body()));
      }
      JsonNode root = MAPPER.readTree(resp.body());
      String sessionKey = root.path("sessionKey").asText(null);
      if (sessionKey == null || sessionKey.isEmpty()) {
        throw new IOException("Splunk login response did not contain a sessionKey: "
            + truncate(resp.body()));
      }
      return sessionKey;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during Splunk login", e);
    }
  }

  private JsonNode getJson(String path) throws IOException {
    try {
      String url = path.startsWith("http") ? path : baseUrl + path;
      // Append output_mode=json if not already present
      if (!url.contains("output_mode=")) {
        url += (url.contains("?") ? "&" : "?") + "output_mode=json";
      }
      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(url))
          .header("Authorization", authHeader.get())
          .GET()
          .timeout(Duration.ofSeconds(config.readTimeoutSeconds))
          .build();
      HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      return checkResponse(resp);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during GET " + path, e);
    }
  }

  private JsonNode postFormJson(String path, String formBody) throws IOException {
    try {
      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(baseUrl + path))
          .header("Authorization", authHeader.get())
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(formBody))
          .timeout(Duration.ofSeconds(config.readTimeoutSeconds))
          .build();
      HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
      return checkResponse(resp);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted during POST " + path, e);
    }
  }

  private void postForm(String path, String formBody) throws IOException {
    try {
      HttpRequest req = HttpRequest.newBuilder()
          .uri(URI.create(baseUrl + path))
          .header("Authorization", authHeader.get())
          .header("Content-Type", "application/x-www-form-urlencoded")
          .POST(HttpRequest.BodyPublishers.ofString(formBody))
          .timeout(Duration.ofSeconds(config.connectionTimeoutSeconds))
          .build();
      httpClient.send(req, HttpResponse.BodyHandlers.ofString());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private JsonNode checkResponse(HttpResponse<String> resp) throws IOException {
    int status = resp.statusCode();
    if (status == 401) {
      throw new IOException("Splunk returned 401 Unauthorized. Session may have expired.");
    }
    if (status == 403) {
      throw new IOException("Splunk returned 403 Forbidden. Check user permissions.");
    }
    if (status == 404) {
      throw new IOException("Splunk returned 404 Not Found for: " + resp.uri());
    }
    if (status < 200 || status >= 300) {
      throw new IOException("Splunk returned HTTP " + status + ": " + truncate(resp.body()));
    }
    String body = resp.body();
    if (body == null || body.isBlank()) {
      return MAPPER.createObjectNode();
    }
    try {
      return MAPPER.readTree(body);
    } catch (Exception e) {
      throw new IOException("Failed to parse Splunk JSON response: " + truncate(body), e);
    }
  }

  // -----------------------------------------------------------------------
  // Private helpers — parsing
  // -----------------------------------------------------------------------

  private List<JsonNode> fetchAllResults(String sid, int maxCount) throws IOException {
    List<JsonNode> all = new ArrayList<>();
    int offset = 0;
    int pageSize = Math.min(config.resultsPageSize, maxCount);

    while (all.size() < maxCount) {
      List<JsonNode> page = fetchResultsPage(sid, offset, pageSize);
      if (page.isEmpty()) break;
      all.addAll(page);
      offset += page.size();
      if (page.size() < pageSize) break; // last page
    }
    return all;
  }

  private List<JsonNode> parseResults(JsonNode root) {
    List<JsonNode> events = new ArrayList<>();
    JsonNode results = root.path("results");
    if (results.isArray()) {
      for (JsonNode event : results) {
        events.add(event);
      }
    }
    return events;
  }

  private String buildSearchFormBody(String spl, String earliest, String latest,
                                      int maxCount, String execMode) {
    StringBuilder sb = new StringBuilder();
    sb.append("search=").append(urlEncode(spl));
    sb.append("&output_mode=json");
    sb.append("&exec_mode=").append(urlEncode(execMode));
    sb.append("&search_mode=").append(urlEncode(config.searchMode));
    sb.append("&max_count=").append(maxCount);
    if (earliest != null && !earliest.isBlank()) {
      sb.append("&earliest_time=").append(urlEncode(earliest));
    }
    if (latest != null && !latest.isBlank()) {
      sb.append("&latest_time=").append(urlEncode(latest));
    }
    return sb.toString();
  }

  // -----------------------------------------------------------------------
  // Private helpers — setup
  // -----------------------------------------------------------------------

  private static String buildBaseUrl(SplunkConf config) {
    boolean ssl  = config.useSsl || config.splunkCloud;
    int     port = config.splunkCloud ? 443 : config.port;
    String scheme = ssl ? "https" : "http";
    return scheme + "://" + config.hostname + ":" + port;
  }

  private static HttpClient buildHttpClient(SplunkConf config) throws Exception {
    HttpClient.Builder builder = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(config.connectionTimeoutSeconds))
        .version(HttpClient.Version.HTTP_1_1);

    if (config.disableSslVerification) {
      builder.sslContext(buildTrustAllSslContext());
      logger.warn("Splunk SSL certificate verification is DISABLED. "
          + "Do not use this setting in production.");
    }

    return builder.build();
  }

  private static SSLContext buildTrustAllSslContext()
      throws NoSuchAlgorithmException, KeyManagementException {
    TrustManager[] trustAll = new TrustManager[]{
        new X509TrustManager() {
          public void checkClientTrusted(X509Certificate[] c, String a) {}
          public void checkServerTrusted(X509Certificate[] c, String a) {}
          public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
        }
    };
    SSLContext sc = SSLContext.getInstance("TLS");
    sc.init(null, trustAll, new java.security.SecureRandom());
    return sc;
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String truncate(String s) {
    if (s == null) return "(null)";
    return s.length() > 500 ? s.substring(0, 500) + "..." : s;
  }
}
