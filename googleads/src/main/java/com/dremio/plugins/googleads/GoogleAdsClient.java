package com.dremio.plugins.googleads;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Thin REST client for the Google Ads API v18.
 *
 * Auth flow: OAuth2 refresh token → access token via oauth2.googleapis.com/token.
 * No external libraries needed — everything via java.net.HttpURLConnection.
 *
 * All GAQL queries go to:
 *   POST https://googleads.googleapis.com/v18/customers/{customerId}/googleAds:search
 * Headers:
 *   Authorization: Bearer {accessToken}
 *   developer-token: {developerToken}
 *   login-customer-id: {loginCustomerId}  (if set)
 */
public class GoogleAdsClient {

  private static final Logger logger = LoggerFactory.getLogger(GoogleAdsClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String ADS_BASE         = "https://googleads.googleapis.com/v18";
  private static final String TOKEN_ENDPOINT   = "https://oauth2.googleapis.com/token";
  private static final int    CONNECT_TIMEOUT  = 15_000;
  private static final int    READ_TIMEOUT     = 60_000;
  private static final int    PAGE_SIZE        = 10_000;

  private final String developerToken;
  private final String clientId;
  private final String clientSecret;
  private final String refreshToken;
  private final String customerId;
  private final String loginCustomerId;

  private volatile String  accessToken;
  private volatile long    tokenExpiresAtMs = 0L;

  public GoogleAdsClient(GoogleAdsConf cfg) {
    this.developerToken   = cfg.developerToken;
    this.clientId         = cfg.clientId;
    this.clientSecret     = cfg.clientSecret;
    this.refreshToken     = cfg.refreshToken;
    this.customerId       = cfg.customerId.replace("-", "");
    this.loginCustomerId  = (cfg.loginCustomerId == null) ? ""
                            : cfg.loginCustomerId.replace("-", "");
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  public void connect() throws IOException {
    refreshAccessToken();
    logger.info("GoogleAdsClient: connected (customer={})", customerId);
  }

  public void healthCheck() throws IOException {
    String gaql = "SELECT customer.id FROM customer LIMIT 1";
    search(gaql, customerId);
  }

  // -----------------------------------------------------------------------
  // GAQL search (paginated)
  // -----------------------------------------------------------------------

  /**
   * Executes a GAQL query and returns all result rows (across all pages).
   * Each element is the raw JSON result object from the API.
   */
  public List<JsonNode> search(String gaql, String targetCustomerId) throws IOException {
    List<JsonNode> results = new ArrayList<>();
    String pageToken = null;

    do {
      ObjectNode body = MAPPER.createObjectNode();
      body.put("query", gaql.trim());
      body.put("pageSize", PAGE_SIZE);
      if (pageToken != null) body.put("pageToken", pageToken);

      String url = ADS_BASE + "/customers/" + targetCustomerId + "/googleAds:search";
      JsonNode response = doPost(url, body.toString());

      JsonNode resultArray = response.get("results");
      if (resultArray != null && resultArray.isArray()) {
        for (JsonNode row : resultArray) {
          results.add(row);
        }
      }

      JsonNode nextToken = response.get("nextPageToken");
      pageToken = (nextToken != null && !nextToken.isNull()) ? nextToken.asText() : null;

    } while (pageToken != null);

    return results;
  }

  /** Convenience: search using the configured customer ID. */
  public List<JsonNode> search(String gaql) throws IOException {
    return search(gaql, customerId);
  }

  public String getCustomerId() { return customerId; }

  // -----------------------------------------------------------------------
  // HTTP helpers
  // -----------------------------------------------------------------------

  private JsonNode doPost(String urlStr, String jsonBody) throws IOException {
    ensureTokenValid();
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT);
    conn.setReadTimeout(READ_TIMEOUT);
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
    conn.setRequestProperty("Authorization", "Bearer " + accessToken);
    conn.setRequestProperty("developer-token", developerToken);
    if (!loginCustomerId.isEmpty()) {
      conn.setRequestProperty("login-customer-id", loginCustomerId);
    }

    byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", String.valueOf(bodyBytes.length));
    try (OutputStream os = conn.getOutputStream()) {
      os.write(bodyBytes);
    }

    return readResponse(conn);
  }

  private JsonNode readResponse(HttpURLConnection conn) throws IOException {
    int status = conn.getResponseCode();
    InputStream is = (status >= 200 && status < 300)
        ? conn.getInputStream() : conn.getErrorStream();
    if (is == null) {
      if (status >= 200 && status < 300) return MAPPER.createObjectNode();
      throw new IOException("HTTP " + status + " with no response body");
    }
    try {
      JsonNode node = MAPPER.readTree(is);
      if (status >= 400) {
        String msg = node.path("error").path("message").asText("HTTP error " + status);
        throw new IOException("Google Ads API error " + status + ": " + msg);
      }
      return node;
    } finally {
      is.close();
    }
  }

  // -----------------------------------------------------------------------
  // OAuth2 token management
  // -----------------------------------------------------------------------

  private void ensureTokenValid() throws IOException {
    // Refresh 60 seconds before expiry
    if (accessToken == null || System.currentTimeMillis() > tokenExpiresAtMs - 60_000L) {
      synchronized (this) {
        if (accessToken == null || System.currentTimeMillis() > tokenExpiresAtMs - 60_000L) {
          refreshAccessToken();
        }
      }
    }
  }

  private void refreshAccessToken() throws IOException {
    String body = "client_id=" + urlEncode(clientId)
        + "&client_secret=" + urlEncode(clientSecret)
        + "&refresh_token=" + urlEncode(refreshToken)
        + "&grant_type=refresh_token";

    HttpURLConnection conn = (HttpURLConnection) new URL(TOKEN_ENDPOINT).openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT);
    conn.setReadTimeout(READ_TIMEOUT);
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
    byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", String.valueOf(bodyBytes.length));
    try (OutputStream os = conn.getOutputStream()) {
      os.write(bodyBytes);
    }

    int status = conn.getResponseCode();
    InputStream is = (status == 200) ? conn.getInputStream() : conn.getErrorStream();
    JsonNode resp;
    try {
      resp = MAPPER.readTree(is);
    } finally {
      if (is != null) is.close();
    }

    if (status != 200) {
      String msg = resp.path("error_description").asText(resp.path("error").asText("unknown"));
      throw new IOException("OAuth2 token refresh failed: " + msg);
    }

    this.accessToken      = resp.get("access_token").asText();
    long expiresIn        = resp.path("expires_in").asLong(3600);
    this.tokenExpiresAtMs = System.currentTimeMillis() + expiresIn * 1000L;
    logger.debug("GoogleAdsClient: access token refreshed, expires in {}s", expiresIn);
  }

  private static String urlEncode(String s) {
    try {
      return java.net.URLEncoder.encode(s, "UTF-8");
    } catch (Exception e) {
      return s;
    }
  }
}
