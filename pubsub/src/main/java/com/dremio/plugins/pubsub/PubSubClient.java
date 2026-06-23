package com.dremio.plugins.pubsub;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Thin REST client for the Google Cloud Pub/Sub API.
 *
 * Uses java.net.HttpURLConnection (no extra HTTP client dependency) plus
 * google-auth-library-oauth2-http for credential management (ADC or SA key file).
 * In emulator mode, all auth is skipped and requests go to the emulator host.
 *
 * All methods throw IOException on HTTP/auth errors.
 */
public class PubSubClient {

  private static final Logger logger = LoggerFactory.getLogger(PubSubClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String PUBSUB_BASE = "https://pubsub.googleapis.com/v1";
  private static final int    CONNECT_TIMEOUT_MS = 10_000;
  private static final int    READ_TIMEOUT_MS    = 30_000;

  private final String  projectId;
  private final String  credentialsFile;
  private final String  emulatorHost;  // "localhost:8085" or empty for production

  // Lazy-initialized credentials (null in emulator mode)
  private volatile Object credentials; // com.google.auth.oauth2.GoogleCredentials

  public PubSubClient(String projectId, String credentialsFile, String emulatorHost) {
    this.projectId      = projectId;
    this.credentialsFile = credentialsFile;
    this.emulatorHost   = (emulatorHost == null) ? "" : emulatorHost.trim();
  }

  // -----------------------------------------------------------------------
  // Lifecycle
  // -----------------------------------------------------------------------

  /** Initialize credentials (no-op in emulator mode). */
  public void connect() throws IOException {
    if (!emulatorHost.isEmpty()) {
      logger.info("PubSubClient: emulator mode → {}", emulatorHost);
      return;
    }
    credentials = buildCredentials();
    logger.info("PubSubClient: connected to production GCP (project={})", projectId);
  }

  /** Verify the connection by listing subscriptions. */
  public void healthCheck() throws IOException {
    listSubscriptions(); // throws if not reachable
  }

  public String getProjectId() {
    return projectId;
  }

  // -----------------------------------------------------------------------
  // Subscription listing
  // -----------------------------------------------------------------------

  /**
   * Returns the short subscription names (last path component) for all subscriptions
   * in the project. Full resource names are "projects/{project}/subscriptions/{name}".
   */
  public List<String> listSubscriptions() throws IOException {
    String url = baseUrl() + "/projects/" + projectId + "/subscriptions";
    JsonNode root = doGet(url);
    List<String> names = new ArrayList<>();
    JsonNode subs = root.get("subscriptions");
    if (subs != null && subs.isArray()) {
      for (JsonNode sub : subs) {
        String fullName = sub.get("name").asText();
        // "projects/my-project/subscriptions/my-sub" → "my-sub"
        names.add(fullName.substring(fullName.lastIndexOf('/') + 1));
      }
    }
    return names;
  }

  // -----------------------------------------------------------------------
  // Message pulling
  // -----------------------------------------------------------------------

  /**
   * Pulls up to maxMessages from the subscription.
   *
   * Uses returnImmediately=false with the API's server-side 10s long-poll
   * so we don't busy-loop when the subscription is idle. The overall budget
   * is controlled by callers via repeated pulls up to totalMaxMessages.
   */
  public List<PubSubMessage> pull(String subscription, int maxMessages) throws IOException {
    String url = baseUrl() + "/projects/" + projectId + "/subscriptions/"
        + subscription + ":pull";
    ObjectNode body = MAPPER.createObjectNode();
    body.put("maxMessages", maxMessages);
    // returnImmediately defaults to false in the API (long-poll up to ~10s)

    JsonNode root = doPost(url, body.toString());
    List<PubSubMessage> messages = new ArrayList<>();
    JsonNode received = root.get("receivedMessages");
    if (received != null && received.isArray()) {
      for (JsonNode item : received) {
        messages.add(parseMessage(item));
      }
    }
    return messages;
  }

  /**
   * NACKs (returns) messages to the subscription by setting their ack deadline to 0.
   * After this call Pub/Sub will redeliver them to the next puller immediately.
   */
  public void nack(String subscription, List<String> ackIds) throws IOException {
    if (ackIds.isEmpty()) return;
    String url = baseUrl() + "/projects/" + projectId + "/subscriptions/"
        + subscription + ":modifyAckDeadline";
    ObjectNode body = MAPPER.createObjectNode();
    body.put("ackDeadlineSeconds", 0);
    com.fasterxml.jackson.databind.node.ArrayNode arr = body.putArray("ackIds");
    for (String id : ackIds) arr.add(id);
    doPost(url, body.toString()); // response is empty {}
  }

  // -----------------------------------------------------------------------
  // HTTP helpers
  // -----------------------------------------------------------------------

  private String baseUrl() {
    if (!emulatorHost.isEmpty()) {
      return "http://" + emulatorHost + "/v1";
    }
    return PUBSUB_BASE;
  }

  private JsonNode doGet(String urlStr) throws IOException {
    HttpURLConnection conn = openConnection(urlStr, "GET");
    conn.setRequestMethod("GET");
    return readResponse(conn);
  }

  private JsonNode doPost(String urlStr, String jsonBody) throws IOException {
    HttpURLConnection conn = openConnection(urlStr, "POST");
    conn.setRequestMethod("POST");
    conn.setDoOutput(true);
    conn.setRequestProperty("Content-Type", "application/json; charset=UTF-8");
    byte[] bodyBytes = jsonBody.getBytes(StandardCharsets.UTF_8);
    conn.setRequestProperty("Content-Length", String.valueOf(bodyBytes.length));
    try (OutputStream os = conn.getOutputStream()) {
      os.write(bodyBytes);
    }
    return readResponse(conn);
  }

  private HttpURLConnection openConnection(String urlStr, String method) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
    conn.setConnectTimeout(CONNECT_TIMEOUT_MS);
    conn.setReadTimeout(READ_TIMEOUT_MS);
    if (!emulatorHost.isEmpty()) {
      // Emulator: no auth needed
      return conn;
    }
    // Production: attach Bearer token
    String token = getAccessToken();
    conn.setRequestProperty("Authorization", "Bearer " + token);
    return conn;
  }

  private JsonNode readResponse(HttpURLConnection conn) throws IOException {
    int status = conn.getResponseCode();
    InputStream is = (status >= 200 && status < 300)
        ? conn.getInputStream()
        : conn.getErrorStream();
    if (is == null) {
      if (status >= 200 && status < 300) return MAPPER.createObjectNode();
      throw new IOException("HTTP " + status + " with no response body");
    }
    try {
      JsonNode node = MAPPER.readTree(is);
      if (status >= 400) {
        String msg = node.path("error").path("message").asText("HTTP error " + status);
        throw new IOException("Pub/Sub API error " + status + ": " + msg);
      }
      return node;
    } finally {
      is.close();
    }
  }

  // -----------------------------------------------------------------------
  // Auth
  // -----------------------------------------------------------------------

  private Object buildCredentials() throws IOException {
    try {
      Class<?> gcClass = Class.forName("com.google.auth.oauth2.GoogleCredentials");
      Object creds;
      if (credentialsFile != null && !credentialsFile.isEmpty()) {
        try (FileInputStream fis = new FileInputStream(credentialsFile)) {
          creds = gcClass.getMethod("fromStream", InputStream.class).invoke(null, fis);
        }
      } else {
        creds = gcClass.getMethod("getApplicationDefault").invoke(null);
      }
      // Scope to cloud-platform
      Class<?> listClass = Class.forName("java.util.List");
      Object scoped = gcClass.getMethod("createScoped", Iterable.class)
          .invoke(creds, java.util.Collections.singletonList(
              "https://www.googleapis.com/auth/cloud-platform"));
      return scoped;
    } catch (ReflectiveOperationException e) {
      throw new IOException(
          "google-auth-library-oauth2-http not available. "
          + "Ensure dremio-pubsub-connector-plugin.jar is deployed.", e);
    }
  }

  private String getAccessToken() throws IOException {
    if (credentials == null) {
      synchronized (this) {
        if (credentials == null) credentials = buildCredentials();
      }
    }
    try {
      // credentials.refreshIfExpired()
      credentials.getClass().getMethod("refreshIfExpired").invoke(credentials);
      // credentials.getAccessToken().getTokenValue()
      Object token = credentials.getClass().getMethod("getAccessToken").invoke(credentials);
      return (String) token.getClass().getMethod("getTokenValue").invoke(token);
    } catch (ReflectiveOperationException e) {
      throw new IOException("Failed to obtain GCP access token", e);
    }
  }

  // -----------------------------------------------------------------------
  // Message parsing
  // -----------------------------------------------------------------------

  private PubSubMessage parseMessage(JsonNode item) {
    String ackId = item.path("ackId").asText("");
    JsonNode msg  = item.path("message");

    String messageId    = msg.path("messageId").asText("");
    String orderingKey  = msg.path("orderingKey").asText("");

    // publishTime is RFC 3339 string e.g. "2021-01-01T00:00:00.123Z"
    long publishTimeMs = 0L;
    String pubTimeStr = msg.path("publishTime").asText("");
    if (!pubTimeStr.isEmpty()) {
      try {
        publishTimeMs = Instant.parse(pubTimeStr).toEpochMilli();
      } catch (Exception ignore) { }
    }

    // data is base64-encoded
    byte[] data = new byte[0];
    String dataStr = msg.path("data").asText("");
    if (!dataStr.isEmpty()) {
      try {
        data = Base64.getDecoder().decode(dataStr);
      } catch (Exception ignore) { }
    }

    // attributes is a JSON object
    Map<String, String> attributes = new HashMap<>();
    JsonNode attrsNode = msg.path("attributes");
    if (attrsNode.isObject()) {
      attrsNode.fields().forEachRemaining(e -> attributes.put(e.getKey(), e.getValue().asText()));
    }

    return new PubSubMessage(ackId, messageId, publishTimeMs, orderingKey, data, attributes);
  }

  // -----------------------------------------------------------------------
  // Message value object
  // -----------------------------------------------------------------------

  public static class PubSubMessage {
    public final String              ackId;
    public final String              messageId;
    public final long                publishTimeMs;
    public final String              orderingKey;
    public final byte[]              data;
    public final Map<String, String> attributes;

    public PubSubMessage(String ackId, String messageId, long publishTimeMs,
                          String orderingKey, byte[] data, Map<String, String> attributes) {
      this.ackId          = ackId;
      this.messageId      = messageId;
      this.publishTimeMs  = publishTimeMs;
      this.orderingKey    = orderingKey;
      this.data           = data;
      this.attributes     = attributes;
    }

    public String dataAsUtf8() {
      return new String(data, StandardCharsets.UTF_8);
    }
  }
}
