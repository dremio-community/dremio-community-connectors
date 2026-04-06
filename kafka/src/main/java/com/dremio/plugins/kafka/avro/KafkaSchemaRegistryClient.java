package com.dremio.plugins.kafka.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;
import java.util.Base64;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Fetches and caches Avro schemas from a Confluent-compatible Schema Registry.
 *
 * Supports:
 *   - HTTP and HTTPS endpoints
 *   - HTTP Basic authentication (username/password or Confluent Cloud API key/secret)
 *   - Optional TLS hostname verification bypass (for self-signed certs)
 *
 * Endpoints used:
 *   GET {url}/subjects/{topic}-value/versions/latest
 *     → {"id": 42, "version": 3, "schema": "{...avro json...}"}
 *   GET {url}/schemas/ids/{id}
 *     → {"schema": "{...avro json...}"}
 *
 * Cache policy:
 *   - Schema by ID: permanent (schema IDs are immutable in Confluent SR)
 *   - Latest schema by subject: TTL-based (subject can evolve)
 *
 * Confluent Cloud setup:
 *   url      = https://psrc-abc123.us-east-1.aws.confluent.cloud
 *   username = <API key>
 *   password = <API secret>
 */
public class KafkaSchemaRegistryClient {

  private static final Logger logger = LoggerFactory.getLogger(KafkaSchemaRegistryClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String  baseUrl;
  private final int     timeoutMs;
  private final long    cacheTtlMs;
  private final String  authHeader;          // null if no auth configured
  private final boolean disableSslVerification;

  /** Lazy-initialised trust-all SSLContext used only when disableSslVerification=true */
  private volatile SSLContext trustAllSslContext;

  /** cache: schemaId → Schema (immutable; never evicted) */
  private final ConcurrentHashMap<Integer, Schema> schemaById;
  /** cache: subject → (Schema, expiry) */
  private final ConcurrentHashMap<String, CachedLatest> latestBySubject;

  private static class CachedLatest {
    final Schema schema;
    final long expiresAt;

    CachedLatest(Schema s, long ttlMs) {
      this.schema = s;
      this.expiresAt = System.currentTimeMillis() + ttlMs;
    }

    boolean isExpired() {
      return System.currentTimeMillis() > expiresAt;
    }
  }

  /**
   * Full constructor.
   *
   * @param baseUrl                 Schema Registry base URL (http:// or https://)
   * @param timeoutMs               Connect + read timeout in milliseconds
   * @param cacheTtlMs              TTL for subject-level latest-schema cache (ms)
   * @param username                Basic auth username or Confluent Cloud API key (null/empty = no auth)
   * @param password                Basic auth password or Confluent Cloud API secret
   * @param disableSslVerification  When true, skips TLS cert and hostname checks (dev/self-signed only)
   */
  public KafkaSchemaRegistryClient(String baseUrl, int timeoutMs, long cacheTtlMs,
                                    String username, String password,
                                    boolean disableSslVerification) {
    this.baseUrl   = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.timeoutMs = timeoutMs;
    this.cacheTtlMs = cacheTtlMs;
    this.disableSslVerification = disableSslVerification;
    this.schemaById       = new ConcurrentHashMap<>();
    this.latestBySubject  = new ConcurrentHashMap<>();

    if (username != null && !username.isEmpty()) {
      String credentials = username + ":" + (password != null ? password : "");
      this.authHeader = "Basic " + Base64.getEncoder()
          .encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
      logger.debug("Schema Registry client configured with Basic auth (user={})", username);
    } else {
      this.authHeader = null;
    }

    if (disableSslVerification) {
      logger.warn("Schema Registry SSL hostname verification is DISABLED. "
          + "This is unsafe outside of development environments.");
    }
  }

  /**
   * Convenience constructor — no authentication, no SSL bypass.
   * Kept for callers that don't need auth (plain http:// registries).
   */
  public KafkaSchemaRegistryClient(String baseUrl, int timeoutMs, long cacheTtlMs) {
    this(baseUrl, timeoutMs, cacheTtlMs, null, null, false);
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Get the latest schema for a topic (uses {topic}-value subject convention).
   */
  public Schema getLatestSchema(String topic) throws Exception {
    String subject = topic + "-value";
    CachedLatest cached = latestBySubject.get(subject);
    if (cached != null && !cached.isExpired()) {
      return cached.schema;
    }

    String url  = baseUrl + "/subjects/" + subject + "/versions/latest";
    String json = httpGet(url);
    JsonNode node = MAPPER.readTree(json);
    int    id        = node.get("id").asInt();
    String schemaStr = node.get("schema").asText();
    Schema schema    = new Schema.Parser().parse(schemaStr);
    schemaById.put(id, schema);
    latestBySubject.put(subject, new CachedLatest(schema, cacheTtlMs));
    logger.debug("Fetched latest schema for subject {} (id={})", subject, id);
    return schema;
  }

  /**
   * Get a schema by its registry ID (used when decoding individual messages).
   * Schema IDs are immutable in Confluent SR — cached forever once fetched.
   */
  public Schema getSchemaById(int id) throws Exception {
    Schema cached = schemaById.get(id);
    if (cached != null) {
      return cached;
    }

    String url  = baseUrl + "/schemas/ids/" + id;
    String json = httpGet(url);
    JsonNode node    = MAPPER.readTree(json);
    String schemaStr = node.get("schema").asText();
    Schema schema    = new Schema.Parser().parse(schemaStr);
    schemaById.put(id, schema);
    logger.debug("Fetched schema by id={}", id);
    return schema;
  }

  // ---------------------------------------------------------------------------
  // HTTP helpers
  // ---------------------------------------------------------------------------

  private String httpGet(String urlStr) throws Exception {
    HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();

    // Apply SSL configuration for HTTPS connections
    if (conn instanceof HttpsURLConnection) {
      applyHttpsConfig((HttpsURLConnection) conn);
    }

    conn.setRequestMethod("GET");
    conn.setConnectTimeout(timeoutMs);
    conn.setReadTimeout(timeoutMs);
    conn.setRequestProperty("Accept", "application/vnd.schemaregistry.v1+json");

    // Inject Basic auth header if configured
    if (authHeader != null) {
      conn.setRequestProperty("Authorization", authHeader);
    }

    int code = conn.getResponseCode();
    if (code != 200) {
      throw new Exception("Schema Registry returned HTTP " + code + " for " + urlStr);
    }
    try (InputStream is = conn.getInputStream();
         Scanner s = new Scanner(is, "UTF-8")) {
      s.useDelimiter("\\A");
      return s.hasNext() ? s.next() : "";
    } finally {
      conn.disconnect();
    }
  }

  /**
   * Applies SSL configuration to an HTTPS connection.
   * When disableSslVerification=true, installs a trust-all TrustManager and a
   * permissive HostnameVerifier so self-signed certificates are accepted.
   *
   * The trust-all SSLContext is created lazily and reused across requests.
   */
  private void applyHttpsConfig(HttpsURLConnection conn) {
    if (!disableSslVerification) {
      return; // use JVM default SSL — verifies cert chain and hostname
    }
    try {
      if (trustAllSslContext == null) {
        synchronized (this) {
          if (trustAllSslContext == null) {
            TrustManager[] trustAll = new TrustManager[]{
                new X509TrustManager() {
                  public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
                  public void checkClientTrusted(X509Certificate[] c, String a) {}
                  public void checkServerTrusted(X509Certificate[] c, String a) {}
                }
            };
            SSLContext ctx = SSLContext.getInstance("TLS");
            ctx.init(null, trustAll, new java.security.SecureRandom());
            trustAllSslContext = ctx;
          }
        }
      }
      conn.setSSLSocketFactory(trustAllSslContext.getSocketFactory());
      conn.setHostnameVerifier((hostname, session) -> true);
    } catch (Exception e) {
      logger.warn("Failed to configure trust-all SSL context: {}", e.getMessage());
    }
  }
}
