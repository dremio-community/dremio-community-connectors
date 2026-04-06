package com.dremio.plugins.kafka.avro;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.avro.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for KafkaSchemaRegistryClient — authentication, caching,
 * and error handling. Uses an embedded HttpServer to avoid external dependencies.
 */
class KafkaSchemaRegistryClientTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private static final String AVRO_SCHEMA_JSON =
      "{\"type\":\"record\",\"name\":\"Order\",\"fields\":["
      + "{\"name\":\"order_id\",\"type\":\"long\"},"
      + "{\"name\":\"customer\",\"type\":\"string\"},"
      + "{\"name\":\"amount\",\"type\":[\"null\",\"double\"],\"default\":null}"
      + "]}";

  private HttpServer server;
  private String     baseUrl;

  // Captured request info set by the test handler
  private volatile String lastAuthHeader;
  private volatile int    requestCount;

  @BeforeEach
  void startServer() throws IOException {
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.setExecutor(Executors.newSingleThreadExecutor());
    lastAuthHeader = null;
    requestCount   = 0;

    // GET /subjects/orders-value/versions/latest
    server.createContext("/subjects/orders-value/versions/latest", exchange -> {
      lastAuthHeader = exchange.getRequestHeaders().getFirst("Authorization");
      requestCount++;
      respond(exchange, 200, MAPPER.writeValueAsString(Map.of(
          "id", 1,
          "version", 1,
          "schema", AVRO_SCHEMA_JSON
      )));
    });

    // GET /schemas/ids/1
    server.createContext("/schemas/ids/1", exchange -> {
      lastAuthHeader = exchange.getRequestHeaders().getFirst("Authorization");
      requestCount++;
      respond(exchange, 200, MAPPER.writeValueAsString(Map.of("schema", AVRO_SCHEMA_JSON)));
    });

    // GET /schemas/ids/99  →  404
    server.createContext("/schemas/ids/99", exchange ->
        respond(exchange, 404, "{\"error_code\":40403,\"message\":\"Schema not found\"}"));

    server.start();
    int port = server.getAddress().getPort();
    baseUrl = "http://localhost:" + port;
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  // ---------------------------------------------------------------------------
  // Unauthenticated requests
  // ---------------------------------------------------------------------------

  @Test
  void getLatestSchema_noAuth_succeeds() throws Exception {
    KafkaSchemaRegistryClient client = new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L);
    Schema schema = client.getLatestSchema("orders");

    assertNotNull(schema);
    assertEquals("Order", schema.getName());
    assertEquals(3, schema.getFields().size());
    assertNull(lastAuthHeader, "No Authorization header should be sent");
  }

  @Test
  void getSchemaById_noAuth_succeeds() throws Exception {
    KafkaSchemaRegistryClient client = new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L);
    Schema schema = client.getSchemaById(1);

    assertNotNull(schema);
    assertEquals("Order", schema.getName());
    assertNull(lastAuthHeader);
  }

  // ---------------------------------------------------------------------------
  // Basic authentication
  // ---------------------------------------------------------------------------

  @Test
  void getLatestSchema_withAuth_sendsBasicHeader() throws Exception {
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L, "alice", "secret123", false);

    client.getLatestSchema("orders");

    assertNotNull(lastAuthHeader, "Authorization header must be present");
    assertTrue(lastAuthHeader.startsWith("Basic "), "Must use Basic scheme");

    String decoded = new String(
        Base64.getDecoder().decode(lastAuthHeader.substring(6)), StandardCharsets.UTF_8);
    assertEquals("alice:secret123", decoded);
  }

  @Test
  void getSchemaById_withAuth_sendsBasicHeader() throws Exception {
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L, "apikey", "apisecret", false);

    client.getSchemaById(1);

    assertNotNull(lastAuthHeader);
    String decoded = new String(
        Base64.getDecoder().decode(lastAuthHeader.substring(6)), StandardCharsets.UTF_8);
    assertEquals("apikey:apisecret", decoded);
  }

  @Test
  void emptyUsername_noAuthHeaderSent() throws Exception {
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L, "", "ignored", false);

    client.getLatestSchema("orders");
    assertNull(lastAuthHeader, "Empty username → no auth header");
  }

  @Test
  void nullUsername_noAuthHeaderSent() throws Exception {
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L, null, null, false);

    client.getLatestSchema("orders");
    assertNull(lastAuthHeader);
  }

  // ---------------------------------------------------------------------------
  // Caching
  // ---------------------------------------------------------------------------

  @Test
  void getLatestSchema_cacheHit_onlyOneRequest() throws Exception {
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L);

    Schema s1 = client.getLatestSchema("orders");
    Schema s2 = client.getLatestSchema("orders");

    assertSame(s1, s2, "Cached result should be the same object");
    assertEquals(1, requestCount, "Only one HTTP request should be made");
  }

  @Test
  void getSchemaById_cacheHit_onlyOneRequest() throws Exception {
    KafkaSchemaRegistryClient client = new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L);

    Schema s1 = client.getSchemaById(1);
    Schema s2 = client.getSchemaById(1);

    assertSame(s1, s2);
    assertEquals(1, requestCount);
  }

  @Test
  void getLatestSchema_expiredCache_refetches() throws Exception {
    // TTL of 0 ms means cache expires immediately
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl, 3000, 0L);

    client.getLatestSchema("orders");
    Thread.sleep(5); // ensure expiry
    client.getLatestSchema("orders");

    assertEquals(2, requestCount, "Expired cache should trigger a second request");
  }

  // ---------------------------------------------------------------------------
  // Error handling
  // ---------------------------------------------------------------------------

  @Test
  void getSchemaById_notFound_throwsException() {
    KafkaSchemaRegistryClient client = new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L);

    Exception ex = assertThrows(Exception.class, () -> client.getSchemaById(99));
    assertTrue(ex.getMessage().contains("404"), "Exception should mention HTTP 404");
  }

  @Test
  void unreachableServer_throwsException() {
    // Port 1 is never listening
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient("http://localhost:1", 500, 60_000L);

    assertThrows(Exception.class, () -> client.getLatestSchema("orders"));
  }

  // ---------------------------------------------------------------------------
  // URL normalisation
  // ---------------------------------------------------------------------------

  @Test
  void trailingSlashInUrl_handledCorrectly() throws Exception {
    // baseUrl with trailing slash should still build correct endpoint paths
    KafkaSchemaRegistryClient client =
        new KafkaSchemaRegistryClient(baseUrl + "/", 3000, 60_000L);

    Schema schema = client.getLatestSchema("orders");
    assertNotNull(schema);
  }

  // ---------------------------------------------------------------------------
  // Nullable union field in schema
  // ---------------------------------------------------------------------------

  @Test
  void schema_nullableUnionField_parsedCorrectly() throws Exception {
    KafkaSchemaRegistryClient client = new KafkaSchemaRegistryClient(baseUrl, 3000, 60_000L);
    Schema schema = client.getLatestSchema("orders");

    Schema.Field amount = schema.getField("amount");
    assertNotNull(amount);
    assertEquals(Schema.Type.UNION, amount.schema().getType());

    long nullCount = amount.schema().getTypes().stream()
        .filter(t -> t.getType() == Schema.Type.NULL)
        .count();
    assertEquals(1, nullCount, "amount field should be a nullable union with null branch");
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static void respond(HttpExchange exchange, int status, String body) throws IOException {
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().set("Content-Type", "application/json");
    exchange.sendResponseHeaders(status, bytes.length);
    try (OutputStream os = exchange.getResponseBody()) {
      os.write(bytes);
    }
  }
}
