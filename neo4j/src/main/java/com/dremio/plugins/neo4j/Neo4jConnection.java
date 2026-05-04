package com.dremio.plugins.neo4j;

import org.neo4j.driver.AuthTokens;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Logging;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Manages the Neo4j Bolt driver connection and provides graph database operations
 * used by the plugin and record reader.
 *
 * Tables = node labels. Schema is inferred by sampling node properties.
 * All Cypher label names are backtick-quoted for safety.
 *
 * Uses Neo4j Java Driver 4.4.x (Java 11 compatible).
 */
public class Neo4jConnection implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(Neo4jConnection.class);

  // Neo4j driver 4.x type name constants
  private static final String TYPE_INTEGER = "INTEGER";
  private static final String TYPE_FLOAT   = "FLOAT";
  private static final String TYPE_BOOLEAN = "BOOLEAN";

  private final Neo4jConf conf;
  private Driver driver;

  public Neo4jConnection(Neo4jConf conf) {
    this.conf = conf;
  }

  public void connect() throws IOException {
    try {
      Config config = Config.builder()
          .withConnectionTimeout(conf.connectionTimeoutSeconds, TimeUnit.SECONDS)
          .withMaxConnectionPoolSize(5)
          .withLogging(Logging.slf4j())
          .build();
      driver = GraphDatabase.driver(
          conf.boltUri,
          AuthTokens.basic(conf.username, conf.password != null ? conf.password : ""),
          config);
      verify();
      logger.info("Neo4j connection established: {}", conf.boltUri);
    } catch (IOException ioe) {
      throw ioe;
    } catch (Exception e) {
      throw new IOException("Failed to connect to Neo4j at " + conf.boltUri + ": " + e.getMessage(), e);
    }
  }

  /**
   * Verifies connectivity by running a simple Cypher statement.
   */
  public void verify() throws IOException {
    try (Session session = openSession()) {
      session.run("RETURN 1").consume();
    } catch (Exception e) {
      throw new IOException("Neo4j connectivity check failed: " + e.getMessage(), e);
    }
  }

  /**
   * Lists all node labels in the database, sorted alphabetically.
   */
  public List<String> listLabels() {
    List<String> labels = new ArrayList<>();
    try (Session session = openSession()) {
      Result result = session.run(
          "CALL db.labels() YIELD label RETURN label ORDER BY label");
      while (result.hasNext()) {
        Record rec = result.next();
        labels.add(rec.get("label").asString());
      }
    }
    logger.debug("Discovered Neo4j labels: {}", labels);
    return labels;
  }

  /**
   * Infers Arrow schema from a sample of nodes with the given label.
   * Returns a LinkedHashMap sorted by property key name.
   */
  public Map<String, ArrowType> inferSchema(String label, int sampleSize) {
    // Collect all property values per key across samples
    Map<String, List<Value>> propValues = new LinkedHashMap<>();

    try (Session session = openSession()) {
      String cypher = "MATCH (n:`" + label + "`) RETURN n LIMIT " + sampleSize;
      Result result = session.run(cypher);
      while (result.hasNext()) {
        Record record = result.next();
        Value nodeValue = record.get("n");
        if (nodeValue == null || nodeValue.isNull()) continue;

        // Get all property keys from the node
        for (String key : nodeValue.asNode().keys()) {
          Value v = nodeValue.asNode().get(key);
          if (v == null || v.isNull()) continue;
          propValues.computeIfAbsent(key, k -> new ArrayList<>()).add(v);
        }
      }
    }

    // Sort keys alphabetically for stable schema
    List<String> sortedKeys = new ArrayList<>(propValues.keySet());
    Collections.sort(sortedKeys);

    Map<String, ArrowType> schema = new LinkedHashMap<>();
    for (String key : sortedKeys) {
      List<Value> values = propValues.get(key);
      ArrowType type = inferType(values);
      schema.put(key, type);
    }

    logger.debug("Inferred schema for label '{}': {} fields", label, schema.size());
    return schema;
  }

  /**
   * Fetches rows for the given label using SKIP/LIMIT pagination.
   * Returns a list of property maps (Value objects) for the requested columns.
   */
  public List<Map<String, Object>> fetchRows(String label, List<String> columns,
                                              long skip, long limit) {
    List<Map<String, Object>> rows = new ArrayList<>();
    try (Session session = openSession()) {
      String cypher = "MATCH (n:`" + label + "`) RETURN n SKIP " + skip + " LIMIT " + limit;
      Result result = session.run(cypher);
      while (result.hasNext()) {
        Record record = result.next();
        Value nodeValue = record.get("n");
        if (nodeValue == null || nodeValue.isNull()) continue;

        Map<String, Object> row = new HashMap<>();
        for (String col : columns) {
          Value v = nodeValue.asNode().get(col);
          if (v == null || v.isNull()) {
            row.put(col, null);
          } else {
            row.put(col, v);
          }
        }
        rows.add(row);
      }
    }
    return rows;
  }

  /**
   * Estimates the number of nodes with the given label.
   */
  public long estimateCount(String label) {
    try (Session session = openSession()) {
      Result result = session.run(
          "MATCH (n:`" + label + "`) RETURN count(n) AS cnt");
      if (result.hasNext()) {
        return result.next().get("cnt").asLong();
      }
    } catch (Exception e) {
      logger.warn("Failed to estimate count for label '{}': {}", label, e.getMessage());
    }
    return 100L;
  }

  private Session openSession() {
    if (conf.database != null && !conf.database.isEmpty()) {
      SessionConfig sessionConfig = SessionConfig.builder()
          .withDatabase(conf.database)
          .build();
      return driver.session(sessionConfig);
    }
    return driver.session();
  }

  /**
   * Infers the Arrow type from a list of non-null Neo4j Values using type name comparison.
   * Neo4j 4.x driver: value.type().name() returns "INTEGER", "FLOAT", "BOOLEAN", etc.
   *
   * - All INTEGER → BigInt
   * - All FLOAT or INTEGER (at least one FLOAT) → Float8
   * - All BOOLEAN → Bool
   * - Otherwise → Utf8
   */
  private ArrowType inferType(List<Value> values) {
    if (values == null || values.isEmpty()) return ArrowType.Utf8.INSTANCE;

    boolean allInteger = true;
    boolean allNumeric = true;
    boolean allBoolean = true;
    boolean hasFloat   = false;

    for (Value v : values) {
      if (v == null || v.isNull()) continue;

      String typeName = v.type().name();
      boolean isInt   = TYPE_INTEGER.equals(typeName);
      boolean isFloat = TYPE_FLOAT.equals(typeName);
      boolean isBool  = TYPE_BOOLEAN.equals(typeName);

      if (!isInt)              allInteger = false;
      if (!isInt && !isFloat)  allNumeric = false;
      if (isFloat)             hasFloat   = true;
      if (!isBool)             allBoolean = false;
    }

    if (allBoolean)                  return ArrowType.Bool.INSTANCE;
    if (allInteger)                  return new ArrowType.Int(64, true);
    if (allNumeric && hasFloat)      return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
    return ArrowType.Utf8.INSTANCE;
  }

  @Override
  public void close() {
    if (driver != null) {
      try {
        driver.close();
        logger.info("Neo4j driver closed");
      } catch (Exception e) {
        logger.warn("Error closing Neo4j driver: {}", e.getMessage());
      }
    }
  }
}
