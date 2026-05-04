package com.dremio.plugins.redis;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Manages the Jedis connection pool and provides Redis operations
 * used by the plugin and record reader.
 *
 * Tables are discovered by scanning all keys and grouping by the prefix
 * before the first key delimiter (default ":"): key "user:1" → table "user".
 * String keys without a delimiter are exposed as table "_keys".
 */
public class RedisConnection implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(RedisConnection.class);
  static final String STRING_TABLE = "_keys";

  private final RedisConf config;
  private JedisPool pool;

  public RedisConnection(RedisConf config) {
    this.config = config;
  }

  public void connect() {
    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(8);
    poolConfig.setMaxIdle(4);
    poolConfig.setMinIdle(1);
    poolConfig.setTestOnBorrow(true);

    String pwd = (config.password != null && !config.password.isEmpty()) ? config.password : null;
    pool = new JedisPool(poolConfig, config.host, config.port,
        config.connectTimeoutMs, config.socketTimeoutMs,
        pwd, config.database, null);

    // Validate connection
    try (Jedis jedis = pool.getResource()) {
      jedis.ping();
      logger.info("Redis connection established: {}:{}/db{}", config.host, config.port, config.database);
    }
  }

  /**
   * Discovers table names by scanning all keys and extracting the prefix
   * before the first key delimiter.
   */
  public List<String> listTables() {
    Set<String> tables = new LinkedHashSet<>();
    String delimiter = config.keyDelimiter;

    try (Jedis jedis = pool.getResource()) {
      String cursor = "0";
      ScanParams params = new ScanParams().count(config.scanCount);
      do {
        ScanResult<String> result = jedis.scan(cursor, params);
        cursor = result.getCursor();
        for (String key : result.getResult()) {
          int delimPos = key.indexOf(delimiter);
          if (delimPos > 0) {
            tables.add(key.substring(0, delimPos));
          }
          // Skip bare string keys — they can't be represented as a table
        }
      } while (!"0".equals(cursor));
    }

    List<String> sorted = new ArrayList<>(tables);
    Collections.sort(sorted);
    logger.debug("Discovered Redis tables: {}", sorted);
    return sorted;
  }

  /**
   * Returns all keys matching the given table pattern (tableName + delimiter + "*").
   */
  public List<String> scanKeys(String tableName) {
    String pattern = tableName + config.keyDelimiter + "*";
    List<String> keys = new ArrayList<>();

    try (Jedis jedis = pool.getResource()) {
      String cursor = "0";
      ScanParams params = new ScanParams().match(pattern).count(config.scanCount);
      do {
        ScanResult<String> result = jedis.scan(cursor, params);
        cursor = result.getCursor();
        keys.addAll(result.getResult());
      } while (!"0".equals(cursor));
    }

    Collections.sort(keys);
    return keys;
  }

  /**
   * Returns a sample of keys for schema inference.
   */
  public List<String> sampleKeys(String tableName, int maxSamples) {
    String pattern = tableName + config.keyDelimiter + "*";
    List<String> keys = new ArrayList<>();

    try (Jedis jedis = pool.getResource()) {
      String cursor = "0";
      ScanParams params = new ScanParams().match(pattern).count(maxSamples);
      ScanResult<String> result = jedis.scan(cursor, params);
      keys.addAll(result.getResult());
    }

    if (keys.size() > maxSamples) {
      keys = keys.subList(0, maxSamples);
    }
    return keys;
  }

  /**
   * Returns all hash fields for a given key via HGETALL.
   * Returns empty map if the key doesn't exist or is not a hash.
   */
  public Map<String, String> hgetAll(String key) {
    try (Jedis jedis = pool.getResource()) {
      Map<String, String> result = jedis.hgetAll(key);
      return result != null ? result : Collections.emptyMap();
    } catch (Exception e) {
      logger.warn("HGETALL failed for key '{}': {}", key, e.getMessage());
      return Collections.emptyMap();
    }
  }

  /**
   * Samples hash data from a table's keys for schema inference.
   * Returns a list of maps (one per key), where each map contains field→value pairs.
   * The special "_id" field is added to represent the key suffix.
   */
  public List<Map<String, String>> sampleHashes(String tableName, int maxSamples) {
    List<String> keys = sampleKeys(tableName, maxSamples);
    List<Map<String, String>> samples = new ArrayList<>(keys.size());
    String delimiter = config.keyDelimiter;
    int prefixLen = tableName.length() + delimiter.length();

    try (Jedis jedis = pool.getResource()) {
      for (String key : keys) {
        Map<String, String> hash = jedis.hgetAll(key);
        if (hash == null || hash.isEmpty()) continue;
        // Add the _id field (key suffix)
        Map<String, String> row = new LinkedHashMap<>();
        row.put("_id", key.length() > prefixLen ? key.substring(prefixLen) : key);
        row.putAll(hash);
        samples.add(row);
      }
    }
    return samples;
  }

  /** Approximate key count for a table (uses SCAN count). */
  public long countKeys(String tableName) {
    return scanKeys(tableName).size();
  }

  @Override
  public void close() {
    if (pool != null && !pool.isClosed()) {
      pool.close();
      logger.info("Redis connection pool closed");
    }
  }
}
