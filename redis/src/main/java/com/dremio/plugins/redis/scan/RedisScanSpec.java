package com.dremio.plugins.redis.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;

/**
 * Carries the Redis-specific scan parameters serialized to executor fragments.
 * tableName is the key prefix (e.g. "user" for keys like "user:1").
 * limit is the LIMIT pushdown value (0 = no limit).
 */
public class RedisScanSpec {

  private final String tableName;
  private final long   limit;

  @JsonCreator
  public RedisScanSpec(
      @JsonProperty("tableName") String tableName,
      @JsonProperty("limit")     long   limit) {
    this.tableName = tableName;
    this.limit     = limit;
  }

  public RedisScanSpec(String tableName) {
    this(tableName, 0);
  }

  @JsonProperty("tableName") public String getTableName() { return tableName; }
  @JsonProperty("limit")     public long   getLimit()     { return limit; }

  public boolean hasLimit() { return limit > 0; }

  public RedisScanSpec withLimit(long limitValue) {
    return new RedisScanSpec(tableName, limitValue);
  }

  public List<String> toTablePath() {
    return java.util.Collections.singletonList(tableName);
  }

  @Override
  public String toString() {
    return tableName + (hasLimit() ? "[limit=" + limit + "]" : "");
  }
}
