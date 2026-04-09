package com.dremio.plugins.dynamodb.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Carries the DynamoDB-specific scan parameters through Dremio's planning
 * and serialization layers to the executor.
 *
 * Serialized as JSON (Jackson) when the SubScan is distributed to executors.
 *
 * Two scan modes:
 *   SCAN  — parallel segment scan (segment/totalSegments)
 *   QUERY — partition key equality lookup (pkPredicate present)
 *
 * pkName / skName are table key metadata set at plan time (from stored metadata
 * extra bytes) and used by DynamoDBFilterRule to route predicates correctly.
 * They are not needed at execution time.
 *
 * sortKeyPredicate goes into KeyConditionExpression (Query mode only).
 * Other predicates go into FilterExpression.
 */
public class DynamoDBScanSpec {

  private final String tableName;

  // Parallel Scan parameters
  private final int segment;       // 0-based segment index for this executor
  private final int totalSegments; // total number of parallel scan segments

  // Predicates pushed from Dremio's filter layer (→ FilterExpression)
  private final List<DynamoDBPredicate> predicates;

  // Partition key EQ predicate — when set, use Query API instead of Scan
  private final DynamoDBPredicate pkPredicate;

  // Sort key predicate for Query mode (→ KeyConditionExpression, optional)
  private final DynamoDBPredicate sortKeyPredicate;

  // LIMIT pushed from Dremio's limit layer (0 = no limit)
  private final long limit;

  // Table key metadata (set at plan time from stored metadata bytes; null = unknown)
  private final String pkName;
  private final String skName;

  @JsonCreator
  public DynamoDBScanSpec(
      @JsonProperty("tableName")        String                  tableName,
      @JsonProperty("segment")          int                     segment,
      @JsonProperty("totalSegments")    int                     totalSegments,
      @JsonProperty("predicates")       List<DynamoDBPredicate> predicates,
      @JsonProperty("pkPredicate")      DynamoDBPredicate       pkPredicate,
      @JsonProperty("sortKeyPredicate") DynamoDBPredicate       sortKeyPredicate,
      @JsonProperty("limit")            long                    limit,
      @JsonProperty("pkName")           String                  pkName,
      @JsonProperty("skName")           String                  skName) {
    this.tableName        = tableName;
    this.segment          = segment;
    this.totalSegments    = Math.max(1, totalSegments);
    this.predicates       = (predicates != null) ? predicates : Collections.emptyList();
    this.pkPredicate      = pkPredicate;
    this.sortKeyPredicate = sortKeyPredicate;
    this.limit            = (limit > 0) ? limit : 0;
    this.pkName           = pkName;
    this.skName           = skName;
  }

  /** Full-table scan spec (no predicates, no limit, key metadata unknown). */
  public DynamoDBScanSpec(String tableName, int segment, int totalSegments) {
    this(tableName, segment, totalSegments, null, null, null, 0, null, null);
  }

  /** Full-table scan spec with key metadata set at plan time. */
  public DynamoDBScanSpec(String tableName, int segment, int totalSegments,
                           String pkName, String skName) {
    this(tableName, segment, totalSegments, null, null, null, 0, pkName, skName);
  }

  @JsonProperty("tableName")        public String                  getTableName()        { return tableName; }
  @JsonProperty("segment")          public int                     getSegment()          { return segment; }
  @JsonProperty("totalSegments")    public int                     getTotalSegments()    { return totalSegments; }
  @JsonProperty("predicates")       public List<DynamoDBPredicate> getPredicates()       { return predicates; }
  @JsonProperty("pkPredicate")      public DynamoDBPredicate       getPkPredicate()      { return pkPredicate; }
  @JsonProperty("sortKeyPredicate") public DynamoDBPredicate       getSortKeyPredicate() { return sortKeyPredicate; }
  @JsonProperty("limit")            public long                    getLimit()            { return limit; }
  @JsonProperty("pkName")           public String                  getPkName()           { return pkName; }
  @JsonProperty("skName")           public String                  getSkName()           { return skName; }

  public boolean hasPredicates()       { return !predicates.isEmpty(); }
  public boolean hasPkPredicate()      { return pkPredicate != null; }
  public boolean hasSortKeyPredicate() { return sortKeyPredicate != null; }
  public boolean hasLimit()            { return limit > 0; }

  /** Returns a copy with the given LIMIT embedded. */
  public DynamoDBScanSpec withLimit(long limitValue) {
    return new DynamoDBScanSpec(tableName, segment, totalSegments,
        predicates, pkPredicate, sortKeyPredicate, limitValue, pkName, skName);
  }

  /** Returns a copy with predicates, pkPredicate, and sortKeyPredicate embedded. */
  public DynamoDBScanSpec withPredicates(List<DynamoDBPredicate> newPredicates,
                                          DynamoDBPredicate newPkPredicate,
                                          DynamoDBPredicate newSortKeyPredicate) {
    return new DynamoDBScanSpec(tableName, segment, totalSegments,
        newPredicates, newPkPredicate, newSortKeyPredicate, limit, pkName, skName);
  }

  /** Returns the table path as [tableName] for SubScanWithProjection. */
  public List<String> toTablePath() {
    return java.util.Collections.singletonList(tableName);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(tableName);
    if (hasPkPredicate()) {
      sb.append("[query:").append(pkPredicate);
      if (hasSortKeyPredicate()) sb.append(",sk:").append(sortKeyPredicate);
      sb.append("]");
    } else {
      sb.append("[seg=").append(segment).append("/").append(totalSegments).append("]");
    }
    if (hasPredicates()) sb.append("[filter:").append(predicates).append("]");
    if (hasLimit())      sb.append("[limit=").append(limit).append("]");
    return sb.toString();
  }
}
