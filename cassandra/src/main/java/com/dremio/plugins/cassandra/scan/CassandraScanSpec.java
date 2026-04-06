package com.dremio.plugins.cassandra.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Carries the Cassandra-specific scan parameters through Dremio's planning
 * and serialization layers to the executor.
 *
 * Serialized as JSON (Jackson) when the SubScan is distributed across executor nodes.
 * Optionally carries a token range for parallel split reads.
 *
 * <h3>LIMIT pushdown</h3>
 * When {@code limit > 0}, both {@link #toCql(List)} and
 * {@link #toCqlWithPushdown(List, List, List, List, boolean)} append
 * {@code LIMIT N} to the generated CQL statement.
 *
 * <p>For multi-fragment token-range scans the {@code Limit} rel node is
 * kept above the scan in the plan to guarantee a correct final row count;
 * the per-fragment CQL {@code LIMIT} acts as an early-stop hint so each
 * fragment stops scanning its token range as soon as N rows are found,
 * reducing network traffic proportionally.
 *
 * <p>For single-fragment predicate-pushdown scans the CQL {@code LIMIT} is
 * exact, making the {@code Limit} rel node above a no-op.
 */
public class CassandraScanSpec {

  private final String keyspace;
  private final String tableName;

  // Optional token range for parallel splits (null = full table scan)
  private final List<String> partitionKeys; // can be null or empty
  private final Long tokenRangeStart;       // inclusive
  private final Long tokenRangeEnd;         // inclusive

  /**
   * Predicates pushed down by CassandraFilterRule.
   * The RecordReader validates which are actually on partition key columns
   * and only pushes those to CQL. Never null; empty = no pushdown.
   */
  private final List<CassandraPredicate> predicates;

  /**
   * Token-range split parallelism sourced from CassandraStoragePluginConfig.splitParallelism.
   * Carried here so CassandraGroupScan can honour the user-configured value without
   * needing its own reference to the plugin config.
   */
  private final int splitParallelism;

  /**
   * LIMIT pushed down by CassandraLimitRule.
   * {@code 0} means no limit (absent in JSON also deserialises to 0).
   * When positive, {@code LIMIT limit} is appended to generated CQL.
   */
  private final long limit;

  @JsonCreator
  public CassandraScanSpec(
      @JsonProperty("keyspace")          String                   keyspace,
      @JsonProperty("tableName")         String                   tableName,
      @JsonProperty("partitionKeys")     List<String>             partitionKeys,
      @JsonProperty("tokenRangeStart")   Long                     tokenRangeStart,
      @JsonProperty("tokenRangeEnd")     Long                     tokenRangeEnd,
      @JsonProperty("predicates")        List<CassandraPredicate> predicates,
      @JsonProperty("splitParallelism")  int                      splitParallelism,
      @JsonProperty("limit")             long                     limit) {
    this.keyspace         = keyspace;
    this.tableName        = tableName;
    this.partitionKeys    = partitionKeys;
    this.tokenRangeStart  = tokenRangeStart;
    this.tokenRangeEnd    = tokenRangeEnd;
    this.predicates       = (predicates != null) ? predicates : Collections.emptyList();
    this.splitParallelism = (splitParallelism > 0) ? splitParallelism : 8;
    this.limit            = (limit > 0) ? limit : 0;
  }

  // Convenience constructor for full table scan — parallelism defaults to 8
  public CassandraScanSpec(String keyspace, String tableName) {
    this(keyspace, tableName, null, null, null, null, 8, 0);
  }

  // Constructor for full table scan with explicit parallelism
  public CassandraScanSpec(String keyspace, String tableName, int splitParallelism) {
    this(keyspace, tableName, null, null, null, null, splitParallelism, 0);
  }

  // Constructor for token-range splits (no predicates)
  public CassandraScanSpec(String keyspace, String tableName,
                            List<String> partitionKeys,
                            Long tokenRangeStart, Long tokenRangeEnd) {
    this(keyspace, tableName, partitionKeys, tokenRangeStart, tokenRangeEnd, null, 8, 0);
  }

  // Constructor for predicate-pushdown specs (no token range)
  public CassandraScanSpec(String keyspace, String tableName,
                            List<String> partitionKeys,
                            Long tokenRangeStart, Long tokenRangeEnd,
                            List<CassandraPredicate> predicates) {
    this(keyspace, tableName, partitionKeys, tokenRangeStart, tokenRangeEnd, predicates, 8, 0);
  }

  // Constructor used by CassandraFilterRule (with predicates and parallelism)
  public CassandraScanSpec(String keyspace, String tableName,
                            List<String> partitionKeys,
                            Long tokenRangeStart, Long tokenRangeEnd,
                            List<CassandraPredicate> predicates,
                            int splitParallelism) {
    this(keyspace, tableName, partitionKeys, tokenRangeStart, tokenRangeEnd,
        predicates, splitParallelism, 0);
  }

  // -------------------------------------------------------------------------
  // Getters
  // -------------------------------------------------------------------------

  @JsonProperty("keyspace")
  public String getKeyspace() { return keyspace; }

  @JsonProperty("tableName")
  public String getTableName() { return tableName; }

  @JsonProperty("partitionKeys")
  public List<String> getPartitionKeys() { return partitionKeys; }

  @JsonProperty("tokenRangeStart")
  public Long getTokenRangeStart() { return tokenRangeStart; }

  @JsonProperty("tokenRangeEnd")
  public Long getTokenRangeEnd() { return tokenRangeEnd; }

  @JsonProperty("predicates")
  public List<CassandraPredicate> getPredicates() { return predicates; }

  @JsonProperty("splitParallelism")
  public int getSplitParallelism() { return splitParallelism; }

  /** Returns the pushed-down LIMIT value, or {@code 0} if no limit was pushed. */
  @JsonProperty("limit")
  public long getLimit() { return limit; }

  public boolean hasTokenRange() {
    return tokenRangeStart != null && tokenRangeEnd != null
        && partitionKeys != null && !partitionKeys.isEmpty();
  }

  public boolean hasPredicates() {
    return !predicates.isEmpty();
  }

  /** Returns {@code true} if a LIMIT value has been pushed into this spec. */
  public boolean hasLimit() {
    return limit > 0;
  }

  // -------------------------------------------------------------------------
  // Factory / copy
  // -------------------------------------------------------------------------

  /**
   * Returns a copy of this spec with the given {@code LIMIT} value embedded.
   * Used by {@code CassandraLimitRule} to embed the SQL LIMIT into the scan spec
   * so the generated CQL carries a {@code LIMIT N} clause.
   *
   * @param limitValue  the maximum number of rows per CQL query; must be &gt; 0
   */
  public CassandraScanSpec withLimit(long limitValue) {
    return new CassandraScanSpec(
        this.keyspace, this.tableName, this.partitionKeys,
        this.tokenRangeStart, this.tokenRangeEnd,
        this.predicates, this.splitParallelism, limitValue);
  }

  // -------------------------------------------------------------------------
  // CQL builders
  // -------------------------------------------------------------------------

  /**
   * CQL SELECT statement that reads all rows (or a token range) from this table.
   */
  public String toCql() {
    return toCql(null);
  }

  /**
   * CQL SELECT statement with an explicit column list for projection pushdown.
   * Appends a token-range WHERE clause if a token range is set, and a
   * {@code LIMIT N} clause if a limit has been pushed.
   */
  public String toCql(List<String> columns) {
    StringBuilder sb = new StringBuilder("SELECT ");
    if (columns == null || columns.isEmpty()) {
      sb.append("*");
    } else {
      for (int i = 0; i < columns.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append("\"").append(columns.get(i)).append("\"");
      }
    }
    sb.append(" FROM \"").append(keyspace).append("\".\"").append(tableName).append("\"");

    // Append token range if present
    if (tokenRangeStart != null && tokenRangeEnd != null
        && partitionKeys != null && !partitionKeys.isEmpty()) {
      String tokenExpr = buildTokenExpr(partitionKeys);
      sb.append(" WHERE ").append(tokenExpr).append(" >= ").append(tokenRangeStart)
        .append(" AND ").append(tokenExpr).append(" <= ").append(tokenRangeEnd);
    }

    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    return sb.toString();
  }

  private String buildTokenExpr(List<String> keys) {
    StringBuilder sb = new StringBuilder("token(");
    for (int i = 0; i < keys.size(); i++) {
      if (i > 0) sb.append(", ");
      sb.append("\"").append(keys.get(i)).append("\"");
    }
    sb.append(")");
    return sb.toString();
  }

  /**
   * Builds a CQL SELECT using pushed-down predicates instead of a token range.
   *
   * <p>CQL clause order (per Cassandra grammar):
   * <pre>SELECT … FROM … [WHERE …] [LIMIT N] [ALLOW FILTERING]</pre>
   *
   * Handles three tiers of pushed predicates (all optional):
   * <ol>
   *   <li>{@code pkPredicates}     — EQ/IN on all partition key columns (direct lookup)</li>
   *   <li>{@code ckPredicates}     — any op on clustering columns (server-side filter)</li>
   *   <li>{@code indexPredicates}  — any op on SAI/secondary-indexed columns</li>
   * </ol>
   *
   * {@code ALLOW FILTERING} is appended when {@code allowFiltering} is true — required
   * for regular (non-SAI) secondary index predicates without full PK coverage.
   *
   * @param columns          projected column names (null/empty → SELECT *)
   * @param pkPredicates     partition key equality conditions
   * @param ckPredicates     clustering key conditions (appended after PK)
   * @param indexPredicates  SAI / secondary index conditions (appended last)
   * @param allowFiltering   true → append ALLOW FILTERING to the statement
   */
  public String toCqlWithPushdown(List<String> columns,
                                   List<CassandraPredicate> pkPredicates,
                                   List<CassandraPredicate> ckPredicates,
                                   List<CassandraPredicate> indexPredicates,
                                   boolean allowFiltering) {
    StringBuilder sb = new StringBuilder("SELECT ");
    if (columns == null || columns.isEmpty()) {
      sb.append("*");
    } else {
      for (int i = 0; i < columns.size(); i++) {
        if (i > 0) sb.append(", ");
        sb.append("\"").append(columns.get(i)).append("\"");
      }
    }
    sb.append(" FROM \"").append(keyspace).append("\".\"").append(tableName).append("\"");

    List<CassandraPredicate> allPreds = new ArrayList<>();
    allPreds.addAll(pkPredicates);
    allPreds.addAll(ckPredicates);
    allPreds.addAll(indexPredicates);

    if (!allPreds.isEmpty()) {
      sb.append(" WHERE ");
      boolean first = true;
      for (CassandraPredicate p : allPreds) {
        if (!first) sb.append(" AND ");
        sb.append(p.toCqlFragment());
        first = false;
      }
    }

    // CQL grammar: LIMIT comes before ALLOW FILTERING
    if (limit > 0) {
      sb.append(" LIMIT ").append(limit);
    }

    if (allowFiltering) {
      sb.append(" ALLOW FILTERING");
    }

    return sb.toString();
  }

  /**
   * Convenience overload without index predicates or ALLOW FILTERING.
   * Retained for backward compatibility and for the full-PK-coverage case
   * where no extra index predicates need to be appended.
   */
  public String toCqlWithPushdown(List<String> columns,
                                   List<CassandraPredicate> pkPredicates,
                                   List<CassandraPredicate> ckPredicates) {
    return toCqlWithPushdown(columns, pkPredicates, ckPredicates,
        Collections.emptyList(), false);
  }

  /**
   * Returns the table path as a List<String> for SubScanWithProjection's
   * referencedTables parameter.
   */
  public List<String> toTablePath() {
    return Arrays.asList(keyspace, tableName);
  }

  @Override
  public String toString() {
    String limitSuffix = limit > 0 ? ", limit=" + limit : "";
    if (hasPredicates()) {
      return keyspace + "." + tableName + "[predicates=" + predicates + limitSuffix + "]";
    }
    if (hasTokenRange()) {
      return keyspace + "." + tableName
          + "[" + tokenRangeStart + ".." + tokenRangeEnd + limitSuffix + "]";
    }
    return keyspace + "." + tableName + (limit > 0 ? "[limit=" + limit + "]" : "");
  }
}
