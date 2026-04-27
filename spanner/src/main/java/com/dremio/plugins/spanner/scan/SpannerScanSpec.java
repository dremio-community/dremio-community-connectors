package com.dremio.plugins.spanner.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Carries Spanner-specific scan parameters through Dremio's planning and
 * serialization layers to the executor.
 *
 * Serialized as JSON (Jackson) when the SubScan is distributed to executor fragments.
 *
 * Fields:
 *   tableName    — fully-qualified Spanner table name
 *   segment      — 0-based index of this parallel split
 *   totalSegments — total number of parallel splits for this scan
 *   columns      — projected column names (empty = SELECT *)
 *   filters      — SQL WHERE fragments pushed from Dremio's filter layer
 *   limit        — LIMIT value pushed from Dremio (0 = no limit)
 */
public class SpannerScanSpec {

  private final String       tableName;
  private final int          segment;
  private final int          totalSegments;
  private final List<String> columns;
  private final List<String> filters;
  private final long         limit;

  @JsonCreator
  public SpannerScanSpec(
      @JsonProperty("tableName")     String       tableName,
      @JsonProperty("segment")       int          segment,
      @JsonProperty("totalSegments") int          totalSegments,
      @JsonProperty("columns")       List<String> columns,
      @JsonProperty("filters")       List<String> filters,
      @JsonProperty("limit")         long         limit) {
    this.tableName     = tableName;
    this.segment       = segment;
    this.totalSegments = Math.max(1, totalSegments);
    this.columns       = (columns != null) ? columns : Collections.emptyList();
    this.filters       = (filters != null) ? filters : Collections.emptyList();
    this.limit         = (limit > 0) ? limit : 0;
  }

  /** Full-table scan with no predicates or limit. */
  public SpannerScanSpec(String tableName) {
    this(tableName, 0, 1, null, null, 0);
  }

  /** Full-table scan with split index. */
  public SpannerScanSpec(String tableName, int segment, int totalSegments) {
    this(tableName, segment, totalSegments, null, null, 0);
  }

  @JsonProperty("tableName")     public String       getTableName()     { return tableName; }
  @JsonProperty("segment")       public int          getSegment()       { return segment; }
  @JsonProperty("totalSegments") public int          getTotalSegments() { return totalSegments; }
  @JsonProperty("columns")       public List<String> getColumns()       { return columns; }
  @JsonProperty("filters")       public List<String> getFilters()       { return filters; }
  @JsonProperty("limit")         public long         getLimit()         { return limit; }

  public boolean hasFilters() { return !filters.isEmpty(); }
  public boolean hasLimit()   { return limit > 0; }

  /** Builds the SQL query for this scan spec. */
  public String toSql() {
    String colList = columns.isEmpty() ? "*"
        : String.join(", ", columns.stream().map(c -> "`" + c + "`")
            .collect(java.util.stream.Collectors.toList()));

    StringBuilder sb = new StringBuilder("SELECT ").append(colList)
        .append(" FROM `").append(tableName).append("`");

    if (hasFilters()) {
      sb.append(" WHERE ").append(String.join(" AND ", filters));
    }
    if (hasLimit()) {
      sb.append(" LIMIT ").append(limit);
    }
    return sb.toString();
  }

  /** Returns the table path as a list for SubScanWithProjection. */
  public List<String> toTablePath() {
    return Collections.singletonList(tableName);
  }

  /** Returns a copy with the given LIMIT embedded. */
  public SpannerScanSpec withLimit(long limitValue) {
    return new SpannerScanSpec(tableName, segment, totalSegments, columns, filters, limitValue);
  }

  /** Returns a copy with the given filter fragments embedded. */
  public SpannerScanSpec withFilters(List<String> newFilters) {
    return new SpannerScanSpec(tableName, segment, totalSegments, columns, newFilters, limit);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder(tableName)
        .append("[seg=").append(segment).append("/").append(totalSegments).append("]");
    if (hasFilters()) sb.append("[filter:").append(filters).append("]");
    if (hasLimit())   sb.append("[limit=").append(limit).append("]");
    return sb.toString();
  }
}
