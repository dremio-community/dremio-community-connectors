package com.dremio.plugins.spanner.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Serializable unit of work sent from coordinator to each executor fragment.
 *
 * Carries the plugin ID, table scan specs (one per assigned split), and
 * the projected column list. Serialized as JSON by Jackson.
 *
 * scanSpecs may contain multiple entries on a single-node cluster where all
 * splits are assigned to one fragment. The RecordReader drains each in turn.
 */
public class SpannerSubScan extends SubScanWithProjection {

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
  public static class SpannerScanSpec {

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
              .collect(Collectors.toList()));

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

  private final StoragePluginId     pluginId;
  private final List<SpannerScanSpec> scanSpecs;

  @JsonCreator
  public SpannerSubScan(
      @JsonProperty("props")     OpProps                    props,
      @JsonProperty("schema")    BatchSchema                schema,
      @JsonProperty("tables")    Collection<List<String>>   tables,
      @JsonProperty("columns")   List<SchemaPath>           columns,
      @JsonProperty("pluginId")  StoragePluginId            pluginId,
      @JsonProperty("scanSpecs") List<SpannerScanSpec>      scanSpecs) {
    super(props, schema, tables, columns);
    this.pluginId  = pluginId;
    this.scanSpecs = scanSpecs != null ? scanSpecs : Collections.emptyList();
  }

  @JsonProperty("pluginId")  public StoragePluginId      getPluginId()  { return pluginId; }
  @JsonProperty("scanSpecs") public List<SpannerScanSpec> getScanSpecs() { return scanSpecs; }

  /** Primary scan spec (first in list). */
  @JsonIgnore
  public SpannerScanSpec getScanSpec() {
    return scanSpecs.isEmpty() ? null : scanSpecs.get(0);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new SpannerSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpecs);
  }

  @Override
  public int getOperatorType() { return 0; } // UNKNOWN
}
