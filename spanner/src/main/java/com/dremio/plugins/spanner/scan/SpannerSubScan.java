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
