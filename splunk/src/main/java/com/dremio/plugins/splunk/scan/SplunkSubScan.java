package com.dremio.plugins.splunk.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.catalog.StoragePluginId;
import com.dremio.exec.physical.base.OpProps;
import com.dremio.exec.physical.base.PhysicalOperator;
import com.dremio.exec.physical.base.PhysicalVisitor;
import com.dremio.exec.physical.base.SubScanWithProjection;
import com.dremio.exec.record.BatchSchema;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Serializable unit of work sent from planner/coordinator to each executor fragment.
 * Carries everything needed to open a Splunk search and read its results:
 * which plugin, which scan spec (SPL + time bounds), and which columns to project.
 *
 * Jackson-annotated for serialization across the fragment RPC boundary.
 */
public class SplunkSubScan extends SubScanWithProjection {

  private final StoragePluginId     pluginId;
  private final SplunkScanSpec      scanSpec;
  private final List<SplunkScanSpec> scanSpecs;

  @JsonCreator
  public SplunkSubScan(
      @JsonProperty("props")     OpProps                  props,
      @JsonProperty("schema")    BatchSchema              schema,
      @JsonProperty("tables")    Collection<List<String>> tables,
      @JsonProperty("columns")   List<SchemaPath>         columns,
      @JsonProperty("pluginId")  StoragePluginId          pluginId,
      @JsonProperty("scanSpec")  SplunkScanSpec           scanSpec,
      @JsonProperty("scanSpecs") List<SplunkScanSpec>     scanSpecs) {
    super(props, schema, tables, columns);
    this.pluginId  = pluginId;
    this.scanSpec  = scanSpec;
    this.scanSpecs = (scanSpecs != null && !scanSpecs.isEmpty())
        ? scanSpecs : Collections.singletonList(scanSpec);
  }

  @JsonProperty("pluginId")
  public StoragePluginId getPluginId() { return pluginId; }

  @JsonProperty("scanSpec")
  public SplunkScanSpec getScanSpec() { return scanSpec; }

  @JsonProperty("scanSpecs")
  public List<SplunkScanSpec> getScanSpecs() { return scanSpecs; }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new SplunkSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpec, scanSpecs);
  }

  @Override
  public int getOperatorType() { return 0; }
}
