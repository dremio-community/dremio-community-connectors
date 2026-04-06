package com.dremio.plugins.cassandra.scan;

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
import java.util.List;

/**
 * Serializable unit of work sent from the planner/coordinator to each executor
 * fragment. Carries everything an executor needs to open a Cassandra query:
 * which plugin to use, which table to scan, and which columns to project.
 *
 * Jackson-annotated for JSON serialization across the fragment RPC boundary.
 */
public class CassandraSubScan extends SubScanWithProjection {

  private final StoragePluginId pluginId;
  private final CassandraScanSpec scanSpec;
  /**
   * All token-range specs assigned to this executor fragment.
   * Multiple specs arise when several splits are co-located on a single executor node
   * (common in single-node deployments). The RecordReader executes each spec in sequence.
   */
  private final List<CassandraScanSpec> scanSpecs;

  @JsonCreator
  public CassandraSubScan(
      @JsonProperty("props") OpProps props,
      @JsonProperty("schema") BatchSchema schema,
      @JsonProperty("tables") Collection<List<String>> tables,
      @JsonProperty("columns") List<SchemaPath> columns,
      @JsonProperty("pluginId") StoragePluginId pluginId,
      @JsonProperty("scanSpec") CassandraScanSpec scanSpec,
      @JsonProperty("scanSpecs") List<CassandraScanSpec> scanSpecs) {
    super(props, schema, tables, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
    this.scanSpecs = (scanSpecs != null && !scanSpecs.isEmpty()) ? scanSpecs
        : java.util.Collections.singletonList(scanSpec);
  }

  @JsonProperty("pluginId")
  public StoragePluginId getPluginId() {
    return pluginId;
  }

  @JsonProperty("scanSpec")
  public CassandraScanSpec getScanSpec() {
    return scanSpec;
  }

  @JsonProperty("scanSpecs")
  public List<CassandraScanSpec> getScanSpecs() {
    return scanSpecs;
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new CassandraSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpec, scanSpecs);
  }

  @Override
  public int getOperatorType() {
    return 0; // UNKNOWN — see CassandraGroupScan for explanation
  }
}
