package com.dremio.plugins.neo4j.scan;

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
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Collection;
import java.util.List;

@JsonTypeName("neo4j-sub-scan")
public class Neo4jSubScan extends SubScanWithProjection {

  public static final int OPERATOR_TYPE = 5006;

  private final StoragePluginId pluginId;
  private final Neo4jScanSpec   scanSpec;

  @JsonCreator
  public Neo4jSubScan(
      @JsonProperty("props")    OpProps                  props,
      @JsonProperty("schema")   BatchSchema              schema,
      @JsonProperty("tables")   Collection<List<String>> tables,
      @JsonProperty("columns")  List<SchemaPath>         columns,
      @JsonProperty("pluginId") StoragePluginId          pluginId,
      @JsonProperty("scanSpec") Neo4jScanSpec            scanSpec) {
    super(props, schema, tables, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @JsonProperty("pluginId") public StoragePluginId getPluginId() { return pluginId; }
  @JsonProperty("scanSpec") public Neo4jScanSpec   getScanSpec()  { return scanSpec; }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new Neo4jSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpec);
  }

  @Override
  public int getOperatorType() { return OPERATOR_TYPE; }
}
