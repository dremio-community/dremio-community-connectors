package com.dremio.plugins.dynamodb.scan;

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
 * Carries everything an executor needs to execute a DynamoDB scan:
 * which plugin to use, which table + segments to scan, and which columns to project.
 *
 * scanSpecs is a list so that a single fragment can scan multiple DynamoDB
 * parallel-scan segments (the common case on a single-node Dremio cluster where
 * all splits are assigned to the same executor fragment).
 */
public class DynamoDBSubScan extends SubScanWithProjection {

  private final StoragePluginId pluginId;
  private final List<DynamoDBScanSpec> scanSpecs;

  @JsonCreator
  public DynamoDBSubScan(
      @JsonProperty("props")     OpProps                   props,
      @JsonProperty("schema")    BatchSchema               schema,
      @JsonProperty("tables")    Collection<List<String>>  tables,
      @JsonProperty("columns")   List<SchemaPath>          columns,
      @JsonProperty("pluginId")  StoragePluginId           pluginId,
      @JsonProperty("scanSpecs") List<DynamoDBScanSpec>    scanSpecs) {
    super(props, schema, tables, columns);
    this.pluginId  = pluginId;
    this.scanSpecs = scanSpecs != null ? scanSpecs : Collections.emptyList();
  }

  @JsonProperty("pluginId")
  public StoragePluginId getPluginId() { return pluginId; }

  @JsonProperty("scanSpecs")
  public List<DynamoDBScanSpec> getScanSpecs() { return scanSpecs; }

  /** Convenience: primary scan spec (first in list). Not serialized — use getScanSpecs(). */
  @JsonIgnore
  public DynamoDBScanSpec getScanSpec() {
    return scanSpecs.isEmpty() ? null : scanSpecs.get(0);
  }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new DynamoDBSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpecs);
  }

  @Override
  public int getOperatorType() {
    return 0; // UNKNOWN
  }
}
