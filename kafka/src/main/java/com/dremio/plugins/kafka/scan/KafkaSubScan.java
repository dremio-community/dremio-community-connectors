package com.dremio.plugins.kafka.scan;

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
 * Serializable unit of work sent from the planner/coordinator to each executor fragment.
 * Carries everything an executor needs to open one or more Kafka partition readers:
 * which plugin to use, which partitions (with frozen offsets), and which columns to project.
 *
 * Jackson-annotated for JSON serialization across the fragment RPC boundary.
 */
public class KafkaSubScan extends SubScanWithProjection {

  private final StoragePluginId pluginId;
  private final KafkaScanSpec scanSpec;     // primary spec (first partition in this fragment)
  private final List<KafkaScanSpec> scanSpecs; // all partition specs for this fragment

  @JsonCreator
  public KafkaSubScan(
      @JsonProperty("props")      OpProps                 props,
      @JsonProperty("schema")     BatchSchema             schema,
      @JsonProperty("tables")     Collection<List<String>> tables,
      @JsonProperty("columns")    List<SchemaPath>        columns,
      @JsonProperty("pluginId")   StoragePluginId         pluginId,
      @JsonProperty("scanSpec")   KafkaScanSpec           scanSpec,
      @JsonProperty("scanSpecs")  List<KafkaScanSpec>     scanSpecs) {
    super(props, schema, tables, columns);
    this.pluginId  = pluginId;
    this.scanSpec  = scanSpec;
    this.scanSpecs = (scanSpecs != null && !scanSpecs.isEmpty())
        ? scanSpecs : Collections.singletonList(scanSpec);
  }

  @JsonProperty("pluginId")
  public StoragePluginId getPluginId() { return pluginId; }

  @JsonProperty("scanSpec")
  public KafkaScanSpec getScanSpec() { return scanSpec; }

  @JsonProperty("scanSpecs")
  public List<KafkaScanSpec> getScanSpecs() { return scanSpecs; }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> physicalVisitor, X value)
      throws E {
    return physicalVisitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new KafkaSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpec, scanSpecs);
  }

  @Override
  public int getOperatorType() {
    return 0; // UNKNOWN
  }
}
