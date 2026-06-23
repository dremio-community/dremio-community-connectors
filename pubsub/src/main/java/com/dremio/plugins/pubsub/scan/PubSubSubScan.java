package com.dremio.plugins.pubsub.scan;

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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 * Serializable unit of work sent from the planner/coordinator to each executor fragment.
 * Carries everything an executor needs to open a Pub/Sub RecordReader.
 *
 * Jackson-annotated for JSON serialization across the fragment RPC boundary.
 */
public class PubSubSubScan extends SubScanWithProjection {

  private final StoragePluginId pluginId;
  private final PubSubScanSpec  scanSpec;

  @JsonCreator
  public PubSubSubScan(
      @JsonProperty("props")    OpProps                  props,
      @JsonProperty("schema")   BatchSchema              schema,
      @JsonProperty("tables")   Collection<List<String>> tables,
      @JsonProperty("columns")  List<SchemaPath>         columns,
      @JsonProperty("pluginId") StoragePluginId          pluginId,
      @JsonProperty("scanSpec") PubSubScanSpec           scanSpec) {
    super(props, schema, tables, columns);
    this.pluginId = pluginId;
    this.scanSpec = scanSpec;
  }

  @JsonProperty("pluginId")
  public StoragePluginId getPluginId() { return pluginId; }

  @JsonProperty("scanSpec")
  public PubSubScanSpec getScanSpec() { return scanSpec; }

  @Override
  public <T, X, E extends Throwable> T accept(PhysicalVisitor<T, X, E> visitor, X value) throws E {
    return visitor.visitSubScan(this, value);
  }

  @Override
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children)
      throws ExecutionSetupException {
    return new PubSubSubScan(getProps(), getFullSchema(), getReferencedTables(),
        getColumns(), pluginId, scanSpec);
  }

  @Override
  public int getOperatorType() { return 0; }

  // ---------------------------------------------------------------------------
  // Inner class: PubSubScanSpec
  // ---------------------------------------------------------------------------

  /**
   * Carries all parameters needed to execute one Pub/Sub subscription scan.
   *
   * Pub/Sub has no partitions or offsets, so the spec is intentionally simple:
   * just the subscription name, how many messages to pull, and the schema mode.
   *
   * Pull-without-ack semantics: the RecordReader pulls up to maxMessages, returns
   * them as rows, then NACKs all ack IDs so messages stay available for other
   * consumers. Every Dremio query against this table starts fresh from the
   * subscription's current backlog.
   */
  public static class PubSubScanSpec {

    private final String subscription;
    private final int    maxMessages;
    private final String schemaMode;

    @JsonCreator
    public PubSubScanSpec(
        @JsonProperty("subscription") String subscription,
        @JsonProperty("maxMessages")  int    maxMessages,
        @JsonProperty("schemaMode")   String schemaMode) {
      this.subscription = subscription;
      this.maxMessages  = maxMessages;
      this.schemaMode   = (schemaMode != null) ? schemaMode : "JSON";
    }

    @JsonProperty("subscription")
    public String getSubscription() { return subscription; }

    @JsonProperty("maxMessages")
    public int getMaxMessages() { return maxMessages; }

    @JsonProperty("schemaMode")
    public String getSchemaMode() { return schemaMode; }

    /** Encodes as pipe-delimited string for split storage. */
    public String toExtendedProperty() {
      return subscription + "|" + maxMessages + "|" + schemaMode;
    }

    /** Decodes a split extended property string back into a PubSubScanSpec. */
    public static PubSubScanSpec fromExtendedProperty(String encoded) {
      if (encoded == null || encoded.isEmpty()) return null;
      String[] parts = encoded.split("\\|", 3);
      if (parts.length >= 3) {
        try {
          return new PubSubScanSpec(parts[0], Integer.parseInt(parts[1]), parts[2]);
        } catch (NumberFormatException ignore) { }
      }
      if (parts.length == 1) {
        return new PubSubScanSpec(parts[0], 1000, "JSON");
      }
      return null;
    }

    /** Returns the table path as a List for SubScanWithProjection. */
    public List<String> toTablePath() {
      return Arrays.asList(subscription);
    }

    @Override
    public String toString() {
      return subscription + "[maxMessages=" + maxMessages + ", " + schemaMode + "]";
    }
  }
}
