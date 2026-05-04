package com.dremio.plugins.neo4j.scan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

/**
 * Carries the Neo4j-specific scan parameters serialized to executor fragments.
 * label is the Neo4j node label (e.g. "Person", "Movie").
 * estimatedRowCount is used for cost estimation.
 */
public class Neo4jScanSpec {

  private final String label;
  private final long   estimatedRowCount;

  @JsonCreator
  public Neo4jScanSpec(
      @JsonProperty("label")             String label,
      @JsonProperty("estimatedRowCount") long   estimatedRowCount) {
    this.label             = label;
    this.estimatedRowCount = estimatedRowCount;
  }

  public Neo4jScanSpec(String label) {
    this(label, 100L);
  }

  @JsonProperty("label")             public String getLabel()             { return label; }
  @JsonProperty("estimatedRowCount") public long   getEstimatedRowCount() { return estimatedRowCount; }

  public List<String> toTablePath() {
    return Collections.singletonList(label);
  }

  @Override
  public String toString() {
    return "Neo4jScanSpec{label='" + label + "', estimatedRows=" + estimatedRowCount + "}";
  }
}
