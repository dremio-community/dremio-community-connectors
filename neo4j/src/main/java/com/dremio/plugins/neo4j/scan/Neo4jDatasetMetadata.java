package com.dremio.plugins.neo4j.scan;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

public class Neo4jDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String label;

  public Neo4jDatasetMetadata(BatchSchema schema, DatasetStats stats, String label) {
    this.schema = schema;
    this.stats  = stats;
    this.label  = label;
  }

  @Override public DatasetStats getDatasetStats()   { return stats; }
  @Override public BatchSchema  getRecordSchema()   { return schema; }
  @Override public BytesOutput  getExtraInfo()      { return BytesOutput.NONE; }

  public String getLabel() { return label; }
}
