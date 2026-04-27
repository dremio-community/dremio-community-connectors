package com.dremio.plugins.spanner.scan;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

/** Carries the Arrow schema and row-count estimate for a Spanner table. */
public class SpannerDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String tableName;

  public SpannerDatasetMetadata(BatchSchema schema, DatasetStats stats, String tableName) {
    this.schema    = schema;
    this.stats     = stats;
    this.tableName = tableName;
  }

  @Override public BatchSchema getRecordSchema()  { return schema; }
  @Override public DatasetStats getDatasetStats()  { return stats; }

  public String getTableName() { return tableName; }
}
