package com.dremio.plugins.googleads.scan;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.connector.metadata.BytesOutput;
import com.dremio.exec.record.BatchSchema;

public class GoogleAdsDatasetMetadata implements DatasetMetadata {
  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String tableName;

  public GoogleAdsDatasetMetadata(BatchSchema schema, DatasetStats stats, String tableName) {
    this.schema    = schema;
    this.stats     = stats;
    this.tableName = tableName;
  }

  @Override public BatchSchema getRecordSchema()  { return schema; }
  @Override public DatasetStats getDatasetStats() { return stats; }
  @Override public BytesOutput  getExtraInfo()    { return BytesOutput.NONE; }

  public String getTableName() { return tableName; }
}
