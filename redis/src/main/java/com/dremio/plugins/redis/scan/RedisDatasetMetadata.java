package com.dremio.plugins.redis.scan;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

public class RedisDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String tableName;

  public RedisDatasetMetadata(BatchSchema schema, DatasetStats stats, String tableName) {
    this.schema    = schema;
    this.stats     = stats;
    this.tableName = tableName;
  }

  @Override public DatasetStats getDatasetStats()   { return stats; }
  @Override public BatchSchema  getRecordSchema()   { return schema; }
  @Override public BytesOutput  getExtraInfo()      { return BytesOutput.NONE; }

  public String getTableName() { return tableName; }
}
