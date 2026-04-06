package com.dremio.plugins.cassandra.scan;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

/**
 * Metadata for a Cassandra table: Arrow schema + row count estimate.
 *
 * DatasetMetadataImpl is package-private in Dremio, so we implement
 * the DatasetMetadata interface directly.
 */
public class CassandraDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String keyspace;
  private final String tableName;

  public CassandraDatasetMetadata(BatchSchema schema, DatasetStats stats,
                                   String keyspace, String tableName) {
    this.schema = schema;
    this.stats = stats;
    this.keyspace = keyspace;
    this.tableName = tableName;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return stats;
  }

  @Override
  public BatchSchema getRecordSchema() {
    return schema;
  }

  public String getKeyspace() {
    return keyspace;
  }

  public String getTableName() {
    return tableName;
  }
}
