package com.dremio.plugins.kafka.scan;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

/**
 * Metadata for a Kafka topic: Arrow schema + row count estimate.
 *
 * The schema always contains the standard metadata columns
 * (_topic, _partition, _offset, _timestamp, _timestamp_type, _key, _headers, _value_raw).
 * In JSON mode, additionally contains inferred payload field columns.
 */
public class KafkaDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String topicName;

  public KafkaDatasetMetadata(BatchSchema schema, DatasetStats stats, String topicName) {
    this.schema    = schema;
    this.stats     = stats;
    this.topicName = topicName;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return stats;
  }

  @Override
  public BatchSchema getRecordSchema() {
    return schema;
  }

  public String getTopicName() {
    return topicName;
  }
}
