package com.dremio.plugins.pubsub.scan;

import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

/**
 * Metadata for a Pub/Sub subscription: Arrow schema + row count estimate.
 *
 * The schema always contains the standard metadata columns
 * (_subscription, _message_id, _publish_time, _ordering_key, _attributes, _value_raw).
 * In JSON mode, additionally contains inferred payload field columns.
 */
public class PubSubDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String subscriptionName;

  public PubSubDatasetMetadata(BatchSchema schema, DatasetStats stats, String subscriptionName) {
    this.schema           = schema;
    this.stats            = stats;
    this.subscriptionName = subscriptionName;
  }

  @Override
  public DatasetStats getDatasetStats() {
    return stats;
  }

  @Override
  public BatchSchema getRecordSchema() {
    return schema;
  }

  public String getSubscriptionName() {
    return subscriptionName;
  }
}
