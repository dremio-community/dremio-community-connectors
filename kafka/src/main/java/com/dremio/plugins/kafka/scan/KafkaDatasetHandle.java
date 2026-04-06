package com.dremio.plugins.kafka.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

/**
 * Identifies a specific Kafka topic to Dremio's metadata subsystem.
 * The EntityPath is [source_name, topic_name].
 */
public class KafkaDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public KafkaDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() {
    return path;
  }

  /** Returns the Kafka topic name (last component of the entity path). */
  public String getTopicName() {
    return path.getComponents().get(path.getComponents().size() - 1);
  }
}
