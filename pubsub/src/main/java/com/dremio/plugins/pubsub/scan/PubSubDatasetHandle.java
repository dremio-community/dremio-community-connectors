package com.dremio.plugins.pubsub.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

/**
 * Identifies a specific Pub/Sub subscription to Dremio's metadata subsystem.
 * The EntityPath is [source_name, subscription_name].
 */
public class PubSubDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public PubSubDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() {
    return path;
  }

  /** Returns the Pub/Sub subscription name (last component of the entity path). */
  public String getSubscriptionName() {
    return path.getComponents().get(path.getComponents().size() - 1);
  }
}
