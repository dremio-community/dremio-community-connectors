package com.dremio.plugins.splunk.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

/**
 * Identifies a specific Splunk index to Dremio's metadata subsystem.
 * The EntityPath is [source_name, index_name].
 */
public class SplunkDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public SplunkDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() {
    return path;
  }

  /** Returns the Splunk index name (last component of the entity path). */
  public String getIndexName() {
    return path.getComponents().get(path.getComponents().size() - 1);
  }
}
