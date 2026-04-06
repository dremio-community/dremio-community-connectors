package com.dremio.plugins.cassandra.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

/**
 * Identifies a specific Cassandra table to Dremio's metadata subsystem.
 * The EntityPath is [source_name, keyspace, table].
 */
public class CassandraDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public CassandraDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() {
    return path;
  }

  public String getKeyspace() {
    return path.getComponents().get(1);
  }

  public String getTableName() {
    return path.getComponents().get(2);
  }
}
