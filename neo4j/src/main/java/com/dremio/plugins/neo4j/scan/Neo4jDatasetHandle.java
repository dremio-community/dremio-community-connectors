package com.dremio.plugins.neo4j.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

public class Neo4jDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public Neo4jDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() {
    return path;
  }

  public String getLabel() {
    return path.getComponents().get(path.getComponents().size() - 1);
  }
}
