package com.dremio.plugins.redis.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

public class RedisDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public RedisDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() {
    return path;
  }

  public String getTableName() {
    return path.getComponents().get(path.getComponents().size() - 1);
  }
}
