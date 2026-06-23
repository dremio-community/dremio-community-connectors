package com.dremio.plugins.googleads.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

public class GoogleAdsDatasetHandle implements DatasetHandle {
  private final EntityPath entityPath;

  public GoogleAdsDatasetHandle(EntityPath entityPath) {
    this.entityPath = entityPath;
  }

  @Override
  public EntityPath getDatasetPath() {
    return entityPath;
  }

  public String getTableName() {
    java.util.List<String> c = entityPath.getComponents();
    return c.get(c.size() - 1);
  }
}
