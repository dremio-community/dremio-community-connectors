package com.dremio.plugins.spanner.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

import java.util.List;

/** Handle for a single Spanner table in Dremio's catalog. */
public class SpannerDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public SpannerDatasetHandle(EntityPath path) {
    this.path = path;
  }

  @Override
  public EntityPath getDatasetPath() { return path; }

  /** Returns the bare table name (last component of the path). */
  public String getTableName() {
    List<String> components = path.getComponents();
    return components.get(components.size() - 1);
  }
}
