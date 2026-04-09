package com.dremio.plugins.dynamodb.scan;

import com.dremio.connector.metadata.DatasetHandle;
import com.dremio.connector.metadata.EntityPath;

/**
 * Identifies a specific DynamoDB table to Dremio's metadata subsystem.
 * The EntityPath is [source_name, table_name] — DynamoDB has no keyspace concept.
 */
public class DynamoDBDatasetHandle implements DatasetHandle {

  private final EntityPath path;

  public DynamoDBDatasetHandle(EntityPath path) {
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
