package com.dremio.plugins.dynamodb;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClientBuilder;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages the DynamoDbClient connection and provides high-level DynamoDB operations
 * used by the Dremio storage plugin.
 */
public class DynamoDBConnection implements AutoCloseable {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDBConnection.class);

  private final DynamoDbClient client;

  public DynamoDBConnection(DynamoDBStoragePluginConfig config) {
    DynamoDbClientBuilder builder = DynamoDbClient.builder()
        .region(Region.of(config.region))
        .httpClient(UrlConnectionHttpClient.builder()
            .connectionTimeout(Duration.ofSeconds(config.readTimeoutSeconds))
            .socketTimeout(Duration.ofSeconds(config.readTimeoutSeconds))
            .build());

    // Credentials: explicit keys or default chain (IAM/instance profile)
    if (config.accessKeyId != null && !config.accessKeyId.isEmpty()
        && config.secretAccessKey != null && !config.secretAccessKey.isEmpty()) {
      builder.credentialsProvider(StaticCredentialsProvider.create(
          AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey)));
    } else {
      builder.credentialsProvider(DefaultCredentialsProvider.create());
    }

    // Endpoint override for DynamoDB Local / VPC endpoints
    if (config.endpointOverride != null && !config.endpointOverride.isEmpty()) {
      builder.endpointOverride(URI.create(config.endpointOverride));
    }

    this.client = builder.build();
  }

  // -----------------------------------------------------------------------
  // Table listing / description
  // -----------------------------------------------------------------------

  /**
   * Returns all table names visible to this connection.
   * DynamoDB's ListTables is paginated (max 100 per call); we collect all pages.
   */
  public List<String> listTables() {
    List<String> tables = new ArrayList<>();
    String lastEvaluatedTableName = null;
    do {
      ListTablesRequest.Builder reqBuilder = ListTablesRequest.builder().limit(100);
      if (lastEvaluatedTableName != null) {
        reqBuilder.exclusiveStartTableName(lastEvaluatedTableName);
      }
      ListTablesResponse resp = client.listTables(reqBuilder.build());
      tables.addAll(resp.tableNames());
      lastEvaluatedTableName = resp.lastEvaluatedTableName();
    } while (lastEvaluatedTableName != null);
    return tables;
  }

  /**
   * Describes a single table, returning its TableDescription.
   */
  public TableDescription describeTable(String tableName) {
    return client.describeTable(DescribeTableRequest.builder()
        .tableName(tableName)
        .build())
        .table();
  }

  /**
   * Returns the approximate item count for a table from its description.
   * This is an eventually-consistent estimate updated roughly every 6 hours.
   */
  public long getApproximateItemCount(String tableName) {
    try {
      Long count = describeTable(tableName).itemCount();
      return (count != null && count > 0) ? count : 10_000L;
    } catch (Exception e) {
      logger.debug("Could not get item count for {}: {}", tableName, e.getMessage());
      return 10_000L;
    }
  }

  /**
   * Returns the partition key attribute name for a table.
   */
  public String getPartitionKeyName(String tableName) {
    try {
      TableDescription desc = describeTable(tableName);
      for (KeySchemaElement el : desc.keySchema()) {
        if (el.keyType() == KeyType.HASH) {
          return el.attributeName();
        }
      }
    } catch (Exception e) {
      logger.debug("Could not get partition key for {}: {}", tableName, e.getMessage());
    }
    return null;
  }

  /**
   * Returns the sort key attribute name for a table, or null if none.
   */
  public String getSortKeyName(String tableName) {
    try {
      TableDescription desc = describeTable(tableName);
      for (KeySchemaElement el : desc.keySchema()) {
        if (el.keyType() == KeyType.RANGE) {
          return el.attributeName();
        }
      }
    } catch (Exception e) {
      logger.debug("Could not get sort key for {}: {}", tableName, e.getMessage());
    }
    return null;
  }

  // -----------------------------------------------------------------------
  // Schema inference
  // -----------------------------------------------------------------------

  /**
   * Samples up to {@code limit} items from a table by doing a quick Scan.
   * Used for schema inference — not for production reads.
   */
  public List<Map<String, AttributeValue>> sampleItems(String tableName, int limit) {
    ScanRequest req = ScanRequest.builder()
        .tableName(tableName)
        .limit(limit)
        .build();
    try {
      ScanResponse resp = client.scan(req);
      return resp.items();
    } catch (Exception e) {
      logger.warn("Could not sample items from {}: {}", tableName, e.getMessage());
      return new ArrayList<>();
    }
  }

  // -----------------------------------------------------------------------
  // Data reads
  // -----------------------------------------------------------------------

  /**
   * Returns a paginated Scan iterable for a specific parallel scan segment.
   * DynamoDB divides the table into {@code totalSegments} partitions;
   * this call reads only segment {@code segment} (0-based).
   *
   * When {@code projectionColumns} is non-empty, a ProjectionExpression is sent
   * to DynamoDB to reduce response payload size.
   */
  public ScanIterable scan(String tableName, int segment, int totalSegments, int pageSize,
                            String filterExpression,
                            Map<String, String> expressionAttributeNames,
                            Map<String, AttributeValue> expressionAttributeValues,
                            List<String> projectionColumns,
                            long limit) {
    ScanRequest.Builder b = ScanRequest.builder()
        .tableName(tableName)
        .segment(segment)
        .totalSegments(totalSegments)
        .limit(pageSize);

    // Merged ExpressionAttributeNames map (filter + projection share the same map)
    Map<String, String> mergedNames = new HashMap<>();
    if (expressionAttributeNames != null) mergedNames.putAll(expressionAttributeNames);

    if (filterExpression != null && !filterExpression.isEmpty()) {
      b.filterExpression(filterExpression);
      if (expressionAttributeValues != null && !expressionAttributeValues.isEmpty()) {
        b.expressionAttributeValues(expressionAttributeValues);
      }
    }

    if (projectionColumns != null && !projectionColumns.isEmpty()) {
      StringBuilder proj = new StringBuilder();
      for (int i = 0; i < projectionColumns.size(); i++) {
        if (i > 0) proj.append(", ");
        proj.append("#proj").append(i);
        mergedNames.put("#proj" + i, projectionColumns.get(i));
      }
      b.projectionExpression(proj.toString());
    }

    if (!mergedNames.isEmpty()) {
      b.expressionAttributeNames(mergedNames);
    }

    return client.scanPaginator(b.build());
  }

  /**
   * Returns a paginated Query iterable using a partition key equality condition.
   * Used when a partition key EQ predicate is pushed down — much more efficient
   * than a full table Scan.
   *
   * {@code sortKeyPredicate} is optional.  When non-null it is appended to the
   * KeyConditionExpression so DynamoDB applies sort-key filtering on the server
   * before returning items.  Supported operators: EQ, GT, GTE, LT, LTE.
   *
   * {@code projectionColumns} is optional.  When non-empty, a ProjectionExpression
   * is added to reduce response payload size.
   */
  public QueryIterable query(String tableName,
                              String pkName,
                              AttributeValue pkValue,
                              int pageSize,
                              String filterExpression,
                              Map<String, String> expressionAttributeNames,
                              Map<String, AttributeValue> expressionAttributeValues,
                              com.dremio.plugins.dynamodb.scan.DynamoDBPredicate sortKeyPredicate,
                              List<String> projectionColumns) {
    Map<String, String>       exprNames  = new HashMap<>();
    Map<String, AttributeValue> exprValues = new HashMap<>();
    if (expressionAttributeNames  != null) exprNames.putAll(expressionAttributeNames);
    if (expressionAttributeValues != null) exprValues.putAll(expressionAttributeValues);

    // Partition key condition (always required for Query)
    exprNames.put("#pk", pkName);
    exprValues.put(":pkval", pkValue);
    StringBuilder keyCondition = new StringBuilder("#pk = :pkval");

    // Sort key condition (optional; appended to KeyConditionExpression)
    if (sortKeyPredicate != null) {
      String skAlias   = "#sk";
      String skValAlias = ":skval";
      exprNames.put(skAlias, sortKeyPredicate.getColumn());
      AttributeValue skAttrVal = sortKeyPredicate.isString()
          ? AttributeValue.builder().s(sortKeyPredicate.getValue()).build()
          : AttributeValue.builder().n(sortKeyPredicate.getValue()).build();
      exprValues.put(skValAlias, skAttrVal);
      keyCondition.append(" AND ")
                  .append(skAlias)
                  .append(" ")
                  .append(sortKeyPredicate.getOp().toDynamoOp())
                  .append(" ")
                  .append(skValAlias);
    }

    QueryRequest.Builder b = QueryRequest.builder()
        .tableName(tableName)
        .keyConditionExpression(keyCondition.toString())
        .expressionAttributeNames(exprNames)
        .expressionAttributeValues(exprValues)
        .limit(pageSize);

    if (filterExpression != null && !filterExpression.isEmpty()) {
      b.filterExpression(filterExpression);
    }

    // Projection expression (reduces response payload)
    if (projectionColumns != null && !projectionColumns.isEmpty()) {
      StringBuilder proj = new StringBuilder();
      for (int i = 0; i < projectionColumns.size(); i++) {
        if (i > 0) proj.append(", ");
        proj.append("#proj").append(i);
        exprNames.put("#proj" + i, projectionColumns.get(i));
      }
      b.projectionExpression(proj.toString());
      // exprNames is the same map passed to expressionAttributeNames above;
      // re-set it to include the new projection aliases
      b.expressionAttributeNames(exprNames);
    }

    return client.queryPaginator(b.build());
  }

  public DynamoDbClient getClient() {
    return client;
  }

  @Override
  public void close() {
    try {
      client.close();
    } catch (Exception e) {
      logger.warn("Error closing DynamoDB client: {}", e.getMessage());
    }
  }
}
