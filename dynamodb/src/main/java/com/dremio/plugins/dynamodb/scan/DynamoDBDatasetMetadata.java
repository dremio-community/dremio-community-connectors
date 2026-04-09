package com.dremio.plugins.dynamodb.scan;

import com.dremio.connector.metadata.BytesOutput;
import com.dremio.connector.metadata.DatasetMetadata;
import com.dremio.connector.metadata.DatasetStats;
import com.dremio.exec.record.BatchSchema;

import java.nio.charset.StandardCharsets;

/**
 * Carries the Arrow schema and row-count statistics for a DynamoDB table.
 * Produced by DynamoDBStoragePlugin.getDatasetMetadata().
 *
 * Also stores the table's partition key name (pkName) and optional sort key
 * name (skName) so they can be recovered during query planning via
 * TableMetadata.getReadDefinition().getExtendedProperty().
 *
 * Extra bytes format (stored in ReadDefinition.extendedProperty):
 *   JSON object: {"pkName":"<pk>","skName":"<sk>"}
 *   skName is omitted when the table has no sort key.
 */
public class DynamoDBDatasetMetadata implements DatasetMetadata {

  private final BatchSchema schema;
  private final DatasetStats stats;
  private final String tableName;
  private final String pkName;   // partition key attribute name (may be null)
  private final String skName;   // sort key attribute name (may be null)

  public DynamoDBDatasetMetadata(BatchSchema schema, DatasetStats stats, String tableName,
                                  String pkName, String skName) {
    this.schema    = schema;
    this.stats     = stats;
    this.tableName = tableName;
    this.pkName    = pkName;
    this.skName    = skName;
  }

  /** Backward-compatible constructor (no key metadata). */
  public DynamoDBDatasetMetadata(BatchSchema schema, DatasetStats stats, String tableName) {
    this(schema, stats, tableName, null, null);
  }

  @Override
  public DatasetStats getDatasetStats() {
    return stats;
  }

  @Override
  public BatchSchema getRecordSchema() {
    return schema;
  }

  public String getTableName() { return tableName; }
  public String getPkName()    { return pkName; }
  public String getSkName()    { return skName; }

  /**
   * Serializes pk/sk names as a compact JSON byte string.
   * Dremio stores the result in ReadDefinition.extendedProperty, where it can be
   * recovered during query planning in DynamoDBScanRule.convertScan().
   *
   * Format: {"pkName":"<pk>","skName":"<sk>"}
   * skName key is omitted when null.
   */
  @Override
  public BytesOutput getExtraInfo() {
    if (pkName == null) return BytesOutput.NONE;
    StringBuilder json = new StringBuilder("{\"pkName\":\"");
    json.append(escapeJson(pkName)).append("\"");
    if (skName != null) {
      json.append(",\"skName\":\"").append(escapeJson(skName)).append("\"");
    }
    json.append("}");
    byte[] bytes = json.toString().getBytes(StandardCharsets.UTF_8);
    return os -> os.write(bytes);
  }

  /** Minimal JSON string escaping for attribute names (which are typically simple identifiers). */
  private static String escapeJson(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
