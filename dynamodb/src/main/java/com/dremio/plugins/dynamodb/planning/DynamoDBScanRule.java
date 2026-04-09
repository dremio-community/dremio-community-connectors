package com.dremio.plugins.dynamodb.planning;

import com.dremio.exec.calcite.logical.ScanCrel;
import com.dremio.exec.planner.logical.Rel;
import com.dremio.exec.store.common.SourceLogicalConverter;
import com.dremio.plugins.dynamodb.DynamoDBStoragePluginConfig;
import com.dremio.plugins.dynamodb.scan.DynamoDBScanSpec;
import org.apache.calcite.plan.RelOptRuleCall;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Converts a generic ScanCrel into a DynamoDBScanDrel during LOGICAL planning.
 * SourceLogicalConverter matches ScanCrel nodes for AMAZON_DYNAMODB sources.
 *
 * Also extracts the table's partition key name (pkName) and sort key name
 * (skName) from the extended property bytes stored in ReadDefinition by
 * DynamoDBDatasetMetadata.getExtraInfo().  These are stored in the initial
 * DynamoDBScanSpec so DynamoDBFilterRule can route predicates correctly at
 * plan time without needing to look up the plugin.
 */
public class DynamoDBScanRule extends SourceLogicalConverter {

  public static final DynamoDBScanRule INSTANCE = new DynamoDBScanRule();

  private static final Logger logger = LoggerFactory.getLogger(DynamoDBScanRule.class);

  private DynamoDBScanRule() {
    super(DynamoDBStoragePluginConfig.class);
  }

  @Override
  public Rel convertScan(ScanCrel scan) {
    List<String> qualifiedName = scan.getTable().getQualifiedName();
    String tableName = qualifiedName.get(qualifiedName.size() - 1);

    // Extract pk/sk from stored metadata extra bytes (populated by
    // DynamoDBDatasetMetadata.getExtraInfo() → ReadDefinition.extendedProperty).
    // Returns null for both if the metadata has never been refreshed or if lookup
    // fails — safe fallback (filter rule will use FilterExpression instead of Query).
    String[] keyNames = extractKeyNames(scan);
    String pkName = keyNames[0];
    String skName = keyNames[1];

    DynamoDBScanSpec spec = new DynamoDBScanSpec(tableName, 0, 1, pkName, skName);
    return new DynamoDBScanDrel(
        scan.getCluster(),
        scan.getTraitSet().replace(Rel.LOGICAL),
        scan.getTable(), scan.getPluginId(),
        scan.getTableMetadata(), scan.getProjectedColumns(),
        scan.getObservedRowcountAdjustment(), scan.getHints(), spec);
  }

  /**
   * Reads pk/sk names from ReadDefinition.extendedProperty (JSON bytes written
   * by DynamoDBDatasetMetadata.getExtraInfo()).
   *
   * Format: {"pkName":"<pk>","skName":"<sk>"}
   *
   * @return String[2] — [pkName, skName]; either may be null on error or absence.
   */
  private static String[] extractKeyNames(ScanCrel scan) {
    try {
      com.dremio.exec.store.TableMetadata tm = scan.getTableMetadata();
      if (tm == null) return new String[]{null, null};
      com.dremio.service.namespace.dataset.proto.ReadDefinition rd = tm.getReadDefinition();
      if (rd == null) return new String[]{null, null};
      // ByteString is a protobuf type not always on compile classpath; access via reflection
      Object ep = rd.getExtendedProperty();
      if (ep == null) return new String[]{null, null};
      byte[] bytes = (byte[]) ep.getClass().getMethod("toByteArray").invoke(ep);
      if (bytes == null || bytes.length == 0) return new String[]{null, null};
      // Simple manual JSON extraction (avoids ObjectMapper dependency in planning layer)
      String json = new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
      String pkName = extractJsonStringField(json, "pkName");
      String skName = extractJsonStringField(json, "skName");
      if (pkName != null) {
        logger.debug("Resolved key metadata for table: pkName='{}', skName='{}'", pkName, skName);
      }
      return new String[]{pkName, skName};
    } catch (Exception e) {
      logger.debug("Could not extract key metadata from table metadata: {}", e.getMessage());
      return new String[]{null, null};
    }
  }

  /**
   * Minimal JSON string field extractor.  Handles the simple flat JSON format
   * {"pkName":"value","skName":"value"} produced by DynamoDBDatasetMetadata.
   */
  static String extractJsonStringField(String json, String fieldName) {
    String key = "\"" + fieldName + "\":\"";
    int start = json.indexOf(key);
    if (start < 0) return null;
    start += key.length();
    int end = json.indexOf('"', start);
    if (end < 0) return null;
    String value = json.substring(start, end);
    // Unescape \" and \\
    return value.replace("\\\"", "\"").replace("\\\\", "\\");
  }
}
