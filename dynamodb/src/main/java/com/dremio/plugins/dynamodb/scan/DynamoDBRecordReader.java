package com.dremio.plugins.dynamodb.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.dynamodb.DynamoDBConnection;
import com.dremio.plugins.dynamodb.DynamoDBStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.paginators.ScanIterable;
import software.amazon.awssdk.services.dynamodb.paginators.QueryIterable;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.stream.Collectors;

/**
 * Reads items from a DynamoDB table and writes them into Dremio Arrow vectors.
 *
 * Data flow:
 *   setup()  → build DynamoDB Scan or Query request, start paginator, allocate vectors
 *   next()   → fill up to TARGET_BATCH_SIZE items into Arrow vectors per call
 *   close()  → nothing (connection owned by plugin)
 *
 * Scan mode (default):
 *   Uses DynamoDB Parallel Scan with Segment/TotalSegments. Each executor fragment
 *   reads one segment. FilterExpression is applied when predicates are present.
 *
 * Query mode (partition key EQ predicate pushed):
 *   Uses DynamoDB Query API with KeyConditionExpression on the partition key
 *   (and optionally the sort key). Much more efficient — only reads matching partition(s).
 *
 * Projection pushdown:
 *   When the query projects a subset of columns, the projected column names are sent
 *   to DynamoDB as a ProjectionExpression to reduce response payload size.
 *
 * Native collection types:
 *   SS (String Set) → Arrow ListVector<VarChar>
 *   NS (Number Set) → Arrow ListVector<Float8>
 *   L  (List)       → VarChar (JSON-serialized; element types too variable to schema-infer)
 *   M  (Map)        → VarChar (JSON-serialized; key/type set varies per item)
 */
public class DynamoDBRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(DynamoDBRecordReader.class);
  private static final int TARGET_BATCH_SIZE = 4_000;

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final DynamoDBStoragePlugin plugin;
  private final DynamoDBSubScan subScan;

  // Schema: projected fields from this scan's schema
  private List<Field> projectedFields;
  // Arrow vectors, one per projected field (same order)
  private List<ValueVector> vectors;

  // Paginated item source — supports multiple scan segments per fragment.
  // On single-node Dremio all splits are assigned to one executor fragment, so we
  // drain each segment's iterator in turn via the specQueue.
  private final Queue<Map<String, AttributeValue>> itemBuffer = new ArrayDeque<>();
  private final Queue<DynamoDBScanSpec> specQueue = new ArrayDeque<>();
  private Iterator<ScanResponse>  scanPageIterator;
  private Iterator<QueryResponse> queryPageIterator;
  private boolean scanMode; // true=Scan, false=Query (set from first spec)
  private boolean done = false;

  public DynamoDBRecordReader(DynamoDBStoragePlugin plugin,
                               DynamoDBSubScan subScan,
                               OperatorContext context) {
    super(context, subScan.getColumns());
    this.plugin  = plugin;
    this.subScan = subScan;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    // Look up pre-allocated vectors from Dremio's output container.
    // We must NOT call addField() — Dremio pre-allocates vectors before
    // calling setup(), and calling addField() triggers a schema-change
    // false-positive (identical schemas compare unequal due to metadata).
    List<Field> schemaFields = subScan.getFullSchema().getFields();

    projectedFields = new ArrayList<>();
    vectors = new ArrayList<>();
    for (Field field : schemaFields) {
      ValueVector v = output.getVector(field.getName());
      if (v == null) continue; // not projected (e.g. COUNT(*))
      projectedFields.add(field);
      vectors.add(v);
    }

    // All scan specs for this fragment (may be >1 on single-node clusters
    // where all splits are assigned to the same executor fragment)
    List<DynamoDBScanSpec> specs = subScan.getScanSpecs();
    if (specs == null || specs.isEmpty()) {
      done = true;
      return;
    }

    DynamoDBScanSpec firstSpec = specs.get(0);
    scanMode = !firstSpec.hasPkPredicate();

    // Enqueue remaining specs; the first one is started immediately below
    specQueue.addAll(specs.subList(1, specs.size()));
    startSpec(firstSpec);
  }

  /** Builds the list of projected column names for ProjectionExpression. */
  private List<String> buildProjectionColumns() {
    if (projectedFields.isEmpty()) return null; // COUNT(*) or no projection — let DynamoDB return all
    return projectedFields.stream().map(Field::getName).collect(Collectors.toList());
  }

  /** Opens the DynamoDB iterator for a single ScanSpec. */
  private void startSpec(DynamoDBScanSpec spec) throws ExecutionSetupException {
    DynamoDBConnection conn = plugin.getConnection();
    int pageSize = plugin.getConfig().maxPageSize;

    Map<String, String>        exprNames  = new LinkedHashMap<>();
    Map<String, AttributeValue> exprValues = new LinkedHashMap<>();
    String filterExpr = buildFilterExpression(spec.getPredicates(), exprNames, exprValues);

    List<String> projColumns = buildProjectionColumns();

    if (spec.hasPkPredicate()) {
      scanMode = false;
      DynamoDBPredicate pk = spec.getPkPredicate();
      AttributeValue pkAttrVal = pk.isString()
          ? AttributeValue.builder().s(pk.getValue()).build()
          : AttributeValue.builder().n(pk.getValue()).build();

      QueryIterable qi = conn.query(
          spec.getTableName(), pk.getColumn(), pkAttrVal,
          pageSize, filterExpr, exprNames, exprValues,
          spec.getSortKeyPredicate(),
          projColumns);
      queryPageIterator = qi.iterator();
      scanPageIterator  = null;
      logger.debug("DynamoDB Query mode: table={}, pk={}={}, sk={}, proj={}",
          spec.getTableName(), pk.getColumn(), pk.getValue(),
          spec.getSortKeyPredicate(), projColumns != null ? projColumns.size() + " cols" : "all");
    } else {
      scanMode = true;
      ScanIterable si = conn.scan(
          spec.getTableName(),
          spec.getSegment(),
          spec.getTotalSegments(),
          pageSize,
          filterExpr, exprNames, exprValues,
          projColumns,
          spec.getLimit());
      scanPageIterator  = si.iterator();
      queryPageIterator = null;
      logger.debug("DynamoDB Scan mode: table={}, segment={}/{}, filter={}, proj={}",
          spec.getTableName(), spec.getSegment(), spec.getTotalSegments(),
          filterExpr != null && !filterExpr.isEmpty() ? filterExpr : "none",
          projColumns != null ? projColumns.size() + " cols" : "all");
    }
  }

  @Override
  public int next() {
    int count = 0;
    // Use the limit from the first spec (same for all specs of the same table)
    DynamoDBScanSpec firstSpec = subScan.getScanSpec();
    long limitRemaining = (firstSpec != null && firstSpec.hasLimit())
        ? firstSpec.getLimit() : Long.MAX_VALUE;

    while (count < TARGET_BATCH_SIZE && count < limitRemaining && !done) {
      if (itemBuffer.isEmpty()) {
        if (!fetchNextPage()) {
          // Current spec exhausted — try advancing to the next one
          if (specQueue.isEmpty()) {
            done = true;
            break;
          }
          try {
            startSpec(specQueue.poll());
          } catch (ExecutionSetupException e) {
            throw new RuntimeException("Failed to start next DynamoDB scan segment", e);
          }
          continue;
        }
        if (itemBuffer.isEmpty()) continue;
      }

      Map<String, AttributeValue> item = itemBuffer.poll();
      if (item == null) continue;

      writeItem(item, count);
      count++;
    }

    for (ValueVector v : vectors) {
      v.setValueCount(count);
    }
    return count;
  }

  /** Fetches the next page from the current iterator into itemBuffer. */
  private boolean fetchNextPage() {
    if (scanMode) {
      if (scanPageIterator == null || !scanPageIterator.hasNext()) return false;
      ScanResponse page = scanPageIterator.next();
      if (page.items() != null) itemBuffer.addAll(page.items());
      return true;
    } else {
      if (queryPageIterator == null || !queryPageIterator.hasNext()) return false;
      QueryResponse page = queryPageIterator.next();
      if (page.items() != null) itemBuffer.addAll(page.items());
      return true;
    }
  }

  /** Writes one DynamoDB item at position {@code idx} into all projected vectors. */
  private void writeItem(Map<String, AttributeValue> item, int idx) {
    for (int i = 0; i < projectedFields.size(); i++) {
      Field field = projectedFields.get(i);
      AttributeValue av = item.get(field.getName());
      writeAttributeValue(vectors.get(i), idx, av, field.getType());
    }
  }

  /**
   * Writes an AttributeValue into the appropriate Arrow vector at position {@code idx}.
   * If the value is null or DynamoDB NULL, the position is left unset (null in Arrow).
   */
  private void writeAttributeValue(ValueVector vector, int idx,
                                    AttributeValue av, ArrowType arrowType) {
    if (av == null || (av.nul() != null && av.nul())) {
      return; // null → leave slot unset
    }

    if (vector instanceof ListVector) {
      writeListVector((ListVector) vector, idx, av);
    } else if (vector instanceof VarCharVector) {
      String str = extractAsString(av);
      if (str != null) {
        ((VarCharVector) vector).setSafe(idx, str.getBytes(StandardCharsets.UTF_8));
      }
    } else if (vector instanceof Float8Vector) {
      if (av.n() != null) {
        try {
          ((Float8Vector) vector).setSafe(idx, Double.parseDouble(av.n()));
        } catch (NumberFormatException ignore) {}
      } else if (av.s() != null) {
        try {
          ((Float8Vector) vector).setSafe(idx, Double.parseDouble(av.s()));
        } catch (NumberFormatException ignore) {}
      }
    } else if (vector instanceof BigIntVector) {
      if (av.n() != null) {
        try {
          ((BigIntVector) vector).setSafe(idx, Long.parseLong(av.n()));
        } catch (NumberFormatException e) {
          try {
            ((BigIntVector) vector).setSafe(idx, (long) Double.parseDouble(av.n()));
          } catch (NumberFormatException ignore) {}
        }
      }
    } else if (vector instanceof BitVector) {
      if (av.bool() != null) {
        ((BitVector) vector).setSafe(idx, av.bool() ? 1 : 0);
      }
    } else if (vector instanceof VarBinaryVector) {
      if (av.b() != null) {
        ((VarBinaryVector) vector).setSafe(idx, av.b().asByteArray());
      }
    }
  }

  /**
   * Writes a DynamoDB collection attribute (SS or NS) into a ListVector.
   * SS → child VarCharVector; NS → child Float8Vector.
   * L and M types fall through to VarChar JSON serialization before reaching this.
   */
  private void writeListVector(ListVector lv, int idx, AttributeValue av) {
    if (av.hasSs() && av.ss() != null && !av.ss().isEmpty()) {
      // String Set → List<VarChar>
      List<String> values = av.ss();
      int startOffset = lv.startNewValue(idx);
      ValueVector child = lv.getDataVector();
      if (child instanceof VarCharVector) {
        VarCharVector cv = (VarCharVector) child;
        for (int i = 0; i < values.size(); i++) {
          String s = values.get(i);
          if (s != null) cv.setSafe(startOffset + i, s.getBytes(StandardCharsets.UTF_8));
        }
      }
      lv.endValue(idx, values.size());

    } else if (av.hasNs() && av.ns() != null && !av.ns().isEmpty()) {
      // Number Set → List<Float8>
      List<String> values = av.ns();
      int startOffset = lv.startNewValue(idx);
      ValueVector child = lv.getDataVector();
      if (child instanceof Float8Vector) {
        Float8Vector fv = (Float8Vector) child;
        for (int i = 0; i < values.size(); i++) {
          try {
            fv.setSafe(startOffset + i, Double.parseDouble(values.get(i)));
          } catch (NumberFormatException ignore) {}
        }
      }
      lv.endValue(idx, values.size());

    } else {
      // Empty or unrecognized — write empty list
      lv.startNewValue(idx);
      lv.endValue(idx, 0);
    }
  }

  /**
   * Converts any AttributeValue to its String representation.
   * Used for VarChar vectors that hold S, N, L, M, SS, NS, BS values.
   */
  private String extractAsString(AttributeValue av) {
    if (av.s() != null)             return av.s();
    if (av.n() != null)             return av.n();
    if (av.bool() != null)          return av.bool().toString();
    if (av.hasL() && av.l() != null) {
      return toJsonArray(av.l());
    }
    if (av.hasM() && av.m() != null) {
      return toJsonObject(av.m());
    }
    if (av.hasSs() && av.ss() != null) {
      try { return MAPPER.writeValueAsString(av.ss()); } catch (Exception e) { return av.ss().toString(); }
    }
    if (av.hasNs() && av.ns() != null) {
      try { return MAPPER.writeValueAsString(av.ns()); } catch (Exception e) { return av.ns().toString(); }
    }
    if (av.hasBs() && av.bs() != null) {
      // base64-encode each binary element
      List<String> encoded = new ArrayList<>();
      for (software.amazon.awssdk.core.SdkBytes b : av.bs()) {
        encoded.add(java.util.Base64.getEncoder().encodeToString(b.asByteArray()));
      }
      try { return MAPPER.writeValueAsString(encoded); } catch (Exception e) { return encoded.toString(); }
    }
    return null;
  }

  /** Serializes a DynamoDB List to a JSON array string. */
  private String toJsonArray(List<AttributeValue> list) {
    StringBuilder sb = new StringBuilder("[");
    for (int i = 0; i < list.size(); i++) {
      if (i > 0) sb.append(",");
      String elem = extractAsString(list.get(i));
      if (elem == null) {
        sb.append("null");
      } else {
        AttributeValue av = list.get(i);
        if (av.s() != null || av.hasL() || av.hasM() || av.hasSs() || av.hasNs() || av.hasBs()) {
          sb.append("\"").append(elem.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        } else {
          sb.append(elem);
        }
      }
    }
    sb.append("]");
    return sb.toString();
  }

  /** Serializes a DynamoDB Map to a JSON object string. */
  private String toJsonObject(Map<String, AttributeValue> map) {
    StringBuilder sb = new StringBuilder("{");
    boolean first = true;
    for (Map.Entry<String, AttributeValue> e : map.entrySet()) {
      if (!first) sb.append(",");
      first = false;
      sb.append("\"").append(e.getKey().replace("\"", "\\\"")).append("\":");
      String val = extractAsString(e.getValue());
      if (val == null) {
        sb.append("null");
      } else {
        AttributeValue av = e.getValue();
        if (av.s() != null || av.hasL() || av.hasM() || av.hasSs() || av.hasNs() || av.hasBs()) {
          sb.append("\"").append(val.replace("\\", "\\\\").replace("\"", "\\\"")).append("\"");
        } else {
          sb.append(val);
        }
      }
    }
    sb.append("}");
    return sb.toString();
  }

  /**
   * Builds a DynamoDB FilterExpression string from a list of predicates.
   * Populates {@code nameMap} and {@code valueMap} with the corresponding
   * ExpressionAttributeNames and ExpressionAttributeValues entries.
   */
  private String buildFilterExpression(List<DynamoDBPredicate> predicates,
                                        Map<String, String> nameMap,
                                        Map<String, AttributeValue> valueMap) {
    if (predicates == null || predicates.isEmpty()) return null;
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < predicates.size(); i++) {
      if (i > 0) sb.append(" AND ");
      DynamoDBPredicate p = predicates.get(i);
      sb.append(p.toFilterFragment(i));
      p.addToExpressionMaps(i, nameMap, valueMap);
    }
    return sb.toString();
  }

  @Override
  public void close() throws Exception {
    // Connection is owned by the plugin; nothing to close here
  }
}
