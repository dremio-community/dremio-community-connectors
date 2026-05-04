package com.dremio.plugins.neo4j.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.neo4j.Neo4jConnection;
import com.dremio.plugins.neo4j.Neo4jStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.neo4j.driver.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads Neo4j node data using SKIP/LIMIT pagination and writes into Dremio Arrow vectors.
 *
 * Flow:
 *   setup()  → resolve projected vectors from output mutator
 *   next()   → fetch fetchBatchSize rows via SKIP/LIMIT, write to vectors
 *   close()  → no-op (connection owned by plugin)
 *
 * Each call to next() fetches the next page from Neo4j using SKIP/LIMIT.
 */
public class Neo4jRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(Neo4jRecordReader.class);

  private final Neo4jStoragePlugin plugin;
  private final Neo4jSubScan       subScan;

  private List<Field>       projectedFields;
  private List<ValueVector> vectors;

  private long skip = 0;
  private boolean done = false;

  public Neo4jRecordReader(Neo4jStoragePlugin plugin, Neo4jSubScan subScan,
                            OperatorContext context) {
    super(context, subScan.getColumns());
    this.plugin  = plugin;
    this.subScan = subScan;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    // Resolve projected vectors — must NOT call addField()
    List<Field> schemaFields = subScan.getFullSchema().getFields();
    projectedFields = new ArrayList<>();
    vectors         = new ArrayList<>();
    for (Field field : schemaFields) {
      ValueVector v = output.getVector(field.getName());
      if (v == null) continue;
      projectedFields.add(field);
      vectors.add(v);
    }

    logger.debug("Neo4jRecordReader setup: label={}, projectedFields={}",
        subScan.getScanSpec().getLabel(), projectedFields.size());
  }

  @Override
  public int next() {
    if (done) return 0;

    Neo4jConnection conn = plugin.getConnection();
    Neo4jScanSpec spec = subScan.getScanSpec();
    String label = spec.getLabel();
    int batchSize = plugin.getConfig().fetchBatchSize;

    // Build column list from projected fields
    List<String> columns = new ArrayList<>(projectedFields.size());
    for (Field f : projectedFields) {
      columns.add(f.getName());
    }

    List<Map<String, Object>> rows = conn.fetchRows(label, columns, skip, batchSize);

    if (rows.isEmpty()) {
      done = true;
      return 0;
    }

    int count = 0;
    for (Map<String, Object> row : rows) {
      writeRow(row, count);
      count++;
    }

    skip += rows.size();

    // If we got fewer than a full batch, there are no more rows
    if (rows.size() < batchSize) {
      done = true;
    }

    for (ValueVector v : vectors) {
      v.setValueCount(count);
    }

    return count;
  }

  private void writeRow(Map<String, Object> row, int idx) {
    for (int i = 0; i < projectedFields.size(); i++) {
      Field field = projectedFields.get(i);
      ValueVector vector = vectors.get(i);
      String fieldName = field.getName();

      Object rawValue = row.get(fieldName);
      if (rawValue == null) continue;

      // rawValue is a Neo4j Value
      if (!(rawValue instanceof Value)) continue;
      Value value = (Value) rawValue;
      if (value.isNull()) continue;

      if (vector instanceof BigIntVector) {
        try {
          ((BigIntVector) vector).setSafe(idx, value.asLong());
        } catch (Exception e) {
          try {
            ((BigIntVector) vector).setSafe(idx, (long) value.asDouble());
          } catch (Exception ignore) {}
        }
      } else if (vector instanceof Float8Vector) {
        try {
          ((Float8Vector) vector).setSafe(idx, value.asDouble());
        } catch (Exception ignore) {}
      } else if (vector instanceof BitVector) {
        try {
          ((BitVector) vector).setSafe(idx, value.asBoolean() ? 1 : 0);
        } catch (Exception ignore) {}
      } else if (vector instanceof VarCharVector) {
        try {
          String str;
          try {
            str = value.asString();
          } catch (Exception ex) {
            str = value.toString();
          }
          ((VarCharVector) vector).setSafe(idx, str.getBytes(StandardCharsets.UTF_8));
        } catch (Exception ignore) {}
      }
    }
  }

  @Override
  public void close() throws Exception {
    // Connection owned by plugin — nothing to close
  }
}
