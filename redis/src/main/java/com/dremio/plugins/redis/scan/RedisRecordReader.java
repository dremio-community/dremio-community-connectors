package com.dremio.plugins.redis.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.redis.RedisConnection;
import com.dremio.plugins.redis.RedisPlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Reads Redis hash data and writes into Dremio Arrow vectors.
 *
 * Flow:
 *   setup()  → resolve projected vectors from output mutator, fetch all matching keys
 *   next()   → iterate keys, HGETALL each, write batch into vectors
 *   close()  → no-op (connection owned by plugin)
 *
 * Client-side filtering is not implemented here — Dremio will evaluate any
 * filter predicates on the returned rows in a later operator stage.
 */
public class RedisRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(RedisRecordReader.class);
  private static final int TARGET_BATCH_SIZE = 4_000;

  private final RedisPlugin plugin;
  private final RedisSubScan subScan;

  private List<Field>       projectedFields;
  private List<ValueVector> vectors;

  private List<String> keys;
  private int keyIndex  = 0;
  private boolean done  = false;

  // delimiter length prefix for extracting the _id suffix
  private int prefixLen;

  public RedisRecordReader(RedisPlugin plugin, RedisSubScan subScan, OperatorContext context) {
    super(context, subScan.getColumns());
    this.plugin  = plugin;
    this.subScan = subScan;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    RedisScanSpec spec = subScan.getScanSpec();
    String tableName = spec.getTableName();
    String delimiter = plugin.getConfig().keyDelimiter;
    prefixLen = tableName.length() + delimiter.length();

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

    // Fetch all matching keys up front
    RedisConnection conn = plugin.getConnection();
    keys = conn.scanKeys(tableName);

    if (spec.hasLimit() && keys.size() > spec.getLimit()) {
      keys = keys.subList(0, (int) spec.getLimit());
    }

    logger.debug("RedisRecordReader setup: table={}, keys={}", tableName, keys.size());
  }

  @Override
  public int next() {
    if (done || keys == null || keyIndex >= keys.size()) {
      return 0;
    }

    RedisConnection conn = plugin.getConnection();
    int count = 0;

    while (count < TARGET_BATCH_SIZE && keyIndex < keys.size()) {
      String key = keys.get(keyIndex++);
      Map<String, String> hash = conn.hgetAll(key);
      if (hash == null) continue;

      // Build the _id value from key suffix
      String id = key.length() > prefixLen ? key.substring(prefixLen) : key;

      writeRow(hash, id, count);
      count++;
    }

    if (keyIndex >= keys.size()) done = true;

    for (ValueVector v : vectors) {
      v.setValueCount(count);
    }
    return count;
  }

  private void writeRow(Map<String, String> hash, String id, int idx) {
    for (int i = 0; i < projectedFields.size(); i++) {
      Field field = projectedFields.get(i);
      ValueVector vector = vectors.get(i);
      String fieldName = field.getName();

      String value = "_id".equals(fieldName) ? id : hash.get(fieldName);
      if (value == null) continue;

      ArrowType type = field.getType();
      if (vector instanceof VarCharVector) {
        ((VarCharVector) vector).setSafe(idx, value.getBytes(StandardCharsets.UTF_8));
      } else if (vector instanceof BigIntVector) {
        try {
          ((BigIntVector) vector).setSafe(idx, Long.parseLong(value));
        } catch (NumberFormatException e) {
          try {
            ((BigIntVector) vector).setSafe(idx, (long) Double.parseDouble(value));
          } catch (NumberFormatException ignore) {}
        }
      } else if (vector instanceof Float8Vector) {
        try {
          ((Float8Vector) vector).setSafe(idx, Double.parseDouble(value));
        } catch (NumberFormatException ignore) {}
      } else if (vector instanceof BitVector) {
        String lower = value.toLowerCase();
        if (lower.equals("true") || lower.equals("1")) {
          ((BitVector) vector).setSafe(idx, 1);
        } else if (lower.equals("false") || lower.equals("0")) {
          ((BitVector) vector).setSafe(idx, 0);
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    // Connection owned by plugin — nothing to close
  }
}
