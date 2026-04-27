package com.dremio.plugins.spanner.scan;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.exec.store.AbstractRecordReader;
import com.dremio.plugins.spanner.SpannerConnection;
import com.dremio.plugins.spanner.SpannerStoragePlugin;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import com.google.cloud.Date;
import com.google.cloud.spanner.ResultSet;
import com.google.cloud.spanner.Statement;
import com.google.cloud.spanner.Type;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.DateMilliVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.TimeStampMilliTZVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

/**
 * Reads rows from a Spanner table into Dremio Arrow vectors.
 *
 * Data flow:
 *   setup() → resolve projected vectors, enqueue all scan specs
 *   next()  → advance ResultSet, write rows into vectors up to TARGET_BATCH_SIZE
 *   close() → close open ResultSet
 *
 * Multiple ScanSpecs are drained sequentially (normal on single-node Dremio where
 * all splits land in one executor fragment). Each spec executes its own SQL query
 * against the assigned partition (MOD-based segmentation via WHERE ABS(FARM_FINGERPRINT...)).
 */
public class SpannerRecordReader extends AbstractRecordReader {

  private static final Logger logger = LoggerFactory.getLogger(SpannerRecordReader.class);
  private static final int TARGET_BATCH_SIZE = 4_000;

  private final SpannerStoragePlugin plugin;
  private final SpannerSubScan       subScan;

  private List<Field>       projectedFields;
  private List<ValueVector> vectors;

  private final Queue<SpannerScanSpec> specQueue = new ArrayDeque<>();
  private ResultSet currentRs;
  private boolean   done = false;

  public SpannerRecordReader(SpannerStoragePlugin plugin,
                              SpannerSubScan subScan,
                              OperatorContext context) {
    super(context, subScan.getColumns());
    this.plugin  = plugin;
    this.subScan = subScan;
  }

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    // Resolve projected vectors — do NOT call addField(), vectors are pre-allocated
    List<Field> schemaFields = subScan.getFullSchema().getFields();
    projectedFields = new ArrayList<>();
    vectors         = new ArrayList<>();
    for (Field field : schemaFields) {
      ValueVector v = output.getVector(field.getName());
      if (v == null) continue;
      projectedFields.add(field);
      vectors.add(v);
    }

    List<SpannerScanSpec> specs = subScan.getScanSpecs();
    if (specs == null || specs.isEmpty()) {
      done = true;
      return;
    }

    specQueue.addAll(specs.subList(1, specs.size()));
    startSpec(specs.get(0));
  }

  private void startSpec(SpannerScanSpec spec) throws ExecutionSetupException {
    if (currentRs != null) {
      try { currentRs.close(); } catch (Exception ignore) {}
      currentRs = null;
    }

    SpannerConnection conn = plugin.getConnection();
    String sql = buildSql(spec);
    logger.debug("Spanner query: {}", sql);
    try {
      currentRs = conn.executeQuery(Statement.of(sql));
    } catch (Exception e) {
      throw new ExecutionSetupException("Failed to execute Spanner query: " + sql, e);
    }
  }

  /**
   * Builds a SQL query for the given spec with MOD-based segmentation.
   * When totalSegments == 1 the WHERE clause is omitted for efficiency.
   */
  private String buildSql(SpannerScanSpec spec) {
    StringBuilder sb = new StringBuilder(spec.toSql());

    if (spec.getTotalSegments() > 1) {
      boolean hasWhere = spec.hasFilters();
      String segmentClause = String.format(
          "MOD(ABS(FARM_FINGERPRINT(TO_JSON_STRING(`%s`))), %d) = %d",
          spec.getTableName(), spec.getTotalSegments(), spec.getSegment());
      if (hasWhere) {
        sb.append(" AND ").append(segmentClause);
      } else {
        sb.append(" WHERE ").append(segmentClause);
      }
    }

    return sb.toString();
  }

  @Override
  public int next() {
    int count = 0;
    SpannerScanSpec firstSpec = subScan.getScanSpec();
    long limitRemaining = (firstSpec != null && firstSpec.hasLimit())
        ? firstSpec.getLimit() : Long.MAX_VALUE;

    while (count < TARGET_BATCH_SIZE && count < limitRemaining && !done) {
      if (currentRs == null || !currentRs.next()) {
        // Advance to next spec
        if (specQueue.isEmpty()) {
          done = true;
          break;
        }
        try {
          startSpec(specQueue.poll());
        } catch (ExecutionSetupException e) {
          throw new RuntimeException("Failed to start next Spanner scan segment", e);
        }
        continue;
      }

      writeRow(count);
      count++;
    }

    for (ValueVector v : vectors) {
      v.setValueCount(count);
    }
    return count;
  }

  private void writeRow(int idx) {
    for (int i = 0; i < projectedFields.size(); i++) {
      Field field = projectedFields.get(i);
      ValueVector vector = vectors.get(i);
      String colName = field.getName();

      try {
        if (currentRs.isNull(colName)) continue; // leave slot null

        if (vector instanceof BitVector) {
          ((BitVector) vector).setSafe(idx, currentRs.getBoolean(colName) ? 1 : 0);

        } else if (vector instanceof BigIntVector) {
          ((BigIntVector) vector).setSafe(idx, currentRs.getLong(colName));

        } else if (vector instanceof Float4Vector) {
          ((Float4Vector) vector).setSafe(idx, currentRs.getFloat(colName));

        } else if (vector instanceof Float8Vector) {
          ((Float8Vector) vector).setSafe(idx, currentRs.getDouble(colName));

        } else if (vector instanceof DateMilliVector) {
          Date d = currentRs.getDate(colName);
          long epochMillis = java.time.LocalDate.of(d.getYear(), d.getMonth(), d.getDayOfMonth())
              .atStartOfDay(java.time.ZoneOffset.UTC).toInstant().toEpochMilli();
          ((DateMilliVector) vector).setSafe(idx, epochMillis);

        } else if (vector instanceof TimeStampMilliTZVector) {
          com.google.cloud.Timestamp ts = currentRs.getTimestamp(colName);
          long millis = TimeUnit.SECONDS.toMillis(ts.getSeconds())
              + TimeUnit.NANOSECONDS.toMillis(ts.getNanos());
          ((TimeStampMilliTZVector) vector).setSafe(idx, millis);

        } else if (vector instanceof VarBinaryVector) {
          com.google.cloud.ByteArray ba = currentRs.getBytes(colName);
          ((VarBinaryVector) vector).setSafe(idx, ba.toByteArray());

        } else if (vector instanceof VarCharVector) {
          // STRING, NUMERIC, JSON, ARRAY → all stored as UTF-8 text
          String val = getAsString(colName);
          if (val != null) {
            ((VarCharVector) vector).setSafe(idx, val.getBytes(StandardCharsets.UTF_8));
          }
        }
      } catch (Exception e) {
        logger.warn("Error reading column '{}' at row {}: {}", colName, idx, e.getMessage());
      }
    }
  }

  private String getAsString(String colName) {
    try {
      Type colType = currentRs.getColumnType(colName);
      Type.Code code = colType.getCode();
      switch (code) {
        case STRING:  return currentRs.getString(colName);
        case NUMERIC: return currentRs.getBigDecimal(colName).toPlainString();
        case JSON:    return currentRs.getJson(colName);
        case ARRAY:   return currentRs.getValue(colName).toString();
        default:      return currentRs.getString(colName);
      }
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void close() throws Exception {
    if (currentRs != null) {
      try { currentRs.close(); } catch (Exception ignore) {}
      currentRs = null;
    }
  }
}
