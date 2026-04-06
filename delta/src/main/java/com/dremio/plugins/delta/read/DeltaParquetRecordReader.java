package com.dremio.plugins.delta.read;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.expression.SchemaPath;
import com.dremio.exec.store.RecordReader;
import com.dremio.sabot.exec.context.OperatorContext;
import com.dremio.sabot.op.scan.OutputMutator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.TimeStampMilliVector;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.TimeUnit;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads a single Delta Lake Parquet file and converts records to Arrow format
 * for Dremio's query engine.
 *
 * Delta tables store all data as self-contained Parquet files — there are no
 * delta logs or merge operations at read time (unlike Hudi MOR). Each file
 * either belongs to the current snapshot (active) or has been superseded by
 * a later write or delete (stale). This reader validates the file against the
 * Delta transaction log before reading, skipping stale files silently.
 *
 * Snapshot validation
 * -------------------
 * Dremio's file scanner lists ALL Parquet files in the table directory,
 * including files that have been REMOVEd in later Delta commits. This reader
 * calls {@code DeltaSnapshotUtils.getLatestFilePaths()} once per setup to
 * obtain the active file set, then skips any file not in that set.
 *
 * Deep partition support
 * ----------------------
 * {@code findDeltaTableRoot()} walks upward from the file's location up to
 * {@link #MAX_PARTITION_DEPTH} levels to find the {@code _delta_log/}
 * directory — covering year/month/day/hour and other deep layouts.
 *
 * This reader is used by DeltaFormatPlugin.getRecordReader() to power
 * SELECT queries on Delta sources where DeltaFormatMatcher auto-discovered
 * the table (eliminating the manual promotion step).
 */
public class DeltaParquetRecordReader implements RecordReader {

  private static final Logger logger = LoggerFactory.getLogger(DeltaParquetRecordReader.class);

  /**
   * Maximum directory levels to walk upward when searching for _delta_log/.
   *
   *   depth 1 — non-partitioned:               table_root/file.parquet
   *   depth 2 — single partition:              table_root/region=us/file.parquet
   *   depth 3 — double partition:              table_root/year=2024/month=01/file.parquet
   *   depth 4 — triple partition (year/mo/day)
   *   depth 5 — quad partition  (year/mo/day/hr)
   *
   * 8 covers all realistic production partition schemes with margin to spare.
   * The loop exits early as soon as _delta_log/ is found.
   */
  private static final int MAX_PARTITION_DEPTH = 8;

  private final String filePath;
  private final List<SchemaPath> columns;
  private final int  batchSize;
  private final long cacheTtlMs;

  private ParquetReader<GenericRecord> reader;
  private Schema avroSchema;

  // field name → Arrow vector (insertion-ordered for consistent column output)
  private final Map<String, ValueVector> vectors = new LinkedHashMap<>();

  // First record read during setup() to infer schema; re-used in first next() call
  private GenericRecord pendingRecord;

  // Set to true when this file is not in the current Delta snapshot; next() returns 0
  private boolean skip = false;

  public DeltaParquetRecordReader(OperatorContext context, String filePath,
      List<SchemaPath> columns, int batchSize, long cacheTtlMs) {
    this.filePath   = filePath;
    this.columns    = columns;
    this.batchSize  = batchSize > 0 ? batchSize : 4096;
    this.cacheTtlMs = cacheTtlMs >= 0 ? cacheTtlMs : 60_000L;
  }

  // -----------------------------------------------------------------------
  // RecordReader lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      Configuration conf = new Configuration();

      // Validate this file against the Delta transaction log.
      // Files that have been REMOVEd in later commits are skipped so queries
      // don't return stale or duplicate rows.
      if (!isFileInCurrentSnapshot(filePath, conf)) {
        skip = true;
        return;
      }

      reader = AvroParquetReader.<GenericRecord>builder(
              new org.apache.hadoop.fs.Path(filePath))
          .withConf(conf)
          .build();

      // Read one record to infer the Avro schema, then hold it for next()
      pendingRecord = reader.read();
      if (pendingRecord == null) {
        logger.debug("Parquet file is empty: {}", filePath);
        return;
      }
      avroSchema = pendingRecord.getSchema();
      logger.debug("Inferred Avro schema from {}: {}", filePath, avroSchema.getName());

      // Create an Arrow vector for each field we can map
      for (Schema.Field avroField : avroSchema.getFields()) {
        Field arrowField = toArrowField(avroField);
        if (arrowField == null) {
          logger.debug("Skipping unmapped field: {} ({})", avroField.name(),
              unwrapUnion(avroField.schema()).getType());
          continue;
        }
        try {
          ValueVector vec = output.addField(arrowField, vectorClass(avroField.schema()));
          vectors.put(avroField.name(), vec);
        } catch (com.dremio.exec.exception.SchemaChangeException e) {
          throw new ExecutionSetupException("Failed to add field: " + avroField.name(), e);
        }
      }

    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot open Parquet file: " + filePath, e);
    }
  }

  @Override
  public int next() {
    if (skip) return 0;
    if (reader == null || vectors.isEmpty()) return 0;

    vectors.values().forEach(ValueVector::reset);

    List<GenericRecord> batch = new ArrayList<>(batchSize);
    try {
      if (pendingRecord != null) {
        batch.add(pendingRecord);
        pendingRecord = null;
      }
      while (batch.size() < batchSize) {
        GenericRecord record = reader.read();
        if (record == null) break;
        batch.add(record);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading Parquet records from " + filePath, e);
    }

    int count = 0;
    for (GenericRecord rec : batch) {
      writeRecord(rec, count++);
    }

    final int finalCount = count;
    vectors.values().forEach(v -> v.setValueCount(finalCount));
    return finalCount;
  }

  @Override
  public void allocate(Map<String, ValueVector> vectorMap) {
    for (ValueVector v : vectorMap.values()) {
      v.allocateNew();
    }
  }

  @Override
  public void close() throws Exception {
    if (reader != null) {
      reader.close();
    }
    vectors.clear();
  }

  // -----------------------------------------------------------------------
  // Delta snapshot validation
  // -----------------------------------------------------------------------

  /**
   * Returns true if {@code filePath} is listed as an active file in the
   * current Delta snapshot (i.e., it has been ADDed and not subsequently
   * REMOVEd in the transaction log).
   *
   * Falls back to {@code true} (allow read) on any error so that snapshot
   * validation failures do not silently drop all data.
   */
  private boolean isFileInCurrentSnapshot(String filePath, Configuration conf) {
    try {
      String tableRoot = findDeltaTableRoot(filePath);
      if (tableRoot == null) return true;

      List<String> activePaths = DeltaSnapshotUtils.getLatestFilePaths(tableRoot, conf, cacheTtlMs);
      if (activePaths.isEmpty()) return true;

      java.nio.file.Path normFile = Paths.get(filePath).normalize();
      boolean active = activePaths.stream()
          .anyMatch(p -> Paths.get(p).normalize().equals(normFile));

      if (!active) {
        logger.debug("Skipping stale Delta file (not in current snapshot): {}", filePath);
      }
      return active;

    } catch (Exception e) {
      logger.warn("Could not verify Delta snapshot for {}; reading file anyway. Error: {}",
          filePath, e.getMessage());
      return true;
    }
  }

  /**
   * Walks upward from {@code filePath}'s parent directory until a directory
   * containing {@code _delta_log/} is found, returning that directory as the
   * Delta table root. Handles any partition depth up to {@link #MAX_PARTITION_DEPTH}.
   *
   * Returns {@code null} if no {@code _delta_log/} is found within the limit,
   * which causes the caller to skip snapshot validation (safe fallback).
   */
  private String findDeltaTableRoot(String filePath) {
    java.nio.file.Path current = Paths.get(filePath).getParent();

    for (int depth = 1; depth <= MAX_PARTITION_DEPTH && current != null; depth++) {
      if (Files.isDirectory(current.resolve("_delta_log"))) {
        if (depth > 2) {
          logger.debug("Found Delta table root at depth {} for file: {}", depth, filePath);
        }
        return current.toString();
      }
      current = current.getParent();
    }

    logger.warn("Could not find _delta_log/ within {} levels of {}; "
        + "snapshot validation skipped for this file.", MAX_PARTITION_DEPTH, filePath);
    return null;
  }

  // -----------------------------------------------------------------------
  // Record writing
  // -----------------------------------------------------------------------

  private void writeRecord(GenericRecord record, int index) {
    for (Schema.Field avroField : avroSchema.getFields()) {
      ValueVector vec = vectors.get(avroField.name());
      if (vec == null) continue;
      Object value = record.get(avroField.name());
      writeValue(vec, index, value, avroField.schema());
    }
  }

  private void writeValue(ValueVector vec, int index, Object value, Schema schema) {
    if (value == null) return;

    Schema actual = unwrapUnion(schema);
    switch (actual.getType()) {
      case STRING:
        if (vec instanceof VarCharVector) {
          ((VarCharVector) vec).setSafe(index,
              value.toString().getBytes(StandardCharsets.UTF_8));
        }
        break;
      case INT:
        if (vec instanceof IntVector) {
          ((IntVector) vec).setSafe(index, ((Number) value).intValue());
        }
        break;
      case LONG:
        if (vec instanceof TimeStampMilliVector) {
          ((TimeStampMilliVector) vec).setSafe(index, ((Number) value).longValue());
        } else if (vec instanceof TimeStampMicroVector) {
          ((TimeStampMicroVector) vec).setSafe(index, ((Number) value).longValue());
        } else if (vec instanceof BigIntVector) {
          ((BigIntVector) vec).setSafe(index, ((Number) value).longValue());
        }
        break;
      case FLOAT:
        if (vec instanceof Float4Vector) {
          ((Float4Vector) vec).setSafe(index, ((Number) value).floatValue());
        }
        break;
      case DOUBLE:
        if (vec instanceof Float8Vector) {
          ((Float8Vector) vec).setSafe(index, ((Number) value).doubleValue());
        }
        break;
      case BOOLEAN:
        if (vec instanceof BitVector) {
          ((BitVector) vec).setSafe(index, Boolean.TRUE.equals(value) ? 1 : 0);
        }
        break;
      case BYTES:
        if (vec instanceof VarBinaryVector) {
          ByteBuffer buf = (ByteBuffer) value;
          byte[] bytes = new byte[buf.remaining()];
          buf.get(bytes);
          ((VarBinaryVector) vec).setSafe(index, bytes);
        }
        break;
      case FIXED:
        if (vec instanceof VarBinaryVector) {
          org.apache.avro.generic.GenericFixed fixed =
              (org.apache.avro.generic.GenericFixed) value;
          ((VarBinaryVector) vec).setSafe(index, fixed.bytes());
        }
        break;
      case ENUM:
        if (vec instanceof VarCharVector) {
          ((VarCharVector) vec).setSafe(index,
              value.toString().getBytes(StandardCharsets.UTF_8));
        }
        break;
      default:
        // Complex types (RECORD, ARRAY, MAP) left as null for now
        break;
    }
  }

  // -----------------------------------------------------------------------
  // Arrow type mapping
  // -----------------------------------------------------------------------

  private Field toArrowField(Schema.Field avroField) {
    Schema actual = unwrapUnion(avroField.schema());
    boolean nullable = avroField.schema().getType() == Schema.Type.UNION;
    ArrowType arrowType = toArrowType(actual);
    if (arrowType == null) return null;
    return new Field(avroField.name(),
        new FieldType(nullable, arrowType, null, null),
        Collections.emptyList());
  }

  private ArrowType toArrowType(Schema schema) {
    switch (schema.getType()) {
      case STRING:
      case ENUM:    return new ArrowType.Utf8();
      case INT:     return new ArrowType.Int(32, true);
      case LONG:
        if (schema.getLogicalType() != null) {
          String lt = schema.getLogicalType().getName();
          if ("timestamp-millis".equals(lt))
            return new ArrowType.Timestamp(TimeUnit.MILLISECOND, null);
          if ("timestamp-micros".equals(lt))
            return new ArrowType.Timestamp(TimeUnit.MICROSECOND, null);
        }
        return new ArrowType.Int(64, true);
      case FLOAT:   return new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE);
      case DOUBLE:  return new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE);
      case BOOLEAN: return new ArrowType.Bool();
      case BYTES:
      case FIXED:   return new ArrowType.Binary();
      default:      return null;
    }
  }

  @SuppressWarnings("unchecked")
  private Class<? extends ValueVector> vectorClass(Schema schema) {
    Schema actual = unwrapUnion(schema);
    switch (actual.getType()) {
      case STRING:
      case ENUM:    return VarCharVector.class;
      case INT:     return IntVector.class;
      case LONG:
        if (actual.getLogicalType() != null) {
          String lt = actual.getLogicalType().getName();
          if ("timestamp-millis".equals(lt)) return TimeStampMilliVector.class;
          if ("timestamp-micros".equals(lt)) return TimeStampMicroVector.class;
        }
        return BigIntVector.class;
      case FLOAT:   return Float4Vector.class;
      case DOUBLE:  return Float8Vector.class;
      case BOOLEAN: return BitVector.class;
      case BYTES:
      case FIXED:   return VarBinaryVector.class;
      default:      return VarCharVector.class;
    }
  }

  private Schema unwrapUnion(Schema schema) {
    if (schema.getType() != Schema.Type.UNION) return schema;
    for (Schema s : schema.getTypes()) {
      if (s.getType() != Schema.Type.NULL) return s;
    }
    return schema;
  }
}
