package com.dremio.plugins.hudi.read;

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
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
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
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Reads a single Hudi base file (Parquet) and converts records to Arrow format
 * for Dremio's query engine.
 *
 * COW tables: reads the base Parquet file directly after validating it against
 * the Hudi timeline (stale pre-compaction files are skipped).
 *
 * MOR tables: additionally reads log files for the file slice via
 * HudiMORRecordReader, builds a merge map, and applies it during scanning:
 *   - Updated records: base record replaced by the log version
 *   - Deleted records: base record dropped (tombstone in log)
 *   - Insert-only records: emitted after the base file is exhausted
 *
 * This is used by HudiFormatPlugin.getRecordReader() so that SELECT queries
 * on Hudi sources work through Dremio's standard scan pipeline.
 */
public class HudiParquetRecordReader implements RecordReader {

  private static final Logger logger = LoggerFactory.getLogger(HudiParquetRecordReader.class);

  /**
   * Maximum number of directory levels to walk upward from a Parquet file's
   * location when searching for the Hudi table root (.hoodie/ directory).
   *
   * Depth accounting (file path is NOT counted, only parent directories are):
   *   depth 1 — non-partitioned:               table_root/file.parquet
   *   depth 2 — single partition:              table_root/region=us/file.parquet
   *   depth 3 — double partition:              table_root/year=2024/month=01/file.parquet
   *   depth 4 — triple partition:              table_root/year=2024/month=01/day=15/file.parquet
   *   depth 5 — quad partition (year/mo/day/hr)
   *
   * 8 covers all realistic production partition schemes with margin to spare.
   * The loop exits early as soon as .hoodie/ is found, so shallow tables pay
   * no extra cost.
   */
  private static final int MAX_PARTITION_DEPTH = 8;

  private final String filePath;
  private final List<SchemaPath> columns;
  // Record key field name used to look up records in the MOR merge map.
  // Comes from HudiPluginConfig.defaultRecordKeyField (e.g. "user_id").
  private final String recordKeyField;
  private final int batchSize;

  private ParquetReader<GenericRecord> reader;
  private Schema avroSchema;

  // field name → Arrow vector (insertion-ordered for consistent column output)
  private final Map<String, ValueVector> vectors = new LinkedHashMap<>();

  // First record read during setup() to infer schema; re-used in first next() call
  private GenericRecord pendingRecord;

  // Set to true when the file is not in the latest Hudi snapshot; next() returns 0
  private boolean skip = false;

  // -----------------------------------------------------------------------
  // MOR merge state
  // -----------------------------------------------------------------------

  // Non-null only for MOR slices that have log files
  private Map<String, Option<GenericRecord>> mergeMap = null;
  // Tracks which base-file record keys were seen during merge (to find insert-only records)
  private Set<String> mergedBaseKeys = null;
  // Flips to true when reader.read() returns null (base file exhausted)
  private boolean baseFileExhausted = false;
  // Records from the log that have no matching base file entry (pure inserts)
  private List<GenericRecord> insertOnlyPending = null;
  private int insertOnlyOffset = 0;

  // -----------------------------------------------------------------------
  // Log-only file group state (MOR tables only)
  // -----------------------------------------------------------------------

  // True if this reader is designated to emit records from log-only file groups.
  // Designation: the reader whose base file path is lexicographically smallest
  // among all readers in this scan is the sole emitter, avoiding duplicates.
  private boolean isDesignatedLogOnlyEmitter = false;
  // Records collected from all log-only slices (no base Parquet file at all)
  private List<GenericRecord> logOnlyRecords = null;
  private int logOnlyOffset = 0;

  public HudiParquetRecordReader(OperatorContext context, String filePath,
      List<SchemaPath> columns, String recordKeyField, int batchSize) {
    this.filePath       = filePath;
    this.columns        = columns;
    this.recordKeyField = recordKeyField;
    this.batchSize      = batchSize > 0 ? batchSize : 4096;
  }

  // -----------------------------------------------------------------------
  // RecordReader lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void setup(OutputMutator output) throws ExecutionSetupException {
    try {
      Configuration conf = new Configuration();

      // Validate this file against the Hudi timeline before reading.
      // Stale pre-compaction files are skipped so queries don't return
      // duplicate or overwritten data.
      if (!isFileInLatestSnapshot(filePath, conf)) {
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

      // For MOR tables: build a merge map from the log files associated with
      // this base file's slice. Applied during next() to surface updates/deletes.
      buildMergeMapIfMOR(filePath, conf);

    } catch (IOException e) {
      throw new ExecutionSetupException("Cannot open Parquet file: " + filePath, e);
    }
  }

  @Override
  public int next() {
    if (skip) return 0;

    // Phase 2 + 3: base file exhausted
    if (baseFileExhausted) {
      // Phase 2: insert-only records from this slice's log files
      int n = drainInsertOnly();
      if (n > 0) return n;
      // Phase 3: records from log-only file groups (slices with no base Parquet)
      return drainLogOnlyRecords();
    }

    if (reader == null || vectors.isEmpty()) return 0;

    vectors.values().forEach(ValueVector::reset);

    // Collect raw Avro records from the base Parquet file
    List<GenericRecord> batch = new ArrayList<>(batchSize);
    try {
      if (pendingRecord != null) {
        batch.add(pendingRecord);
        pendingRecord = null;
      }
      while (batch.size() < batchSize) {
        GenericRecord record = reader.read();
        if (record == null) {
          baseFileExhausted = true;
          break;
        }
        batch.add(record);
      }
    } catch (IOException e) {
      throw new RuntimeException("Error reading Parquet records from " + filePath, e);
    }

    // Apply MOR merge: updates replace base records, deletes are dropped
    List<GenericRecord> outputBatch = (mergeMap != null) ? applyMerge(batch) : batch;

    // Once the base file is done, collect insert-only records from the log
    if (baseFileExhausted && mergeMap != null) {
      buildInsertOnlyPending();
    }

    int count = 0;
    for (GenericRecord rec : outputBatch) {
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
  // MOR merge logic
  // -----------------------------------------------------------------------

  /**
   * For MERGE_ON_READ tables, builds the merge map from this slice's log files
   * and detects log-only file groups (slices with no base Parquet file at all).
   *
   * Two cases handled:
   *   1. This reader's slice has log files → build merge map for updates/deletes/inserts
   *   2. Some slices have NO base file → one designated reader loads all those records
   *
   * No-ops for COW tables.
   */
  private void buildMergeMapIfMOR(String filePath, Configuration conf) {
    try {
      String tableRoot = findHudiTableRoot(filePath);
      if (tableRoot == null) return;

      if (HudiSnapshotUtils.getTableType(tableRoot, conf) != HoodieTableType.MERGE_ON_READ) {
        return; // COW table: no log merging needed
      }

      List<FileSlice> slices = HudiSnapshotUtils.getLatestFileSlices(tableRoot, conf);
      java.nio.file.Path normFile = Paths.get(filePath).normalize();

      // Find the file slice whose base file matches the current split
      FileSlice mySlice = slices.stream()
          .filter(s -> s.getBaseFile().isPresent())
          .filter(s -> Paths.get(s.getBaseFile().get().getPath()).normalize().equals(normFile))
          .findFirst().orElse(null);

      // Slices that have log files but no base Parquet file at all
      List<FileSlice> logOnlySlices = slices.stream()
          .filter(s -> !s.getBaseFile().isPresent())
          .filter(s -> s.getLogFiles().findAny().isPresent())
          .collect(Collectors.toList());

      boolean hasMySliceLogs = mySlice != null && mySlice.getLogFiles().findAny().isPresent();
      boolean hasLogOnlySlices = !logOnlySlices.isEmpty();

      if (!hasMySliceLogs && !hasLogOnlySlices) {
        return; // nothing MOR-specific to do
      }

      HoodieTableMetaClient metaClient = HudiSnapshotUtils.buildMetaClient(tableRoot, conf);

      // Case 1: this slice has log files — build the merge map
      if (hasMySliceLogs) {
        try (HudiMORRecordReader morReader = new HudiMORRecordReader(
            mySlice, avroSchema, metaClient, conf)) {
          mergeMap = morReader.buildMergeMap();
          mergedBaseKeys = new HashSet<>();
          logger.info("MOR merge map built: {} log entries for {}", mergeMap.size(), filePath);
        }
      }

      // Case 2: log-only slices exist — designate exactly one reader to emit them.
      // We pick the reader whose base file path is lexicographically smallest so
      // every executor thread arrives at the same decision without shared state.
      if (hasLogOnlySlices) {
        java.util.Optional<String> minBasePath = slices.stream()
            .filter(s -> s.getBaseFile().isPresent())
            .map(s -> s.getBaseFile().get().getPath())
            .min(Comparator.naturalOrder());
        if (minBasePath.isPresent()
            && Paths.get(filePath).normalize()
               .equals(Paths.get(minBasePath.get()).normalize())) {
          isDesignatedLogOnlyEmitter = true;
          loadLogOnlyRecords(logOnlySlices, metaClient, conf);
        }
      }

    } catch (Exception e) {
      logger.warn("Could not build MOR merge map for {}; reading base file only. Error: {}",
          filePath, e.getMessage());
      mergeMap = null;
    }
  }

  /**
   * Reads all records from log-only file slices (groups with no base Parquet file)
   * and accumulates them into {@code logOnlyRecords} for batch emission.
   * Only called on the designated emitter reader.
   */
  private void loadLogOnlyRecords(List<FileSlice> logOnlySlices,
      HoodieTableMetaClient metaClient, Configuration conf) {
    logOnlyRecords = new ArrayList<>();
    for (FileSlice slice : logOnlySlices) {
      try (HudiMORRecordReader morReader = new HudiMORRecordReader(
          slice, avroSchema, metaClient, conf)) {
        Map<String, Option<GenericRecord>> logMap = morReader.buildMergeMap();
        for (Option<GenericRecord> opt : logMap.values()) {
          if (opt.isPresent()) {
            logOnlyRecords.add(opt.get());
          }
        }
      } catch (Exception e) {
        logger.warn("Could not read log-only slice; skipping. Error: {}", e.getMessage());
      }
    }
    logOnlyOffset = 0;
    logger.info("MOR log-only: {} records loaded from {} log-only slice(s) via {}",
        logOnlyRecords.size(), logOnlySlices.size(), filePath);
  }

  /**
   * Drains the log-only records list in batchSize chunks.
   * Returns 0 when all records have been emitted or this reader is not the
   * designated log-only emitter.
   */
  private int drainLogOnlyRecords() {
    if (!isDesignatedLogOnlyEmitter
        || logOnlyRecords == null
        || logOnlyOffset >= logOnlyRecords.size()) {
      return 0;
    }

    vectors.values().forEach(ValueVector::reset);

    int count = 0;
    while (count < batchSize && logOnlyOffset < logOnlyRecords.size()) {
      writeRecord(logOnlyRecords.get(logOnlyOffset++), count++);
    }

    final int finalCount = count;
    vectors.values().forEach(v -> v.setValueCount(finalCount));
    return finalCount;
  }

  /**
   * Applies the merge map to a batch of base file records.
   *   key in mergeMap, value present  → replace with log version (update)
   *   key in mergeMap, value empty    → drop record (delete / tombstone)
   *   key not in mergeMap             → emit unchanged
   */
  private List<GenericRecord> applyMerge(List<GenericRecord> records) {
    List<GenericRecord> result = new ArrayList<>(records.size());
    for (GenericRecord rec : records) {
      Object keyVal = (recordKeyField != null) ? rec.get(recordKeyField) : null;
      String key = (keyVal != null) ? keyVal.toString() : null;

      if (key != null && mergeMap.containsKey(key)) {
        mergedBaseKeys.add(key);
        Option<GenericRecord> logRec = mergeMap.get(key);
        if (logRec.isPresent()) {
          result.add(logRec.get()); // update: use log version
        }
        // else: delete — skip this record
      } else {
        result.add(rec); // unchanged
      }
    }
    return result;
  }

  /**
   * After the base file is fully read, finds records that exist only in the
   * log (pure inserts with no matching base file entry) and queues them for
   * emission in drainInsertOnly().
   */
  private void buildInsertOnlyPending() {
    insertOnlyPending = new ArrayList<>();
    for (Map.Entry<String, Option<GenericRecord>> entry : mergeMap.entrySet()) {
      if (!mergedBaseKeys.contains(entry.getKey()) && entry.getValue().isPresent()) {
        insertOnlyPending.add(entry.getValue().get());
      }
    }
    insertOnlyOffset = 0;
    if (!insertOnlyPending.isEmpty()) {
      logger.debug("MOR: {} insert-only records to emit for {}", insertOnlyPending.size(), filePath);
    }
  }

  /**
   * Drains the insert-only pending list in batchSize chunks.
   * Returns 0 when all insert-only records have been emitted.
   */
  private int drainInsertOnly() {
    if (insertOnlyPending == null || insertOnlyOffset >= insertOnlyPending.size()) {
      return 0;
    }

    vectors.values().forEach(ValueVector::reset);

    int count = 0;
    while (count < batchSize && insertOnlyOffset < insertOnlyPending.size()) {
      writeRecord(insertOnlyPending.get(insertOnlyOffset++), count++);
    }

    final int finalCount = count;
    vectors.values().forEach(v -> v.setValueCount(finalCount));
    return finalCount;
  }

  // -----------------------------------------------------------------------
  // Hudi snapshot validation
  // -----------------------------------------------------------------------

  private boolean isFileInLatestSnapshot(String filePath, Configuration conf) {
    try {
      String tableRoot = findHudiTableRoot(filePath);
      if (tableRoot == null) return true;

      List<String> latestPaths = HudiSnapshotUtils.getLatestBaseFilePaths(tableRoot, conf);
      if (latestPaths.isEmpty()) return true;

      java.nio.file.Path normFile = Paths.get(filePath).normalize();
      boolean inSnapshot = latestPaths.stream()
          .anyMatch(p -> Paths.get(p).normalize().equals(normFile));

      if (!inSnapshot) {
        logger.debug("Skipping stale Hudi file (not in latest snapshot): {}", filePath);
      }
      return inSnapshot;

    } catch (Exception e) {
      logger.warn("Could not verify Hudi snapshot for {}; reading file anyway. Error: {}",
          filePath, e.getMessage());
      return true;
    }
  }

  /**
   * Walks upward from {@code filePath}'s parent directory until a directory
   * containing {@code .hoodie/} is found, returning that directory as the
   * Hudi table root.
   *
   * Handles any partition depth up to {@link #MAX_PARTITION_DEPTH}:
   *   non-partitioned         → found at depth 1 (file's immediate parent)
   *   region=us/              → found at depth 2
   *   year=2024/month=01/     → found at depth 3
   *   year=2024/month=01/day/ → found at depth 4
   *   … and so on
   *
   * Returns {@code null} if no {@code .hoodie/} directory is found within
   * {@code MAX_PARTITION_DEPTH} levels, which causes the caller to skip
   * snapshot validation and MOR merge-map building (safe fallback — the
   * file is still read directly).
   */
  private String findHudiTableRoot(String filePath) {
    java.nio.file.Path current = Paths.get(filePath).getParent();

    for (int depth = 1; depth <= MAX_PARTITION_DEPTH && current != null; depth++) {
      if (Files.isDirectory(current.resolve(".hoodie"))) {
        if (depth > 2) {
          // Log once so deep-partition tables are visible in diagnostics
          logger.debug("Found Hudi table root at depth {} for file: {}", depth, filePath);
        }
        return current.toString();
      }
      current = current.getParent();
    }

    logger.warn("Could not find .hoodie/ within {} levels of {}; "
        + "snapshot validation and MOR merge skipped for this file.",
        MAX_PARTITION_DEPTH, filePath);
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
        // Complex types (RECORD, ARRAY, MAP, UNION) left as null for now
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
