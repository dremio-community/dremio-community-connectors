package com.dremio.plugins.delta.write;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.plugins.delta.conf.DeltaPluginConfig;
import com.dremio.plugins.delta.read.DeltaSnapshotUtils;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.Snapshot;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Format;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.types.StructType;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Core write component: translates Dremio's Arrow-based write pipeline into
 * Delta Lake Parquet files + transaction log commits.
 *
 * Write Path
 * ----------
 * Delta Lake stores data as standard Parquet files. Unlike Hudi, there is no
 * Java "write client" that handles both Parquet writing and log management.
 * We handle both steps ourselves:
 *
 *   1. Parquet writing: Arrow -> Avro (via ArrowToAvroConverter) ->
 *                       Parquet file (via AvroParquetWriter from parquet-avro)
 *
 *   2. Delta log commit: DeltaLog.startTransaction() ->
 *                        optionally update Metadata (new table / schema evolution) ->
 *                        commit AddFile actions for all written Parquet files
 *
 * One writer per executor thread. Each thread may write one Parquet file per
 * partition. File paths follow Hive-style layout:
 *
 *   Non-partitioned:  {tablePath}/part-{uuid}.snappy.parquet
 *   Single partition: {tablePath}/region=us-east/part-{uuid}.snappy.parquet
 *   Multi-partition:  {tablePath}/year=2025/month=01/part-{uuid}.snappy.parquet
 *
 * Partition Lifecycle
 * -------------------
 * Dremio calls startPartition() before routing a new partition's rows to this
 * writer. For each partition:
 *   1. startPartition() sets the partition state (dir path, values map) and
 *      flushes any previously open writer to pendingAddFiles.
 *   2. writeBatch() lazily opens the Parquet writer on the first call.
 *   3. After all batches, startPartition() is called again (next partition)
 *      or close() is called (last partition).
 *   4. close() commits all accumulated AddFile actions as a single Delta
 *      transaction, regardless of how many partitions were written.
 *
 * Threading Model
 * ---------------
 * Dremio creates one DeltaRecordWriter per executor thread. All threads write
 * their own Parquet files independently and commit their own Delta transactions.
 *
 * Operation Modes
 * ---------------
 *   INSERT INTO / CTAS   -> new Parquet file + AddFile committed to Delta log
 *   MERGE INTO / UPDATE  -> detected via _delta_upsert schema marker
 *                           (TODO: requires reading + rewriting affected files;
 *                            current implementation falls back to INSERT)
 *   DELETE FROM          -> detected via _delta_is_deleted schema marker
 *                           (TODO: requires reading + filtering + rewriting files;
 *                            rows with _delta_is_deleted=true are excluded from output)
 *
 * Delta Table Creation
 * --------------------
 * When writing to a path that has no existing Delta log, the writer initializes
 * the Delta table by committing a Metadata action (schema + format) before the
 * first AddFile. This is the CTAS path.
 *
 * Schema Evolution
 * ----------------
 * If allowSchemaEvolution=true (DeltaPluginConfig), the writer checks whether
 * the incoming Arrow schema has columns not present in the Delta schema. If so,
 * it updates the Metadata action with the new merged schema before committing.
 */
public class DeltaRecordWriter implements RecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(DeltaRecordWriter.class);

  /** Schema marker: rows with _delta_is_deleted=true are excluded from output. */
  private static final String IS_DELETED_FIELD = "_delta_is_deleted";

  /** Schema marker: presence triggers upsert routing (future implementation). */
  private static final String UPSERT_MARKER_FIELD = "_delta_upsert";

  private final String tablePath;
  private final WriterOptions writerOptions;
  private final DeltaPluginConfig pluginConfig;
  private final PluginSabotContext context;

  // Schema derived from incoming Arrow data in setup() — used in writeBatch() and close()
  private List<org.apache.arrow.vector.types.pojo.Field> incomingFields;

  // Retained from setup() for use in writeBatch()
  private VectorAccessible incoming;

  // Value vectors extracted from incoming in setup() — one per column, in field order
  private List<org.apache.arrow.vector.ValueVector> vectorList;

  // Avro schema derived from the incoming Arrow schema
  private Schema avroSchema;

  // Resolved once in setup() — used in openParquetWriter() for each partition
  private CompressionCodecName codec;
  private long targetFileSize;

  // Hadoop configuration used for filesystem access
  private Configuration hadoopConf;

  // Parquet writer — one per partition; null between partitions
  private ParquetWriter<GenericRecord> parquetWriter;

  // The Hadoop path of the currently open Parquet file (null when writer is closed)
  private Path parquetOutputPath;

  // -----------------------------------------------------------------------
  // Partition state — updated by startPartition()
  // -----------------------------------------------------------------------

  /**
   * The WritePartition passed by Dremio's planner for the current partition.
   * Stored so openParquetWriter() can call partition.getQualifiedPath() to
   * let Dremio construct the correct partition directory path.
   */
  private WritePartition currentPartition = null;

  /**
   * Hive-style relative partition directory for the current partition, or ""
   * for non-partitioned tables.
   *
   * Examples:
   *   ""                           — non-partitioned
   *   "region=us-east"             — single partition column
   *   "year=2025/month=01"         — two partition columns
   */
  private String currentPartitionDir = "";

  /**
   * Map of partition column name -> partition value string for the current
   * partition. Written into the AddFile action so Delta readers can do
   * partition pruning without scanning file content.
   *
   * Empty map for non-partitioned tables.
   */
  private Map<String, String> currentPartitionValues = new HashMap<>();

  /**
   * Accumulates one AddFile action per flushed file (one per partition per file
   * split). Committed atomically to the Delta log in close(). A single Delta
   * transaction for all files written by this thread is both more efficient
   * (one log entry) and safer (atomically visible to readers).
   */
  private final List<AddFile> pendingAddFiles = new ArrayList<>();

  /**
   * Rows written to the CURRENT Parquet file. Reset to 0 each time a new file
   * is opened (partition change or file-size roll). Used for per-file stats.
   */
  private long rowsInCurrentFile = 0;

  /**
   * Total rows written across all files by this writer. Used for the final
   * stats report to Dremio's WriterCommitterOperator.
   */
  private long totalRowsWritten = 0;

  // Operation mode flags (set in setup())
  private boolean isDeleteMode  = false;
  private boolean isUpsertMode  = false;

  // Delete mode: index of the _delta_is_deleted column in the incoming schema (-1 if absent)
  private int deletedFieldIndex = -1;

  // Whether setup() has been called
  private boolean initialized = false;

  // Whether close() has already committed to the Delta log (guards against double-close)
  private boolean committed = false;

  // Output stats callbacks
  private OutputEntryListener outputEntryListener;
  private WriteStatsListener  writeStatsListener;

  public DeltaRecordWriter(
      String tablePath,
      WriterOptions writerOptions,
      DeltaPluginConfig pluginConfig,
      PluginSabotContext context) {
    this.tablePath    = tablePath;
    this.writerOptions = writerOptions;
    this.pluginConfig  = pluginConfig;
    this.context       = context;
  }

  // -----------------------------------------------------------------------
  // RecordWriter lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener)
      throws IOException {
    this.outputEntryListener = listener;
    this.writeStatsListener  = statsListener;
    this.incoming            = incoming;

    // Cache value vectors in field order for use in writeBatch()
    this.vectorList = new ArrayList<>();
    for (VectorWrapper<?> vw : incoming) {
      this.vectorList.add(vw.getValueVector());
    }

    BatchSchema schema = incoming.getSchema();

    // Detect operation mode from schema markers
    this.isDeleteMode = schema.getFields().stream()
        .anyMatch(f -> IS_DELETED_FIELD.equals(f.getName()));
    this.isUpsertMode = !isDeleteMode && schema.getFields().stream()
        .anyMatch(f -> UPSERT_MARKER_FIELD.equals(f.getName()));

    if (isDeleteMode) {
      // Find the column index of the delete marker for fast lookup in writeBatch()
      List<org.apache.arrow.vector.types.pojo.Field> fields = schema.getFields();
      for (int i = 0; i < fields.size(); i++) {
        if (IS_DELETED_FIELD.equals(fields.get(i).getName())) {
          this.deletedFieldIndex = i;
          break;
        }
      }
    }

    logger.info("DeltaRecordWriter mode: isUpsertMode={}, isDeleteMode={}", isUpsertMode, isDeleteMode);

    // Cache schema fields for use in writeBatch() and close()
    this.incomingFields = schema.getFields();

    // Derive Avro schema — ArrowToAvroConverter.toAvroSchema excludes internal
    // marker columns (_delta_is_deleted, _delta_upsert) automatically
    this.avroSchema = ArrowToAvroConverter.toAvroSchema(
        this.incomingFields, "DeltaRecord", "com.dremio.plugins.delta");

    // Resolve Hadoop configuration for filesystem access
    this.hadoopConf = new Configuration();

    // Resolve codec and target file size once; used by openParquetWriter() for each partition
    this.codec = resolveCodec(
        pluginConfig != null ? pluginConfig.compressionCodec : DeltaPluginConfig.CompressionCodec.SNAPPY);
    this.targetFileSize = pluginConfig != null ? pluginConfig.targetFileSizeBytes : 134_217_728L;

    // NOTE: the Parquet writer is NOT opened here. It is opened lazily in
    // writeBatch() (via openParquetWriter()) so that startPartition() can set
    // the correct partition path before any data is written.

    logger.info("DeltaRecordWriter initialized. Table: {}, Codec: {}", tablePath, codec);

    this.initialized = true;
  }

  /**
   * Called by Dremio before routing a new partition's rows to this writer.
   *
   * For each transition:
   *  1. Flush the previous partition's writer to pendingAddFiles (if any rows were written).
   *  2. Store the WritePartition for use in openParquetWriter().
   *  3. Null out parquetWriter — openParquetWriter() will open a new one in writeBatch().
   *
   * The actual partition path is resolved in openParquetWriter() via
   * {@code partition.getQualifiedPath(tablePath, fileName)} — Dremio constructs the
   * Hive-style directory path (e.g. region=us-east/) itself. We parse the resulting
   * path to populate {@link #currentPartitionDir} and {@link #currentPartitionValues}
   * for the Delta AddFile action.
   *
   * For non-partitioned tables, Dremio calls this once with WritePartition.NONE
   * whose getPartitionValues() returns null or "", resulting in files written
   * directly under the table root.
   */
  @Override
  public void startPartition(WritePartition partition) throws Exception {
    // Flush data written under the previous partition before switching
    if (parquetWriter != null) {
      if (rowsInCurrentFile > 0) {
        flushPartition();
      } else {
        // Writer was opened but no rows written (e.g. all rows were deleted)
        parquetWriter.close();
        cleanupEmptyFile();
        parquetWriter = null;
      }
    }

    // Reset per-partition state; openParquetWriter() will populate these
    rowsInCurrentFile      = 0;
    currentPartition       = partition;
    currentPartitionDir    = "";
    currentPartitionValues = new HashMap<>();

    logger.debug("startPartition called; partition values='{}'",
        partition != null ? partition.getPartitionValues() : "null");
  }

  /**
   * Called for each Arrow record batch. Converts each row to Avro and writes
   * it to the current Parquet file.
   *
   * Large-file splitting: after every row, {@code parquetWriter.getDataSize()}
   * is checked against {@code targetFileSizeBytes}. When the threshold is
   * exceeded the current file is flushed ({@link #flushPartition()}) and a new
   * file is opened ({@link #openParquetWriter()}) within the same partition.
   * This produces multiple appropriately-sized files per partition rather than
   * one unbounded file per writer thread.
   *
   * The check is done after each write rather than each batch so we don't
   * overshoot by a full batch size (up to 4096 rows). The cost is one
   * {@code getDataSize()} call per row — this is an O(1) field read inside
   * Parquet so the overhead is negligible.
   *
   * The Parquet writer is opened lazily on the first call after setup() or
   * startPartition() so that the correct partition-aware output path is used.
   *
   * Delete mode: rows where _delta_is_deleted=true are skipped.
   */
  @Override
  public int writeBatch(int offset, int length) throws IOException {
    if (!initialized) {
      throw new IllegalStateException("setup() must be called before writeBatch()");
    }

    // Lazy writer open: first write after setup() or startPartition()
    if (parquetWriter == null) {
      openParquetWriter();
    }

    int written = 0;
    for (int row = offset; row < offset + length; row++) {
      // Delete mode: check the _delta_is_deleted flag for this row
      if (isDeleteMode && isRowDeleted(row)) {
        continue;
      }

      GenericRecord avroRecord = ArrowToAvroConverter.toGenericRecord(
          this.incomingFields, avroSchema, this.vectorList, row);

      parquetWriter.write(avroRecord);
      rowsInCurrentFile++;
      totalRowsWritten++;
      written++;

      // File-size roll: if the current file has grown past the target size,
      // flush it and open a fresh file for the remaining rows in this batch.
      // We use getDataSize() (uncompressed in-memory estimate) as a proxy for
      // on-disk size. It is an overestimate for compressed columns — that's
      // intentional: better to produce slightly smaller files than slightly
      // larger ones.
      if (parquetWriter.getDataSize() >= targetFileSize) {
        logger.debug("File size roll: dataSize={} >= targetFileSize={}, partition='{}'",
            parquetWriter.getDataSize(), targetFileSize, currentPartitionDir);
        try {
          flushPartition();
          openParquetWriter();
        } catch (Exception e) {
          throw new IOException("Failed to roll to new Parquet file during size-based split", e);
        }
      }
    }

    logger.debug("writeBatch: processed {} rows, partition='{}', fileRows={}, totalRows={}",
        length, currentPartitionDir, rowsInCurrentFile, totalRowsWritten);
    return written;
  }

  @Override
  public void abort() throws IOException {
    try {
      if (parquetWriter != null) {
        parquetWriter.close();
        parquetWriter = null;
      }
      cleanupEmptyFile();
      logger.info("DeltaRecordWriter aborted; cleaned up current output file {}", parquetOutputPath);
      // Note: previously flushed partition files (in pendingAddFiles) remain on disk
      // but are not referenced in the Delta log — they will be cleaned up by VACUUM.
    } catch (Exception e) {
      throw new IOException("Error during DeltaRecordWriter abort", e);
    }
  }

  /**
   * Closes the Parquet writer for the last partition, then commits all accumulated
   * AddFile actions to the Delta log as a single atomic transaction.
   *
   * If no rows were written across all partitions, no Delta log entry is created.
   * All partitions written by this thread land in one Delta log version entry,
   * making the write atomically visible to readers.
   */
  @Override
  public void close() throws Exception {
    if (!initialized) {
      logger.info("DeltaRecordWriter.close(): writer was never initialized, nothing to do");
      return;
    }
    if (committed) {
      logger.debug("DeltaRecordWriter.close(): already committed, skipping duplicate close");
      return;
    }

    // Flush the last (or only) file in the current partition
    if (parquetWriter != null) {
      if (rowsInCurrentFile > 0) {
        flushPartition();
      } else {
        parquetWriter.close();
        cleanupEmptyFile();
        parquetWriter = null;
      }
    }

    if (pendingAddFiles.isEmpty()) {
      logger.info("DeltaRecordWriter.close(): no rows written to {}", tablePath);
      return;
    }

    // Open the Delta log and start an optimistic transaction
    DeltaLog deltaLog = DeltaLog.forTable(hadoopConf, tablePath);
    OptimisticTransaction txn = deltaLog.startTransaction();

    List<Action> actions = new ArrayList<>();

    // If this is a new table (no prior commits), write the Metadata action
    // which sets the Delta table schema, format, and partition columns.
    Snapshot snapshot = deltaLog.snapshot();
    if (snapshot.getVersion() < 0) {
      StructType deltaSchema = ArrowToDeltaSchemaConverter.toStructType(this.incomingFields);
      Metadata metadata = new Metadata(
          UUID.randomUUID().toString(),       // table id
          "",                                 // name (optional)
          "",                                 // description (optional)
          new Format("parquet", Collections.emptyMap()),
          resolvePartitionColumns(),          // List<String> partition column names
          buildTableConfiguration(),          // table properties (Map<String,String>)
          java.util.Optional.empty(),         // createdTime
          deltaSchema                         // StructType schema (last)
      );
      txn.updateMetadata(metadata);
      logger.info("New Delta table: committed schema with {} fields at {}",
          deltaSchema.getFields().length, tablePath);

    } else if (pluginConfig != null && pluginConfig.allowSchemaEvolution) {
      evolveSchemaIfNeeded(txn, snapshot, actions);
    }

    // Add all file AddFile actions accumulated during this write
    actions.addAll(pendingAddFiles);

    // Commit the transaction — writes a new JSON entry to _delta_log/
    Operation operation = new Operation(
        isUpsertMode ? Operation.Name.MERGE : Operation.Name.WRITE);
    txn.commit(actions, operation, "Dremio Delta Connector/1.0");
    committed = true;

    // Invalidate the snapshot cache so any read immediately following this
    // write sees the just-committed files rather than a stale cached list.
    DeltaSnapshotUtils.invalidateCache(tablePath);

    logger.info("Committed Delta transaction: {} file(s), {} total rows",
        pendingAddFiles.size(), totalRowsWritten);

    // Report stats to Dremio's query profile
    if (outputEntryListener != null) {
      long totalBytes = pendingAddFiles.stream().mapToLong(AddFile::getSize).sum();
      outputEntryListener.recordsWritten(
          totalRowsWritten, totalBytes, tablePath,
          null,  // metadata bytes
          1,     // operationType: WRITE
          null,  // icebergMetadata
          null,  // fileSchema
          null,  // partitionData
          null,  // partition index
          null,  // partitionValue
          0L,    // rejectedRecords
          null   // referencedDataFiles
      );
    }
  }

  // -----------------------------------------------------------------------
  // Partition flush
  // -----------------------------------------------------------------------

  /**
   * Closes the current Parquet writer and records an AddFile action for the
   * written file in {@link #pendingAddFiles}.
   *
   * Called by startPartition() when transitioning to a new partition, and by
   * close() for the final (or only) partition. After this call, parquetWriter
   * is null and rowsWritten is reset — ready for the next partition.
   */
  private void flushPartition() throws Exception {
    if (parquetWriter == null) return;

    // Capture footer BEFORE close() — getFooter() is populated during close()
    // so we call it first, then close.
    // Note: in Parquet 1.13, getFooter() returns the footer as written; it is
    // available after close() completes. We close first, then call getFooter().
    parquetWriter.close();

    // Capture Parquet footer for stats BEFORE nulling the writer reference.
    // getFooter() is valid immediately after close() on AvroParquetWriter.
    ParquetMetadata footer = null;
    try {
      footer = parquetWriter.getFooter();
    } catch (Exception e) {
      logger.debug("Could not read Parquet footer for stats ({}); stats will be null", e.getMessage());
    }
    parquetWriter = null;

    // Measure the file that was written
    FileSystem fs = FileSystem.get(parquetOutputPath.toUri(), hadoopConf);
    FileStatus status = fs.getFileStatus(parquetOutputPath);
    long fileSize = status.getLen();

    // Compute the path relative to the table root for the Delta log entry.
    // Delta log stores paths relative to the table root:
    //   "region=us-east/part-abc123.snappy.parquet"   (partitioned)
    //   "part-abc123.snappy.parquet"                  (non-partitioned)
    String relativePath = toRelativePath(parquetOutputPath.toString(), tablePath);

    // Build per-file column statistics for the Delta log using the per-file row count.
    // Stats enable data-skipping: Delta readers can prune files whose min/max
    // range does not overlap the query predicate, without scanning the file.
    //
    // Note on delta-standalone 0.6.0 @JsonRawValue bug:
    // When delta-standalone WRITES an AddFile with non-null stats, it emits the
    // String value as raw JSON (correct Delta format). When it READS the log back
    // via deltaLog.snapshot(), it tries to deserialize the raw JSON object into a
    // String field → MismatchedInputException. Our isFileInCurrentSnapshot() in
    // DeltaParquetRecordReader already catches this and falls back to allowing all
    // files (same behavior as null stats). External readers (Spark, Trino, DuckDB)
    // that implement the Delta protocol correctly WILL benefit from these stats.
    String stats = buildFileStats(footer, rowsInCurrentFile);

    AddFile addFile = new AddFile(
        relativePath,
        new HashMap<>(currentPartitionValues),  // snapshot of current partition values
        fileSize,
        System.currentTimeMillis(),
        true,    // dataChange: true for INSERT/UPSERT
        stats,   // column statistics JSON (null if footer unavailable)
        null     // tags
    );
    pendingAddFiles.add(addFile);

    logger.debug("Flushed file: partition='{}', path={}, rows={}, size={} bytes, stats={}",
        currentPartitionDir, relativePath, rowsInCurrentFile, fileSize,
        stats != null ? "present (" + footer.getBlocks().size() + " row groups)" : "null");

    rowsInCurrentFile = 0;
  }

  // -----------------------------------------------------------------------
  // Parquet writer initialization
  // -----------------------------------------------------------------------

  /**
   * Opens a new Parquet writer for the current partition.
   *
   * Path construction delegates to {@code WritePartition.getQualifiedPath()} so
   * that Dremio's own path logic (Hive-style col=val directories, S3/ADLS URI
   * handling) is used consistently. The resulting path is also used to derive
   * {@link #currentPartitionDir} and {@link #currentPartitionValues} for the
   * Delta AddFile action.
   *
   * Fallback: if currentPartition is null or its getPartitionValues() is empty,
   * files land directly under the table root (non-partitioned layout).
   */
  private void openParquetWriter() throws IOException {
    String fileName = "part-" + UUID.randomUUID() + "." + codec.name().toLowerCase() + ".parquet";

    // Use Dremio's partition path logic when a partition is active.
    // getQualifiedPath(location, fileName) builds the full path including the
    // Hive-style partition directory (e.g. region=us-east/part-xxx.parquet).
    String rawValues = currentPartition != null ? currentPartition.getPartitionValues() : null;
    boolean isPartitioned = rawValues != null && !rawValues.isEmpty();

    if (isPartitioned) {
      try {
        com.dremio.io.file.Path dremioPath = currentPartition.getQualifiedPath(tablePath, fileName);
        this.parquetOutputPath = new Path(dremioPath.toString());

        // Derive partition dir relative to table root by stripping the filename
        String fullPathStr = parquetOutputPath.toString();
        String parentStr   = parquetOutputPath.getParent().toString();
        currentPartitionDir = toRelativePath(parentStr, tablePath)
            .replaceAll("^/+|/+$", ""); // strip leading/trailing slashes

        // Parse Hive-style col=val segments from the directory for AddFile.partitionValues
        currentPartitionValues = new HashMap<>();
        for (String segment : currentPartitionDir.split("/")) {
          int eq = segment.indexOf('=');
          if (eq > 0) {
            currentPartitionValues.put(segment.substring(0, eq), segment.substring(eq + 1));
          }
        }

        logger.debug("Opened partitioned Parquet writer: dir='{}' path={} values={}",
            currentPartitionDir, parquetOutputPath, currentPartitionValues);

      } catch (Exception e) {
        // Fallback to non-partitioned path if getQualifiedPath() fails
        logger.warn("getQualifiedPath failed ({}); writing to table root instead", e.getMessage());
        this.parquetOutputPath = new Path(tablePath, fileName);
        currentPartitionDir    = "";
        currentPartitionValues = new HashMap<>();
      }
    } else {
      // Non-partitioned: write directly under the table root
      this.parquetOutputPath = new Path(tablePath, fileName);
      currentPartitionDir    = "";
      logger.debug("Opened non-partitioned Parquet writer: path={}", parquetOutputPath);
    }

    // Ensure the output directory exists (no-op if already present)
    FileSystem fs = FileSystem.get(parquetOutputPath.toUri(), hadoopConf);
    fs.mkdirs(parquetOutputPath.getParent());

    this.parquetWriter = AvroParquetWriter.<GenericRecord>builder(parquetOutputPath)
        .withSchema(avroSchema)
        .withConf(hadoopConf)
        .withCompressionCodec(codec)
        .withRowGroupSize(targetFileSize)
        .withDictionaryEncoding(true)
        .build();
  }

  // -----------------------------------------------------------------------
  // DELETE FROM support
  // -----------------------------------------------------------------------

  /**
   * TODO: Full DELETE FROM support.
   *
   * The current implementation only skips rows marked _delta_is_deleted=true
   * from the incoming batch. True DELETE FROM requires:
   *
   *   1. Identify which existing Parquet files contain rows matching the WHERE
   *      clause (DeltaSnapshotUtils.getLatestFiles() for all candidate files)
   *
   *   2. For each affected file:
   *      a. Read all rows from the Parquet file
   *      b. Filter out rows matching the delete predicate
   *      c. Write the remaining rows to a NEW Parquet file
   *      d. Create a RemoveFile action for the old file
   *      e. Create an AddFile action for the new file
   *
   *   3. Commit all RemoveFile + AddFile pairs in a single transaction
   *
   * This rewrite logic belongs in DeltaFormatPlugin as it needs access to the
   * existing table files. The DeltaRecordWriter's role would then be just to
   * write the "survivor" rows passed to it after filtering.
   *
   * For Delta 2.x+, Deletion Vectors (DVs) offer a more efficient alternative
   * to full file rewrites for deletes. DVs are bitmaps stored alongside the
   * Parquet files that mark deleted rows without rewriting the file.
   * Support for DVs can be added via the delta-kernel library.
   */
  private boolean isRowDeleted(int rowIndex) {
    if (deletedFieldIndex < 0 || deletedFieldIndex >= vectorList.size()) return false;
    try {
      org.apache.arrow.vector.ValueVector vec = vectorList.get(deletedFieldIndex);
      if (vec.isNull(rowIndex)) return false;
      Object val = vec.getObject(rowIndex);
      return Boolean.TRUE.equals(val) || Integer.valueOf(1).equals(val);
    } catch (Exception e) {
      logger.warn("Could not read _delta_is_deleted at row {}: {}", rowIndex, e.getMessage());
      return false;
    }
  }

  // -----------------------------------------------------------------------
  // Schema evolution
  // -----------------------------------------------------------------------

  /**
   * Checks if the incoming Arrow schema has columns missing from the current
   * Delta table schema. If so, and allowSchemaEvolution=true, updates the
   * table Metadata with the merged schema.
   */
  private void evolveSchemaIfNeeded(
      OptimisticTransaction txn, Snapshot snapshot, List<Action> actions) {

    StructType currentDeltaSchema = snapshot.getMetadata().getSchema();

    if (!ArrowToDeltaSchemaConverter.isSchemaCompatible(this.incomingFields, currentDeltaSchema)) {
      logger.info("Schema evolution: incoming schema has new columns. Updating Delta table metadata.");
      StructType newSchema = ArrowToDeltaSchemaConverter.toStructType(this.incomingFields);
      Metadata updatedMetadata = new Metadata(
          snapshot.getMetadata().getId(),
          snapshot.getMetadata().getName(),
          snapshot.getMetadata().getDescription(),
          snapshot.getMetadata().getFormat(),
          snapshot.getMetadata().getPartitionColumns(),
          snapshot.getMetadata().getConfiguration(),
          snapshot.getMetadata().getCreatedTime(),
          newSchema
      );
      txn.updateMetadata(updatedMetadata);
    }
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private String toRelativePath(String absolutePath, String tableRoot) {
    if (!tableRoot.endsWith("/")) tableRoot = tableRoot + "/";
    return absolutePath.startsWith(tableRoot)
        ? absolutePath.substring(tableRoot.length())
        : absolutePath;
  }

  /**
   * Returns the ordered list of partition column names for this table.
   * Sourced from DeltaPluginConfig.defaultPartitionColumn (single column for now).
   *
   * Future: support multi-column partitioning via a comma-separated config field
   * or a structured List<String> property.
   */
  private List<String> resolvePartitionColumns() {
    String col = pluginConfig != null ? pluginConfig.defaultPartitionColumn : "";
    if (col == null || col.isEmpty()) return Collections.emptyList();
    // Support comma-separated multi-column partition spec: "region,year"
    String[] cols = col.split(",");
    List<String> result = new ArrayList<>();
    for (String c : cols) {
      String trimmed = c.trim();
      if (!trimmed.isEmpty()) result.add(trimmed);
    }
    return result;
  }

  private Map<String, String> buildTableConfiguration() {
    Map<String, String> config = new HashMap<>();
    // Enable statistics collection for query planning / file skipping
    config.put("delta.dataSkippingNumIndexedCols", "32");
    return config;
  }

  private CompressionCodecName resolveCodec(DeltaPluginConfig.CompressionCodec codec) {
    if (codec == null) return CompressionCodecName.SNAPPY;
    switch (codec) {
      case GZIP:         return CompressionCodecName.GZIP;
      case ZSTD:         return CompressionCodecName.ZSTD;
      case UNCOMPRESSED: return CompressionCodecName.UNCOMPRESSED;
      default:           return CompressionCodecName.SNAPPY;
    }
  }

  // -----------------------------------------------------------------------
  // Column statistics for Delta log
  // -----------------------------------------------------------------------

  /**
   * Builds the Delta-protocol column statistics JSON for an AddFile action.
   *
   * Delta format:
   * <pre>
   * {
   *   "numRecords": 100,
   *   "minValues": {"user_id": 1,  "region": "ap-south"},
   *   "maxValues": {"user_id": 100, "region": "us-west"},
   *   "nullCount": {"user_id": 0,  "region": 5}
   * }
   * </pre>
   *
   * Min/max values are merged across all Parquet row groups so the stats
   * represent the complete file. Only columns with valid (non-null) statistics
   * are included — dictionary-only pages may omit statistics.
   *
   * Returns {@code null} if footer is null or contains no blocks (empty file).
   *
   * @param footer    Parquet footer from {@code ParquetWriter.getFooter()}
   * @param rowsWritten  Row count from our own counter (used as numRecords)
   */
  private String buildFileStats(ParquetMetadata footer, long rowsWritten) {
    if (footer == null || footer.getBlocks().isEmpty()) return null;

    // --- Merge statistics across all row groups ---
    // Use LinkedHashMap to preserve column order in the output JSON.
    Map<String, Object> minValues  = new LinkedHashMap<>();
    Map<String, Object> maxValues  = new LinkedHashMap<>();
    Map<String, Long>   nullCounts = new LinkedHashMap<>();

    for (BlockMetaData block : footer.getBlocks()) {
      for (ColumnChunkMetaData col : block.getColumns()) {
        String colName = col.getPath().toDotString();

        // Accumulate null count (always present even when min/max are absent)
        Statistics<?> stats = col.getStatistics();
        if (stats == null) continue;

        nullCounts.merge(colName, stats.getNumNulls(), Long::sum);

        // Skip min/max if this column has no non-null values in this row group
        if (!stats.hasNonNullValue()) continue;

        Object minVal = toStatsValue(stats.genericGetMin(), col.getPrimitiveType().getPrimitiveTypeName());
        Object maxVal = toStatsValue(stats.genericGetMax(), col.getPrimitiveType().getPrimitiveTypeName());
        if (minVal == null) continue;

        // Merge min: keep the smaller of existing vs new
        minValues.merge(colName, minVal, (existing, candidate) ->
            compareStatsValues(existing, candidate) <= 0 ? existing : candidate);
        // Merge max: keep the larger
        maxValues.merge(colName, maxVal, (existing, candidate) ->
            compareStatsValues(existing, candidate) >= 0 ? existing : candidate);
      }
    }

    // --- Serialize to JSON ---
    // We build the JSON manually to avoid adding new dependencies and to
    // avoid the shaded-Jackson complexity in the fat JAR.
    StringBuilder sb = new StringBuilder("{");
    sb.append("\"numRecords\":").append(rowsWritten);

    sb.append(",\"minValues\":{");
    appendStatsMap(sb, minValues);
    sb.append("}");

    sb.append(",\"maxValues\":{");
    appendStatsMap(sb, maxValues);
    sb.append("}");

    sb.append(",\"nullCount\":{");
    appendStatsMap(sb, nullCounts);
    sb.append("}");

    sb.append("}");
    return sb.toString();
  }

  /**
   * Converts a Parquet statistics value to a JSON-serializable Java object.
   *
   * Parquet type → Delta stats JSON type:
   *   INT32, INT64 → number (Long)
   *   FLOAT        → number (Float)
   *   DOUBLE       → number (Double)
   *   BOOLEAN      → boolean (Boolean)
   *   BINARY       → string (UTF-8 decoded)
   *   FIXED_LEN_BYTE_ARRAY → hex string (for decimal/UUID encoded bytes)
   *
   * Returns null for types where min/max are not meaningful (e.g. raw BINARY
   * that is not valid UTF-8).
   */
  @SuppressWarnings("unchecked")
  private Object toStatsValue(Object raw, PrimitiveTypeName type) {
    if (raw == null) return null;
    try {
      switch (type) {
        case INT32:   return ((Number) raw).longValue();
        case INT64:   return ((Number) raw).longValue();
        case FLOAT:   return ((Number) raw).floatValue();
        case DOUBLE:  return ((Number) raw).doubleValue();
        case BOOLEAN: return (Boolean) raw;
        case BINARY:
          // Parquet BINARY stats store the raw bytes as a Binary object.
          // Most string columns are UTF-8 encoded BINARY — decode them.
          org.apache.parquet.io.api.Binary bin = (org.apache.parquet.io.api.Binary) raw;
          try {
            return new String(bin.getBytes(), StandardCharsets.UTF_8);
          } catch (Exception e) {
            return null; // non-UTF-8 binary; skip stats for this column
          }
        case FIXED_LEN_BYTE_ARRAY:
          // Used for DECIMAL, UUID — represent as null (no readable min/max)
          return null;
        default:
          return null;
      }
    } catch (Exception e) {
      logger.debug("Could not convert stats value for type {}: {}", type, e.getMessage());
      return null;
    }
  }

  /**
   * Compares two stats values of the same type for min/max merging.
   * Returns negative if a < b, zero if equal, positive if a > b.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private int compareStatsValues(Object a, Object b) {
    if (a instanceof Comparable && b instanceof Comparable) {
      try {
        return ((Comparable) a).compareTo(b);
      } catch (ClassCastException e) {
        return 0;
      }
    }
    return 0;
  }

  /**
   * Appends a map of column name → value pairs as JSON key-value entries
   * into an already-open JSON object (i.e., after the opening "{").
   *
   * Strings are JSON-escaped. Numbers and booleans are emitted as-is.
   */
  private void appendStatsMap(StringBuilder sb, Map<String, ?> map) {
    boolean first = true;
    for (Map.Entry<String, ?> entry : map.entrySet()) {
      if (!first) sb.append(",");
      first = false;
      sb.append("\"").append(jsonEscape(entry.getKey())).append("\":");
      appendStatsValue(sb, entry.getValue());
    }
  }

  private void appendStatsValue(StringBuilder sb, Object value) {
    if (value == null) {
      sb.append("null");
    } else if (value instanceof String) {
      sb.append("\"").append(jsonEscape((String) value)).append("\"");
    } else if (value instanceof Boolean) {
      sb.append(value);
    } else if (value instanceof Number) {
      // Emit integers without decimal point, floats with full precision
      if (value instanceof Long || value instanceof Integer) {
        sb.append(((Number) value).longValue());
      } else if (value instanceof Float) {
        sb.append(((Float) value).doubleValue()); // promote to avoid 1.0E7 notation issues
      } else {
        sb.append(value);
      }
    } else {
      sb.append("\"").append(jsonEscape(value.toString())).append("\"");
    }
  }

  /**
   * Escapes a string for safe embedding in a JSON value.
   * Handles the characters that must be escaped per the JSON spec.
   */
  private String jsonEscape(String s) {
    if (s == null) return "";
    StringBuilder out = new StringBuilder(s.length());
    for (int i = 0; i < s.length(); i++) {
      char c = s.charAt(i);
      switch (c) {
        case '"':  out.append("\\\""); break;
        case '\\': out.append("\\\\"); break;
        case '\b': out.append("\\b");  break;
        case '\f': out.append("\\f");  break;
        case '\n': out.append("\\n");  break;
        case '\r': out.append("\\r");  break;
        case '\t': out.append("\\t");  break;
        default:
          if (c < 0x20) {
            out.append(String.format("\\u%04x", (int) c));
          } else {
            out.append(c);
          }
      }
    }
    return out.toString();
  }

  private void cleanupEmptyFile() {
    try {
      if (parquetOutputPath != null && hadoopConf != null) {
        FileSystem fs = FileSystem.get(parquetOutputPath.toUri(), hadoopConf);
        if (fs.exists(parquetOutputPath)) {
          fs.delete(parquetOutputPath, false);
          logger.debug("Deleted empty/aborted Parquet file: {}", parquetOutputPath);
        }
      }
    } catch (IOException e) {
      logger.warn("Could not delete empty Parquet file {}: {}", parquetOutputPath, e.getMessage());
    }
  }
}
