package com.dremio.plugins.hudi.write;

import com.dremio.exec.catalog.PluginSabotContext;
import com.dremio.exec.physical.base.WriterOptions;
import com.dremio.exec.record.BatchSchema;
import com.dremio.exec.record.VectorAccessible;
import com.dremio.exec.record.VectorWrapper;
import com.dremio.exec.store.RecordWriter;
import com.dremio.exec.store.WritePartition;
import com.dremio.plugins.hudi.conf.HudiPluginConfig;
import com.dremio.plugins.hudi.read.HudiSnapshotUtils;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Core write component: translates Dremio's Arrow-based write pipeline
 * into Apache Hudi commits via HoodieJavaWriteClient.
 *
 * Threading Model
 * ---------------
 * Dremio creates one HudiRecordWriter per executor thread. Each writer
 * accumulates records in a bounded in-memory buffer. When the buffer
 * reaches maxBufferRows it is flushed to storage (insert/upsert) without
 * committing. All accumulated WriteStatus objects are committed atomically
 * in close(), making the entire write a single Hudi timeline instant.
 *
 * Operation Modes
 * ---------------
 *   INSERT INTO / CTAS  -> insert()   (isUpsertMode=false, isDeleteMode=false)
 *   MERGE INTO / UPDATE -> upsert()   (isUpsertMode=true)
 *   DELETE FROM         -> delete()   (isDeleteMode=true)
 *
 * Detecting MERGE/UPDATE mode
 * ---------------------------
 * Dremio's WriterOptions API for signalling DML type is version-dependent.
 * The detectUpsertMode() method contains a hook point with clear comments
 * on where to wire in the Dremio API for the target version. Until that is
 * wired, MERGE INTO can be forced via a schema marker column.
 *
 * Detecting DELETE mode
 * ---------------------
 * If the incoming schema contains a boolean column named "_hoodie_is_deleted",
 * any row where that field is true is routed to writeClient.delete() rather
 * than insert/upsert. This is compatible with Hudi's own delete marker convention.
 *
 * Arrow -> Avro Conversion
 * ------------------------
 * Dremio's execution engine works exclusively with Apache Arrow columnar
 * format (VectorSchemaRoot / VectorAccessible). Hudi's Java client expects
 * Avro GenericRecord objects. ArrowToAvroConverter handles this translation.
 */
public class HudiRecordWriter implements RecordWriter {

  private static final Logger logger = LoggerFactory.getLogger(HudiRecordWriter.class);

  /** Hudi's conventional column name for marking rows as deleted. */
  private static final String HOODIE_IS_DELETED_FIELD = "_hoodie_is_deleted";

  /**
   * Hudi's conventional column name for signalling that an operation is an upsert
   * (MERGE INTO). Dremio can insert this column into the write schema to trigger
   * upsert routing without needing a version-specific WriterOptions API.
   */
  private static final String HOODIE_UPSERT_MARKER_FIELD = "_hoodie_upsert";

  private final String tablePath;
  private final WriterOptions writerOptions;
  private final HudiPluginConfig pluginConfig;
  private final PluginSabotContext context;

  // Schema derived from incoming Arrow data in setup() — used in writeBatch()
  private List<org.apache.arrow.vector.types.pojo.Field> incomingFields;

  // Retained from setup() so writeBatch() can pass it to the Arrow -> Avro converter
  private VectorAccessible incoming;

  // Vectors extracted from incoming in setup() — stable for the lifetime of this writer
  private List<org.apache.arrow.vector.ValueVector> vectorList;

  // Hudi write client - initialized lazily on first batch
  private HoodieJavaWriteClient<HoodieAvroPayload> writeClient;

  // Avro schema derived from incoming Arrow schema
  private Schema avroSchema;

  // Commit timestamp - must be monotonically increasing (Hudi uses epoch millis)
  private String commitTime;

  // --- Operation mode flags (set in setup()) ---
  private boolean isUpsertMode = false;
  private boolean isDeleteMode = false;

  // --- Buffer management ---
  // Row buffer for INSERT / UPSERT operations
  private final List<HoodieRecord<HoodieAvroPayload>> recordBuffer = new ArrayList<>();
  // Key buffer for DELETE FROM operations
  private final List<HoodieKey> deleteKeyBuffer = new ArrayList<>();
  // Accumulated WriteStatus objects from all intermediate flushes; committed atomically in close()
  private final List<WriteStatus> pendingStatuses = new ArrayList<>();
  // Max rows to buffer before an intermediate flush (configurable via HudiPluginConfig)
  private int maxBufferRows;

  // Hadoop configuration — set in setup(), reused by maybeRunAutoCompaction()
  private Configuration hadoopConf;

  // Whether setup() has been called
  private boolean initialized = false;

  // Output stats
  private OutputEntryListener outputEntryListener;
  private WriteStatsListener writeStatsListener;

  public HudiRecordWriter(
      String tablePath,
      WriterOptions writerOptions,
      HudiPluginConfig pluginConfig,
      PluginSabotContext context) {
    this.tablePath = tablePath;
    this.writerOptions = writerOptions;
    this.pluginConfig = pluginConfig;
    this.context = context;
  }

  // -----------------------------------------------------------------------
  // RecordWriter interface - Lifecycle
  // -----------------------------------------------------------------------

  @Override
  public void setup(VectorAccessible incoming, OutputEntryListener listener, WriteStatsListener statsListener)
      throws IOException {
    this.outputEntryListener = listener;
    this.writeStatsListener = statsListener;
    this.incoming = incoming;

    // Cache vectors in field order for use in writeBatch()
    this.vectorList = new ArrayList<>();
    for (VectorWrapper<?> vw : incoming) {
      this.vectorList.add(vw.getValueVector());
    }

    // 1. Resolve buffer size
    this.maxBufferRows = pluginConfig != null ? pluginConfig.writeBufferMaxRows : 100_000;

    // 2. Detect operation mode from schema markers and WriterOptions
    BatchSchema schema = incoming.getSchema();
    this.isDeleteMode  = detectDeleteMode(schema);
    this.isUpsertMode  = !isDeleteMode && detectUpsertMode(writerOptions, schema);

    logger.info("HudiRecordWriter mode: isUpsertMode={}, isDeleteMode={}, maxBufferRows={}",
        isUpsertMode, isDeleteMode, maxBufferRows);

    // 3. Derive Avro schema from incoming Arrow schema
    this.incomingFields = schema.getFields();
    this.avroSchema = ArrowToAvroConverter.toAvroSchema(
        this.incomingFields, "HudiRecord", "com.dremio.plugins.hudi");
    logger.info("Derived Avro schema:\n{}", avroSchema.toString(true));

    // 4. Resolve Hudi write config fields
    String recordKeyField = resolveConfig(pluginConfig != null ? pluginConfig.defaultRecordKeyField : "id");
    String partitionField  = resolveConfig(pluginConfig != null ? pluginConfig.defaultPartitionPathField : "");
    String precombineField = resolveConfig(pluginConfig != null ? pluginConfig.defaultPrecombineField : "ts");
    int parallelism        = pluginConfig != null ? pluginConfig.writeParallelism : 2;
    long targetFileSize    = pluginConfig != null ? pluginConfig.targetFileSizeBytes : 134_217_728L;

    org.apache.hudi.common.model.HoodieTableType tableType =
        (pluginConfig != null
            && pluginConfig.defaultTableType == HudiPluginConfig.HudiTableType.MERGE_ON_READ)
            ? org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ
            : org.apache.hudi.common.model.HoodieTableType.COPY_ON_WRITE;

    java.util.Properties hoodieProps = new java.util.Properties();
    hoodieProps.put(HoodieTableConfig.RECORDKEY_FIELDS.key(), recordKeyField);
    hoodieProps.put(HoodieTableConfig.PARTITION_FIELDS.key(), partitionField);
    hoodieProps.put(HoodieTableConfig.PRECOMBINE_FIELD.key(), precombineField);
    hoodieProps.put(HoodieTableConfig.TYPE.key(), tableType.name());
    // Parquet max file size (replaces removed withParquetMaxFileSize() in 0.15.0)
    hoodieProps.put("hoodie.parquet.max.file.size", String.valueOf(targetFileSize));

    HoodieWriteConfig hudiWriteConfig = HoodieWriteConfig.newBuilder()
        .withPath(tablePath)
        .withSchema(avroSchema.toString())
        .withProps(hoodieProps)
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withIndexConfig(
            HoodieIndexConfig.newBuilder()
                .withIndexType(HoodieIndex.IndexType.INMEMORY)
                .build())
        // autoCommit=false: we control commit timing in close() so that all
        // intermediate flushes land in a single Hudi timeline instant.
        .withAutoCommit(false)
        .build();

    // 5. Initialize Hadoop configuration (also stored as instance field for auto-compaction)
    this.hadoopConf = new Configuration();

    // 6. Initialize the Hudi table on-disk if it does not already exist.
    //    HoodieJavaWriteClient.startCommit() requires .hoodie/ to be present;
    //    HoodieTableMetaClient.initTable() creates it idempotently (no-op if
    //    the table already exists and has the same properties).
    org.apache.hudi.common.table.HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(tableType.name())
        .setTableName(new org.apache.hadoop.fs.Path(tablePath).getName())
        .setPayloadClass(org.apache.hudi.common.model.HoodieAvroPayload.class)
        .setRecordKeyFields(recordKeyField)
        .setPartitionFields(partitionField)
        .setPreCombineField(precombineField)
        .initTable(new HadoopStorageConfiguration(hadoopConf), tablePath);
    logger.info("Hudi table initialized (or already exists) at {}", tablePath);

    HoodieJavaEngineContext engineContext =
        new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf));

    // 7. Instantiate write client and start a new commit instant
    this.writeClient = new HoodieJavaWriteClient<>(engineContext, hudiWriteConfig);
    this.commitTime  = writeClient.startCommit();
    logger.info("HudiRecordWriter initialized. Table: {}, CommitTime: {}", tablePath, commitTime);

    this.initialized = true;
  }

  /**
   * Called for each Arrow record batch flowing through the pipeline.
   *
   * Routes each row to the appropriate buffer:
   *   - DELETE FROM: extract HoodieKey and add to deleteKeyBuffer
   *   - INSERT / UPSERT: convert to HoodieRecord and add to recordBuffer;
   *     flush recordBuffer to storage when it reaches maxBufferRows
   */
  @Override
  public int writeBatch(int offset, int length) throws IOException {
    if (!initialized) {
      throw new IllegalStateException("setup() must be called before writeBatch()");
    }

    int written = 0;
    for (int row = offset; row < offset + length; row++) {
      if (isDeleteMode) {
        // DELETE FROM: we only need the record key + partition path
        GenericRecord avroRecord = ArrowToAvroConverter.toGenericRecord(
            this.incomingFields, avroSchema, this.vectorList, row);
        String recordKey    = extractField(avroRecord, resolveConfig(
            pluginConfig != null ? pluginConfig.defaultRecordKeyField : "id"));
        String partitionPath = extractPartitionPath(avroRecord);
        deleteKeyBuffer.add(new HoodieKey(recordKey, partitionPath));
        written++;

      } else {
        // INSERT / UPSERT: full record conversion
        GenericRecord avroRecord = ArrowToAvroConverter.toGenericRecord(
            this.incomingFields, avroSchema, this.vectorList, row);

        String recordKey    = extractField(avroRecord, resolveConfig(
            pluginConfig != null ? pluginConfig.defaultRecordKeyField : "id"));
        String partitionPath = extractPartitionPath(avroRecord);

        HoodieKey hoodieKey = new HoodieKey(recordKey, partitionPath);
        HoodieAvroRecord<HoodieAvroPayload> hoodieRecord = new HoodieAvroRecord<>(
            hoodieKey,
            new HoodieAvroPayload(org.apache.hudi.common.util.Option.of(avroRecord)));
        recordBuffer.add(hoodieRecord);
        written++;

        // Flush to storage when buffer is full; final commit happens in close()
        if (recordBuffer.size() >= maxBufferRows) {
          flushRecordBuffer();
        }
      }
    }

    logger.debug("writeBatch: processed {} rows. recordBuffer={}, deleteKeyBuffer={}",
        length, recordBuffer.size(), deleteKeyBuffer.size());
    return written;
  }

  @Override
  public void abort() throws IOException {
    try {
      if (writeClient != null && commitTime != null) {
        writeClient.rollback(commitTime);
        logger.info("HudiRecordWriter aborted and rolled back instant {}", commitTime);
      }
    } finally {
      recordBuffer.clear();
      deleteKeyBuffer.clear();
      pendingStatuses.clear();
      if (writeClient != null) {
        writeClient.close();
      }
    }
  }

  @Override
  public void startPartition(WritePartition partition) throws Exception {
    // No-op: we commit everything in close()
  }

  /**
   * Flush all remaining buffers and commit the single Hudi timeline instant.
   *
   * All intermediate flushes (from writeBatch threshold hits) and the final
   * flush here are committed together as one atomic instant. This means the
   * write is all-or-nothing from Hudi's perspective regardless of how many
   * intermediate flushes occurred.
   */
  @Override
  public void close() throws Exception {
    if (!initialized) {
      logger.info("HudiRecordWriter.close(): writer was never initialized, nothing to commit");
      return;
    }

    try {
      // Flush any remaining INSERT/UPSERT records
      flushRecordBuffer();

      // Process DELETE keys
      if (!deleteKeyBuffer.isEmpty()) {
        logger.info("Deleting {} records from Hudi table {}", deleteKeyBuffer.size(), tablePath);
        List<WriteStatus> statuses = writeClient.delete(
            new ArrayList<>(deleteKeyBuffer), commitTime);
        checkStatuses(statuses, "delete");
        pendingStatuses.addAll(statuses);
        deleteKeyBuffer.clear();
      }

      if (pendingStatuses.isEmpty()) {
        logger.info("HudiRecordWriter.close(): no records written to {}", tablePath);
        return;
      }

      // Atomic commit of all pending statuses as one Hudi instant
      logger.info("Committing {} WriteStatus objects to Hudi table {} at instant {}",
          pendingStatuses.size(), tablePath, commitTime);
      writeClient.commit(commitTime, pendingStatuses);
      logger.info("Successfully committed to Hudi table {}", tablePath);

      // Invalidate the snapshot cache so the immediately following SELECT
      // sees the fresh files written by this commit rather than a stale snapshot.
      HudiSnapshotUtils.invalidateCache(tablePath);

      // Trigger auto-clustering (COW) or auto-compaction (MOR) if configured.
      // Must be called before the finally block closes writeClient.
      maybeRunAutoCompaction();

      // Report stats back to Dremio's WriterCommitterOperator so it can
      // register the dataset in the catalog as a mutable USER_TABLE.
      if (outputEntryListener != null) {
        long totalRows = pendingStatuses.stream().mapToLong(WriteStatus::getTotalRecords).sum();

        // Serialize the Arrow schema so WriterCommitter can infer the dataset schema
        // for catalog registration. Without this, the WriterCommitter skips registration.
        byte[] schemaBytes = null;
        try {
          BatchSchema batchSchema = incoming.getSchema();
          schemaBytes = batchSchema.serialize();
        } catch (Exception e) {
          logger.warn("Could not serialize schema for catalog registration: {}", e.getMessage());
        }

        outputEntryListener.recordsWritten(
            totalRows,
            0L,        // byteCount: getTotalBytesWritten() removed in Hudi 0.15.0
            tablePath,
            null,      // metadata bytes
            1,         // operationType: WRITE
            null,      // icebergMetadata
            schemaBytes, // fileSchema: serialized Arrow schema for catalog registration
            null,      // partitionData
            null,      // partition index
            null,      // partitionValue
            0L,        // rejectedRecords
            null       // referencedDataFiles
        );
      }

    } finally {
      if (writeClient != null) {
        writeClient.close();
      }
      recordBuffer.clear();
      deleteKeyBuffer.clear();
      pendingStatuses.clear();
    }
  }

  // -----------------------------------------------------------------------
  // Buffer management
  // -----------------------------------------------------------------------

  /**
   * Flushes the current recordBuffer to Hudi storage without committing.
   * The resulting WriteStatus objects are accumulated in pendingStatuses.
   * The single commit for all flushes happens in close().
   */
  private void flushRecordBuffer() throws IOException {
    if (recordBuffer.isEmpty()) return;

    logger.debug("Flushing {} records to Hudi table {} (upsertMode={})",
        recordBuffer.size(), tablePath, isUpsertMode);

    List<WriteStatus> statuses;
    if (isUpsertMode) {
      statuses = writeClient.upsert(new ArrayList<>(recordBuffer), commitTime);
    } else {
      statuses = writeClient.insert(new ArrayList<>(recordBuffer), commitTime);
    }

    checkStatuses(statuses, isUpsertMode ? "upsert" : "insert");
    pendingStatuses.addAll(statuses);
    recordBuffer.clear();
  }

  /**
   * Triggers auto-clustering (COW) or auto-compaction (MOR) after every N commits,
   * where N is {@code HudiPluginConfig.clusteringCommitInterval}.
   *
   * <p>This is <b>best-effort</b>: any exception is caught, logged as a warning, and
   * swallowed so that clustering failures never roll back or fail the preceding
   * write commit.
   *
   * <p>COW clustering: bins many small Parquet files into fewer large ones using
   * {@code HoodieJavaWriteClient.scheduleClustering()} + {@code cluster()}.
   *
   * <p>MOR compaction: merges Avro delta logs into base Parquet files using
   * {@code HoodieJavaWriteClient.scheduleCompaction()} + {@code compact()}.
   */
  private void maybeRunAutoCompaction() {
    if (pluginConfig == null) return;
    int interval = pluginConfig.clusteringCommitInterval;
    if (interval <= 0) {
      logger.debug("Auto-compaction disabled (interval=0)");
      return;
    }

    try {
      // Read current timeline to count completed commits
      org.apache.hudi.common.table.HoodieTableMetaClient metaClient =
          HudiSnapshotUtils.buildMetaClient(tablePath, hadoopConf);

      long completedCommits = metaClient.getActiveTimeline()
          .getCommitsTimeline()
          .filterCompletedInstants()
          .getInstants()
          .size();

      logger.info("Auto-compaction check: {} completed commits, interval={}",
          completedCommits, interval);

      if (completedCommits % interval != 0) {
        logger.debug("Auto-compaction skipped (next trigger at commit #{})",
            ((completedCommits / interval) + 1) * interval);
        return;
      }

      boolean isMOR = metaClient.getTableType()
          == org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

      if (isMOR) {
        // MOR: merge delta logs into base Parquet files (true compaction)
        logger.info("Triggering MOR compaction for table {} after {} commits",
            tablePath, completedCommits);
        org.apache.hudi.common.util.Option<String> compactionInstant =
            writeClient.scheduleCompaction(org.apache.hudi.common.util.Option.empty());
        if (compactionInstant.isPresent()) {
          writeClient.compact(compactionInstant.get());
          logger.info("MOR compaction completed successfully for table {}", tablePath);
          // Invalidate cache again so the compacted files are visible to the next read
          HudiSnapshotUtils.invalidateCache(tablePath);
        } else {
          logger.info("MOR compaction: no plan generated for {} "
              + "(no delta logs pending or table already up-to-date)", tablePath);
        }

      } else {
        // COW: bin-pack small files into larger ones (clustering)
        logger.info("Triggering COW clustering for table {} after {} commits",
            tablePath, completedCommits);
        org.apache.hudi.common.util.Option<String> clusteringInstant =
            writeClient.scheduleClustering(org.apache.hudi.common.util.Option.empty());
        if (clusteringInstant.isPresent()) {
          writeClient.cluster(clusteringInstant.get(), true);
          logger.info("COW clustering completed successfully for table {}", tablePath);
          // Invalidate cache so the clustered (merged) files are visible to the next read
          HudiSnapshotUtils.invalidateCache(tablePath);
        } else {
          logger.info("COW clustering: no plan generated for {} "
              + "(files may already be optimally sized)", tablePath);
        }
      }

    } catch (Exception e) {
      // Best-effort: log but never fail the write that already committed
      logger.warn("Auto-compaction/clustering failed (non-fatal — write already committed): "
          + "{}: {}", e.getClass().getSimpleName(), e.getMessage(), e);
    }
  }

  /**
   * Checks a WriteStatus list for errors. Rolls back the commit and throws
   * IOException if any errors are found.
   */
  private void checkStatuses(List<WriteStatus> statuses, String operation) throws IOException {
    long errorCount = statuses.stream().mapToLong(WriteStatus::getTotalErrorRecords).sum();
    if (errorCount > 0) {
      logger.error("Hudi {} produced {} error records. Rolling back instant {}.",
          operation, errorCount, commitTime);
      writeClient.rollback(commitTime);
      throw new IOException(String.format(
          "Hudi %s to %s failed with %d error records", operation, tablePath, errorCount));
    }
  }

  // -----------------------------------------------------------------------
  // Operation mode detection
  // -----------------------------------------------------------------------

  /**
   * Returns true if the incoming schema contains Hudi's standard delete marker
   * column (_hoodie_is_deleted). Any row where this column is true will be
   * routed to writeClient.delete() instead of insert/upsert.
   *
   * This convention is compatible with how Hudi's own Spark DataSource and
   * Flink sink signal deletes via schema markers.
   */
  private static boolean detectDeleteMode(BatchSchema schema) {
    return schema.getFields().stream()
        .anyMatch(f -> HOODIE_IS_DELETED_FIELD.equals(f.getName()));
  }

  /**
   * Returns true if the write operation should use upsert() (i.e., MERGE INTO
   * or UPDATE statement).
   *
   * Detection strategy (in order of preference):
   *
   * 1. Schema marker: if incoming schema contains "_hoodie_upsert" boolean column.
   *    Dremio planners can inject this column for MERGE INTO operations.
   *
   * 2. WriterOptions API (version-dependent):
   *    Uncomment and adapt the code below to match your Dremio version's API:
   *
   *    // Dremio 25.x / 26.x  — check IcebergTableProps or WriterOptions directly:
   *    // if (options.getTableFormatOptions() instanceof HudiWriterOptions) {
   *    //   HudiWriterOptions hwo = (HudiWriterOptions) options.getTableFormatOptions();
   *    //   return hwo.isUpsert();
   *    // }
   *    //
   *    // Generic approach for any version:
   *    // try {
   *    //   java.lang.reflect.Method m = options.getClass().getMethod("isUpsert");
   *    //   return Boolean.TRUE.equals(m.invoke(options));
   *    // } catch (ReflectiveOperationException ignored) {}
   *
   * @param options  WriterOptions from Dremio's planner
   * @param schema   Incoming Arrow schema (checked for marker columns)
   */
  private static boolean detectUpsertMode(WriterOptions options, BatchSchema schema) {
    // Schema marker check (portable across all Dremio versions)
    boolean hasMarker = schema.getFields().stream()
        .anyMatch(f -> HOODIE_UPSERT_MARKER_FIELD.equals(f.getName()));
    if (hasMarker) return true;

    // TODO: wire in Dremio version-specific WriterOptions API as described above
    return false;
  }

  // -----------------------------------------------------------------------
  // Helpers
  // -----------------------------------------------------------------------

  private String extractField(GenericRecord record, String fieldName) {
    if (fieldName == null || fieldName.isEmpty()) {
      return UUID.randomUUID().toString();
    }
    Object value = record.get(fieldName);
    return value != null ? value.toString() : UUID.randomUUID().toString();
  }

  private String extractPartitionPath(GenericRecord record) {
    String partitionField = pluginConfig != null ? pluginConfig.defaultPartitionPathField : "";
    if (partitionField == null || partitionField.isEmpty()) {
      return "";
    }
    Object value = record.get(partitionField);
    return value != null ? value.toString() : "default";
  }

  private String resolveConfig(String value) {
    return value != null ? value : "";
  }
}
