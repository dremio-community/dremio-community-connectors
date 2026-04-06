package com.dremio.plugins.hudi.read;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.common.util.Option;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Reads an Apache Hudi MERGE_ON_READ (MOR) file slice and produces a merged
 * snapshot of all records as Avro GenericRecord objects.
 *
 * What is a MOR file slice?
 * -------------------------
 * A Hudi MOR table stores writes as delta log files (Avro/HFile format) and
 * periodically compacts them into base Parquet files. A "file slice" is the
 * unit of reading: one base file + all log files for the same file group that
 * were written after the most recent compaction.
 *
 * To read the current snapshot of a file group you must:
 *   1. Read the base Parquet file (if present) -> "base records"
 *   2. Read all log files in order -> "delta records" (inserts, updates, deletes)
 *   3. Merge: for each record key in the deltas, replace or delete the base record
 *
 * This class wraps Hudi's HoodieMergedLogRecordScanner to handle step 2+3.
 * Step 1 (base Parquet reading) is delegated to Dremio's existing Parquet scanner,
 * which already handles this efficiently. The integration pattern is:
 *
 *   a. Dremio reads the base Parquet file via its native Parquet reader
 *   b. HudiMORRecordReader reads all log files and builds a merge map
 *   c. The merge map is applied to the Parquet output stream to produce the final result
 *
 * Usage
 * -----
 * <pre>
 *   try (HudiMORRecordReader reader = new HudiMORRecordReader(
 *       fileSlice, tableSchema, metaClient, hadoopConf)) {
 *
 *     // Check if any updates/deletes are pending in the logs
 *     if (reader.hasLogData()) {
 *       Map<String, Option<GenericRecord>> mergeMap = reader.buildMergeMap();
 *       // Apply mergeMap to base Parquet records:
 *       //   - key present, value non-empty -> use log record (update)
 *       //   - key present, value empty     -> skip record (delete)
 *       //   - key absent                   -> use base record (unchanged)
 *       //   - insertOnly keys              -> reader.getInsertOnlyRecords() for new rows
 *     } else {
 *       // No log data: read base Parquet file directly, no merge needed
 *     }
 *   }
 * </pre>
 *
 * Integration with Dremio's scan path
 * ------------------------------------
 * Full integration requires implementing a custom Dremio GroupScan that:
 *   1. Calls HudiSnapshotUtils.getLatestFileSlices() to enumerate file slices
 *   2. For each slice with log files, wraps it in HudiMORRecordReader
 *   3. Feeds the merge map to a custom RecordReader that applies it to the
 *      Parquet base file records as they are scanned
 *
 * See HudiFormatPlugin for the GroupScan hook points.
 */
public class HudiMORRecordReader implements Closeable {

  private static final Logger logger = LoggerFactory.getLogger(HudiMORRecordReader.class);

  private final FileSlice fileSlice;
  private final Schema tableSchema;
  private final HoodieTableMetaClient metaClient;
  private final Configuration hadoopConf;

  // Built lazily on first call to buildMergeMap()
  private HoodieMergedLogRecordScanner logScanner;

  public HudiMORRecordReader(
      FileSlice fileSlice,
      Schema tableSchema,
      HoodieTableMetaClient metaClient,
      Configuration hadoopConf) {
    this.fileSlice   = fileSlice;
    this.tableSchema = tableSchema;
    this.metaClient  = metaClient;
    this.hadoopConf  = hadoopConf;
  }

  // -----------------------------------------------------------------------
  // File slice inspection
  // -----------------------------------------------------------------------

  /** Returns true if this file slice has any delta log files to merge. */
  public boolean hasLogData() {
    return fileSlice.getLogFiles().findAny().isPresent();
  }

  /** Returns the base Parquet file path, or empty for insert-only slices. */
  public java.util.Optional<String> getBaseFilePath() {
    Option<HoodieBaseFile> base = fileSlice.getBaseFile();
    return base.isPresent()
        ? java.util.Optional.of(base.get().getPath())
        : java.util.Optional.empty();
  }

  /** Returns the ordered list of log file paths for this file slice. */
  public List<String> getLogFilePaths() {
    return fileSlice.getLogFiles()
        .map(lf -> lf.getPath().toString())
        .collect(Collectors.toList());
  }

  // -----------------------------------------------------------------------
  // Merge map construction
  // -----------------------------------------------------------------------

  /**
   * Reads all log files in this file slice and builds a merge map keyed by
   * Hudi record key.
   *
   * Map semantics:
   *   key -> Option.of(record)   : record was updated; use the log version
   *   key -> Option.empty()      : record was deleted; skip it in output
   *
   * Records that appear ONLY in the log (not in the base file) are pure
   * inserts; retrieve them via {@link #getInsertOnlyRecords()} after calling
   * this method.
   *
   * @return Map of recordKey -> merged Avro record (or empty for deletes)
   * @throws IOException if log files cannot be read
   */
  public Map<String, Option<GenericRecord>> buildMergeMap() throws IOException {
    ensureLogScannerInitialized();

    Map<String, Option<GenericRecord>> mergeMap = new HashMap<>();

    for (String recordKey : logScanner.getRecords().keySet()) {
      HoodieRecord<?> logRecord = logScanner.getRecords().get(recordKey);

      try {
        // HoodieAvroPayload.getInsertValue() returns the merged Avro record,
        // or empty Option for tombstone (deleted) records.
        @SuppressWarnings("unchecked")
        org.apache.hudi.common.model.HoodieRecordPayload<?> payload =
            (org.apache.hudi.common.model.HoodieRecordPayload<?>) logRecord.getData();
        @SuppressWarnings("unchecked")
        Option<org.apache.avro.generic.IndexedRecord> indexedOpt =
            (Option<org.apache.avro.generic.IndexedRecord>) payload.getInsertValue(tableSchema);
        Option<GenericRecord> avroOpt = indexedOpt.isPresent()
            ? Option.of((GenericRecord) indexedOpt.get())
            : Option.empty();
        mergeMap.put(recordKey, avroOpt);
      } catch (Exception e) {
        logger.warn("Failed to deserialize log record for key {}. Skipping. Error: {}",
            recordKey, e.getMessage());
      }
    }

    logger.debug("Merge map built: {} records ({} deletes)",
        mergeMap.size(),
        mergeMap.values().stream().filter(v -> !v.isPresent()).count());

    return mergeMap;
  }

  /**
   * Returns all records that exist only in the log files (pure inserts —
   * no corresponding entry in the base Parquet file). Call after
   * {@link #buildMergeMap()} to get the complete picture.
   *
   * The caller is responsible for distinguishing inserts from updates;
   * a record key present in the merge map but NOT in the base file is an insert.
   */
  public List<GenericRecord> getInsertOnlyRecords(
      Map<String, Option<GenericRecord>> mergeMap,
      java.util.Set<String> baseFileRecordKeys) throws IOException {

    List<GenericRecord> insertOnly = new ArrayList<>();
    for (Map.Entry<String, Option<GenericRecord>> entry : mergeMap.entrySet()) {
      if (!baseFileRecordKeys.contains(entry.getKey()) && entry.getValue().isPresent()) {
        insertOnly.add(entry.getValue().get());
      }
    }
    return insertOnly;
  }

  // -----------------------------------------------------------------------
  // Simplified full-slice read (for testing / small tables)
  // -----------------------------------------------------------------------

  /**
   * Reads ALL records in the file slice as a list by merging log data on top
   * of the base file records. Intended for testing and small-scale reads.
   *
   * For production use, the merge should be applied lazily during Parquet
   * scanning via {@link #buildMergeMap()} to avoid materialising all records
   * in memory at once.
   *
   * @param baseFileRecords Records read from the base Parquet file, keyed by
   *                        the Hudi record key field value.
   * @param recordKeyField  Name of the field that holds the Hudi record key
   * @return Merged record list (updates and deletes applied)
   */
  public List<GenericRecord> merge(
      Map<String, GenericRecord> baseFileRecords,
      String recordKeyField) throws IOException {

    if (!hasLogData()) {
      return new ArrayList<>(baseFileRecords.values());
    }

    Map<String, Option<GenericRecord>> mergeMap = buildMergeMap();

    // Start with base records, then apply log delta
    Map<String, GenericRecord> merged = new HashMap<>(baseFileRecords);

    for (Map.Entry<String, Option<GenericRecord>> entry : mergeMap.entrySet()) {
      String key = entry.getKey();
      if (entry.getValue().isPresent()) {
        // Update: replace base record with log version
        merged.put(key, entry.getValue().get());
      } else {
        // Delete: remove from result
        merged.remove(key);
      }
    }

    return new ArrayList<>(merged.values());
  }

  // -----------------------------------------------------------------------
  // Cleanup
  // -----------------------------------------------------------------------

  @Override
  public void close() throws IOException {
    if (logScanner != null) {
      logScanner.close();
      logScanner = null;
    }
  }

  // -----------------------------------------------------------------------
  // Internal helpers
  // -----------------------------------------------------------------------

  private void ensureLogScannerInitialized() throws IOException {
    if (logScanner != null) return;

    List<String> logPaths = getLogFilePaths();
    if (logPaths.isEmpty()) {
      throw new IllegalStateException(
          "ensureLogScannerInitialized called but no log files in slice: " + fileSlice);
    }

    String latestInstant = metaClient.getCommitsAndCompactionTimeline()
        .filterCompletedInstants()
        .lastInstant()
        .map(org.apache.hudi.common.table.timeline.HoodieInstant::getTimestamp)
        .orElseThrow(() -> new IOException(
            "No completed instants on timeline for table at " + metaClient.getBasePath()));

    logger.debug("Initialising HoodieMergedLogRecordScanner for {} log files at instant {}",
        logPaths.size(), latestInstant);

    // HoodieMergedLogRecordScanner reads and merges all log files in the given list.
    // It applies Hudi's precombine logic (latest timestamp wins per record key)
    // and surfaces tombstones as empty Option payloads.
    logScanner = HoodieMergedLogRecordScanner.newBuilder()
        .withStorage(metaClient.getStorage())
        .withBasePath(metaClient.getBasePath())
        .withLogFilePaths(logPaths)
        .withReaderSchema(tableSchema)
        .withLatestInstantTime(latestInstant)
        .withReverseReader(false)
        .withBufferSize(
            // 16 MB read buffer per scanner; tune for memory vs. I/O trade-off
            16 * 1024 * 1024)
        .withPartition(fileSlice.getPartitionPath())
        .withRecordMerger(
            // HoodieAvroRecordMerger implements the precombine merge strategy
            new HoodieAvroRecordMerger())
        .withTableMetaClient(metaClient)
        .withForceFullScan(true)
        .build();
  }
}
