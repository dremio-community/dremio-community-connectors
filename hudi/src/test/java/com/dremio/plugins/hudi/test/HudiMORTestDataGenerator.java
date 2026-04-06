package com.dremio.plugins.hudi.test;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.client.HoodieJavaWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.common.HoodieJavaEngineContext;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodieAvroRecord;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieIndexConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Standalone test data generator for the MOR log-only file group fix.
 *
 * Creates an Apache Hudi MERGE_ON_READ table where ALL records are stored in
 * log files with NO base Parquet files (the log-only file group scenario).
 * This happens naturally when HoodieJavaWriteClient is used with a MOR table
 * before any compaction has run — each insert creates a new log file group.
 *
 * Purpose
 * -------
 * Tests the fix in HudiParquetRecordReader that makes log-only file groups
 * visible to Dremio queries. Before the fix, SELECT COUNT(*) on this table
 * returned 0. After the fix it returns the expected row count.
 *
 * Usage (from dremio-hudi-connector/ directory)
 * -----------------------------------------------
 *   mvn test-compile \
 *     exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=com.dremio.plugins.hudi.test.HudiMORTestDataGenerator
 *
 * With a custom output path:
 *   mvn test-compile exec:java \
 *     -Dexec.classpathScope=test \
 *     -Dexec.mainClass=com.dremio.plugins.hudi.test.HudiMORTestDataGenerator \
 *     "-Dexec.args=/path/to/output/dir"
 *
 * No Dremio JARs required — uses only Hudi + Avro + Hadoop (all bundled).
 */
public class HudiMORTestDataGenerator {

  /** Simple 5-field schema. "id" is the record key, "ts" is the precombine field. */
  private static final String SCHEMA_STR = "{"
      + "\"type\":\"record\","
      + "\"name\":\"MORTestRecord\","
      + "\"namespace\":\"com.dremio.plugins.hudi.test\","
      + "\"fields\":["
      + "  {\"name\":\"id\",     \"type\":\"int\"},"
      + "  {\"name\":\"name\",   \"type\":\"string\"},"
      + "  {\"name\":\"region\", \"type\":\"string\"},"
      + "  {\"name\":\"amount\", \"type\":\"double\"},"
      + "  {\"name\":\"ts\",     \"type\":\"long\"}"
      + "]}";

  private static final String TABLE_NAME    = "mor_log_only_test";
  private static final String RECORD_KEY    = "id";
  private static final String PRECOMBINE    = "ts";
  private static final int    BATCH_SIZE    = 30;
  private static final int    NUM_BATCHES   = 3;
  private static final int    TOTAL_RECORDS = BATCH_SIZE * NUM_BATCHES;

  private static final String[] REGIONS = {"us-east", "us-west", "eu-west", "ap-south"};

  public static void main(String[] args) throws Exception {
    String outputPath = (args != null && args.length > 0 && !args[0].isBlank())
        ? args[0]
        : Paths.get(System.getProperty("user.dir"),
                    "..", "dremio-connectors-dist", "test-env", "data", "hudi", TABLE_NAME)
               .normalize().toString();

    System.out.println();
    System.out.println("  ╔════════════════════════════════════════════════════╗");
    System.out.println("  ║   Hudi MOR Log-Only Test Data Generator            ║");
    System.out.println("  ╚════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println("  Table type  : MERGE_ON_READ (log-only file groups)");
    System.out.println("  Record key  : " + RECORD_KEY);
    System.out.println("  Batches     : " + NUM_BATCHES + " × " + BATCH_SIZE + " records = "
        + TOTAL_RECORDS + " total");
    System.out.println("  Output      : " + outputPath);
    System.out.println();

    // Clean up any previous run so the table starts fresh
    Path outDir = Paths.get(outputPath);
    if (Files.exists(outDir)) {
      System.out.println("  → Removing existing directory...");
      deleteRecursively(outDir);
    }
    Files.createDirectories(outDir);

    Configuration hadoopConf = new Configuration();
    Schema schema = new Schema.Parser().parse(SCHEMA_STR);

    // Initialize the Hudi table on disk (.hoodie/ directory + metadata)
    HoodieTableMetaClient.withPropertyBuilder()
        .setTableType(HoodieTableType.MERGE_ON_READ.name())
        .setTableName(TABLE_NAME)
        .setPayloadClass(HoodieAvroPayload.class)
        .setRecordKeyFields(RECORD_KEY)
        .setPartitionFields("")
        .setPreCombineField(PRECOMBINE)
        .initTable(new HadoopStorageConfiguration(hadoopConf), outputPath);

    HoodieWriteConfig writeConfig = HoodieWriteConfig.newBuilder()
        .withPath(outputPath)
        .withSchema(SCHEMA_STR)
        .withParallelism(1, 1)
        .withDeleteParallelism(1)
        // autoCommit=false: we commit explicitly after each insert so we can
        // accumulate WriteStatus objects and commit them in one call.
        .withAutoCommit(false)
        .withIndexConfig(HoodieIndexConfig.newBuilder()
            .withIndexType(HoodieIndex.IndexType.INMEMORY)
            .build())
        .build();

    HoodieJavaEngineContext engineContext =
        new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf));

    try (HoodieJavaWriteClient<HoodieAvroPayload> client =
             new HoodieJavaWriteClient<>(engineContext, writeConfig)) {

      for (int batch = 0; batch < NUM_BATCHES; batch++) {
        int startId = batch * BATCH_SIZE + 1;
        int endId   = startId + BATCH_SIZE - 1;

        String instantTime = client.startCommit();
        List<HoodieRecord<HoodieAvroPayload>> records = makeRecords(schema, startId, BATCH_SIZE);
        List<WriteStatus> statuses = client.insert(records, instantTime);
        client.commit(instantTime, statuses);

        long errors = statuses.stream().filter(WriteStatus::hasErrors).count();
        System.out.printf("  ✓ Batch %d: IDs %d–%d committed at %s%s%n",
            batch + 1, startId, endId, instantTime,
            errors > 0 ? "  ← " + errors + " write error(s)!" : "");
      }
    }

    // Verify: count log files and confirm no base Parquet was created
    long logFiles = countFiles(outDir, ".log");
    long parquetFiles = countFiles(outDir, ".parquet");

    System.out.println();
    System.out.println("  File system check:");
    System.out.printf("    Log files    : %d  (these hold your records)%n", logFiles);
    System.out.printf("    Parquet files: %d  (expected 0 — no compaction run)%n", parquetFiles);

    if (parquetFiles > 0) {
      System.out.println("  ⚠ Unexpected base Parquet files found. The test may not"
          + " exercise the log-only path correctly.");
    }

    System.out.println();
    System.out.println("  ┌─────────────────────────────────────────────────────┐");
    System.out.printf( "  │  Expected query result: COUNT(*) = %-4d             │%n", TOTAL_RECORDS);
    System.out.println("  └─────────────────────────────────────────────────────┘");
    System.out.println();
    System.out.println("  Next steps:");
    System.out.println();
    System.out.println("  1. Copy to Dremio container:");
    System.out.printf( "       docker cp \"%s\" try-dremio:/test-data/hudi/%s%n",
        outputPath, TABLE_NAME);
    System.out.println();
    System.out.println("  2. Query in Dremio SQL Runner:");
    System.out.printf( "       SELECT COUNT(*) FROM hudi_local.\"%s\";%n", TABLE_NAME);
    System.out.printf( "       -- expected: %d%n", TOTAL_RECORDS);
    System.out.println();
    System.out.println("  3. Spot-check a few rows:");
    System.out.printf( "       SELECT * FROM hudi_local.\"%s\" LIMIT 10;%n", TABLE_NAME);
    System.out.println();
  }

  // -----------------------------------------------------------------------

  private static List<HoodieRecord<HoodieAvroPayload>> makeRecords(
      Schema schema, int startId, int count) {

    List<HoodieRecord<HoodieAvroPayload>> records = new ArrayList<>(count);
    long now = System.currentTimeMillis();

    for (int i = 0; i < count; i++) {
      int id = startId + i;

      GenericRecord avro = new GenericData.Record(schema);
      avro.put("id",     id);
      avro.put("name",   "record_" + String.format("%04d", id));
      avro.put("region", REGIONS[id % REGIONS.length]);
      avro.put("amount", Math.round(id * 10.5 * 100.0) / 100.0);
      avro.put("ts",     now + i);  // monotonically increasing for precombine

      HoodieKey hoodieKey = new HoodieKey(String.valueOf(id), "");
      records.add(new HoodieAvroRecord<>(
          hoodieKey,
          new HoodieAvroPayload(Option.of(avro))));
    }
    return records;
  }

  private static long countFiles(Path dir, String extension) throws Exception {
    if (!Files.exists(dir)) return 0;
    try (var stream = Files.walk(dir)) {
      return stream
          .filter(Files::isRegularFile)
          .filter(p -> p.toString().contains(extension))
          .count();
    }
  }

  private static void deleteRecursively(Path path) throws Exception {
    if (!Files.exists(path)) return;
    try (var stream = Files.walk(path)) {
      stream.sorted(java.util.Comparator.reverseOrder())
            .map(Path::toFile)
            .forEach(java.io.File::delete);
    }
  }
}
