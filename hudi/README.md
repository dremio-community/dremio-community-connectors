# Dremio Apache Hudi Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read and write support for Apache Hudi tables**.

Dremio 26.x has no native Hudi support. This connector bridges that gap by implementing
Dremio's internal `FileSystemPlugin` + `EasyFormatPlugin` interfaces and wiring them to
Hudi's `HoodieJavaWriteClient` (Spark-free Java write API) for writes and Hudi's
timeline + file system view APIs for snapshot-correct reads.

---

## What Works

| Feature | Status | Notes |
|---|---|---|
| **CTAS (write)** | ✅ Working | Commits data to Hudi with correct `.hoodie/` timeline |
| **SELECT (read)** | ✅ Working | No manual promotion required — auto-discovered via `HudiFormatMatcher` |
| **Auto-discovery** | ✅ Working | Tables with `.hoodie/` directories are detected automatically on query or metadata refresh |
| **Timeline-aware reads** | ✅ Working | Stale pre-compaction files are skipped automatically |
| **MOR log merging** | ✅ Working | Log files applied on top of base Parquet during scan |
| **COW tables** | ✅ Working | Reads base Parquet directly |
| **INSERT INTO** | ❌ Not supported | Dremio 26.x limitation — see below |

---

## Dremio 26.x Architectural Limitations

These are constraints in Dremio itself, not in the connector. They cannot be worked
around without changes to Dremio's core.

### 1. DML (INSERT INTO, MERGE INTO, UPDATE, DELETE) is Iceberg-only

In Dremio 26.x, `WriterCommitterOperator` uses an `IcebergCommitOpHelper` that only
registers catalog entries and enables DML for tables backed by Iceberg metadata.
Non-Iceberg tables (including Hudi) are skipped by this operator.

**Impact:**
- `CTAS` → writes data correctly; the table is queryable immediately via `HudiFormatMatcher` auto-discovery
- `INSERT INTO` on a Hudi table → fails with *"not configured to support DML operations"*

**Fix path:** Implement Hudi → Iceberg metadata sync in the connector so Dremio sees Hudi
tables as Iceberg tables. This is tracked as future work.

---

## Architecture

### Write Path

```
SQL: CREATE TABLE hudi_local.orders AS SELECT ...

Dremio Planner
  └── EasyWriter
        └── HudiFormatPlugin.getRecordWriter()
              └── HudiRecordWriter (per executor thread)
                    ├── setup()      → init HoodieJavaWriteClient (INMEMORY index)
                    ├── writeBatch() → Arrow vectors → Avro GenericRecord → HoodieRecord buffer
                    └── close()      → writeClient.insert() + commit Hudi timeline instant
```

### Read Path

```
SQL: SELECT * FROM hudi_local.orders

Dremio Planner
  └── EasyFormatPlugin scan
        └── HudiFormatPlugin.getRecordReader()
              └── HudiParquetRecordReader (per split / base file)
                    ├── isFileInLatestSnapshot()  → validate against Hudi timeline
                    │     └── HudiSnapshotUtils.getLatestBaseFilePaths()
                    ├── [MOR only] buildMergeMapIfMOR()
                    │     ├── HudiSnapshotUtils.getLatestFileSlices()  → find slice for this file
                    │     └── HudiMORRecordReader.buildMergeMap()     → read log files
                    └── next()
                          ├── read base Parquet via AvroParquetReader
                          ├── [MOR] apply merge map (updates replace, deletes drop)
                          └── [MOR] emit insert-only log records after base file exhausted
```

### Key Classes

| Class | Role |
|---|---|
| `HudiPluginConfig` | Source config shown in Dremio's "Add Source" UI. Extends `FileSystemConf`. |
| `HudiStoragePlugin` | Extends `FileSystemPlugin`. Filesystem access, catalog, table discovery. Exposes `discoverHudiTables()` for bulk registration. |
| `HudiFormatPlugin` | Factory for `HudiRecordWriter` (write) and `HudiParquetRecordReader` (read). Overrides `getMatcher()` to return `HudiFormatMatcher`. |
| `HudiFormatMatcher` | Detects `.hoodie/` directories during metadata refresh; auto-registers Hudi tables without manual promotion. |
| `HudiTableDiscovery` | Hadoop FS walker for bulk discovery of existing Hudi tables up to configurable depth (default: 5 levels). |
| `HudiRecordWriter` | Core write logic. Arrow → Avro → `HoodieJavaWriteClient` commits. |
| `ArrowToAvroConverter` | Translates Dremio Arrow schema + rows to Avro `GenericRecord` objects. |
| `HudiParquetRecordReader` | Reads a base Parquet file; applies MOR merge map if log files are present. Batch size configurable via `readBatchSize`. |
| `HudiSnapshotUtils` | Reads Hudi timeline + file system view to determine active files and slices. 60-second TTL cache shared across all splits of a scan; invalidated after every write. |
| `HudiMORRecordReader` | Reads MOR log files via `HoodieMergedLogRecordScanner`; builds merge map. |

---

## SQL Usage

```sql
-- Create a new Hudi table from an existing source (COW by default)
CREATE TABLE hudi_local.orders
STORE AS (type => 'hudi')
AS SELECT order_id, customer_id, amount, created_at
FROM staging.raw_orders;

-- Query immediately after CTAS — no manual promotion required.
-- HudiFormatMatcher detects the .hoodie/ directory automatically.
SELECT * FROM hudi_local.orders LIMIT 100;

SELECT customer_id, SUM(amount) AS total
FROM hudi_local.orders
GROUP BY customer_id
ORDER BY total DESC;
```

### What does NOT work (Dremio 26.x)

```sql
-- ❌ INSERT INTO an existing Hudi table — fails with DML not supported
INSERT INTO hudi_local.orders SELECT * FROM staging.new_orders;

-- ❌ MERGE INTO — same reason (Iceberg-only DML in Dremio 26.x)
MERGE INTO hudi_local.orders t USING updates u ON t.order_id = u.order_id ...;
```

---

## MOR Table Notes

For MERGE_ON_READ tables, the connector handles log merging automatically:

- **Updates**: the log version of a record replaces the base file version
- **Deletes**: tombstone records in the log cause the base record to be dropped
- **Insert-only log records**: records that exist only in the log (no base file entry) are emitted after the base file is exhausted

The `defaultRecordKeyField` configured in the source settings is used as the merge key.
Ensure it matches the actual unique key column in your data.

For tables with many unapplied log files, the connector automatically schedules Hudi
compaction (MOR) or clustering (COW) every N commits (configurable via
`Auto-Compaction/Clustering Interval` in the source settings, default: every 10 commits).
For tables written externally, you can also trigger compaction manually via Hudi CLI.

The number of rows returned per Arrow batch during SELECT is configurable via
`Read Batch Size` in source settings (default: 4096, matching Dremio's standard batch size).

---

## Building

The connector is built **inside the Docker container** by `install.sh`. Maven is not
required on the host machine. See [INSTALL.md](INSTALL.md) for full instructions.

```bash
# From the dremio-hudi-connector/ directory:
./install.sh
```

The script builds and deploys in one pass and restarts Dremio automatically.

---

## Hudi Version Compatibility

This connector targets **Hudi 0.15.0** (the `hudi-java-client` artifact).
Hudi's internal APIs changed significantly between 0.14.x and 0.15.0:

| API | 0.14.x | 0.15.0 |
|---|---|---|
| `HoodieTableMetaClient.setConf()` | `Configuration` | `HadoopStorageConfiguration` |
| `FSUtils.getAllPartitionPaths()` | 4 params | 5 params (`String` basePath, two new booleans) |
| `FileSystemViewManager.createInMemoryFileSystemView()` | `FileSystemViewStorageConfig` | `HoodieMetadataConfig` |
| `HoodieLogFile.getPath()` | `org.apache.hadoop.fs.Path` | `org.apache.hudi.storage.StoragePath` |
| `HoodieRecordPayload.getInsertValue()` | `Option<GenericRecord>` | `Option<IndexedRecord>` |

All of these are handled correctly in the current implementation.

---

## Key Design Decisions

**Why `HoodieJavaWriteClient` and not Spark?**
Dremio runs on its own JVM with the Sabot execution engine. Introducing Spark would add
classpath conflicts and operational complexity. `HoodieJavaWriteClient` with
`HoodieJavaEngineContext` is the correct approach for embedding Hudi writes inside a
non-Spark process.

**Why INMEMORY index?**
INMEMORY index requires no external state (no ZooKeeper, HBase, or persistent files).
It works on any filesystem. The trade-off is it cannot de-duplicate across existing files,
making it suitable for INSERT-heavy (CTAS) patterns. For upsert-heavy workloads, switch
to BLOOM index.

**Why extend `FileSystemPlugin`?**
`FileSystemPlugin` handles S3/ADLS/HDFS/NAS connections, metadata refresh, directory
listing, and `SchemaMutability` enforcement. Building on it means we inherit all
filesystem support and only implement Hudi-specific format detection and write/read paths.

**Why does INSERT INTO not work?**
Dremio 26.x's `WriterCommitterOperator` uses `IcebergCommitOpHelper` which only completes
catalog registration for Iceberg-backed tables. After a non-Iceberg CTAS, the table exists
on disk but Dremio's catalog has no entry for it. When a manually promoted dataset receives
an INSERT INTO, Dremio's DML planner rejects it because the promoted dataset is a
`PHYSICAL_DATASET` (read-only) not an Iceberg table. The fix is to implement Hudi → Iceberg
metadata sync so Dremio treats Hudi tables as Iceberg tables.

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded to a new version, run `rebuild.sh` to automatically detect the
new version, update `pom.xml`, rebuild the JAR, and redeploy — all in one command:

```bash
./rebuild.sh                                    # Docker (default: try-dremio)
./rebuild.sh --docker my-dremio                 # Named Docker container
./rebuild.sh --local /opt/dremio                # Bare-metal
./rebuild.sh --k8s dremio-master-0              # Kubernetes
./rebuild.sh --k8s dremio-master-0 -n dremio-ns # Kubernetes with namespace
./rebuild.sh --dry-run                          # Preview only, no changes
```

See [INSTALL.md](INSTALL.md#keeping-up-with-dremio-upgrades) for full details.

---

## Future Work

1. **Hudi → Iceberg metadata sync** — expose Hudi tables as Iceberg tables to Dremio so
   that `INSERT INTO`, `MERGE INTO`, and CTAS catalog auto-registration work natively
2. **Bloom index for upserts at scale** — replace INMEMORY with BLOOM for large tables
   where duplicate detection across existing files is required

---

## References

### Dremio Internals
- [FileSystemPlugin.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/dfs/FileSystemPlugin.java)
- [EasyFormatPlugin.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/dfs/easy/EasyFormatPlugin.java)
- [RecordWriter.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/RecordWriter.java)
- [WriterOptions.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/physical/base/WriterOptions.java)

### Apache Hudi
- [HoodieJavaWriteClient Example](https://github.com/apache/hudi/blob/master/hudi-examples/hudi-examples-java/src/main/java/org/apache/hudi/examples/java/HoodieJavaWriteClientExample.java)
- [Hudi Configurations Reference](https://hudi.apache.org/docs/configurations/)
- [Hudi File Layouts](https://hudi.apache.org/docs/file_layouts/)
- [HoodieMergedLogRecordScanner](https://github.com/apache/hudi/blob/master/hudi-common/src/main/java/org/apache/hudi/common/table/log/HoodieMergedLogRecordScanner.java)
