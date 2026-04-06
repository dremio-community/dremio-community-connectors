# Dremio Delta Lake Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read and write support for Delta Lake tables**.

Dremio has native read support for Delta Lake in recent versions, but no write
path. This connector adds full CTAS write support using the Delta `delta-standalone`
Java library (no Spark required) and `parquet-avro` for Parquet file writing.

> **Dremio 26.x DML note:** `INSERT INTO`, `DELETE FROM`, and `MERGE INTO` are
> Iceberg-only in Dremio 26.x and are not supported on Delta tables. **CTAS is the
> supported write path.** Tables are auto-discovered after CTAS — no manual
> promotion required. See [Dremio 26.x Architectural Constraints](#dremio-26x-architectural-constraints).

---

## Architecture

```
SQL:  CREATE TABLE delta_source.orders STORE AS (type => 'delta_write') AS SELECT ...

Dremio Planner
  └── WriterCommitterPrel
        └── DeltaStoragePlugin.createNewTable()
              └── DeltaFormatPlugin.getWriter()
                    └── DeltaRecordWriter (per executor thread)
                          ├── setup()      -> init AvroParquetWriter + detect op mode
                          ├── writeBatch() -> Arrow -> Avro -> Parquet rows
                          └── close()      -> flush Parquet file + DeltaLog.commit(AddFile)
```

### Key Classes

| Class | Role |
|---|---|
| `DeltaPluginConfig` | Source config shown in Dremio's "Add Source" UI. Extends `FileSystemConf`. Includes `readBatchSize` (Tag 8) and `metadataCacheTtlSeconds` (Tag 9). |
| `DeltaStoragePlugin` | Extends `FileSystemPlugin`. Handles filesystem, catalog, table discovery. |
| `DeltaFormatPlugin` | Format detection (`_delta_log` dir check) + factory for `DeltaRecordWriter`. |
| `DeltaFormatPluginConfig` | Format-level config (stats collection, indexed column count). |
| `DeltaRecordWriter` | Core write logic: Arrow -> Parquet via Avro + Delta log commit. |
| `ArrowToAvroConverter` | Translates Dremio's Arrow schema + rows to Avro `GenericRecord` objects. |
| `ArrowToDeltaSchemaConverter` | Translates Arrow `BatchSchema` to Delta `StructType` for Metadata actions. |
| `DeltaSnapshotUtils` | Reads Delta transaction log to identify active Parquet files. Caches active file list per table with configurable TTL + version-check invalidation. `invalidateCache()` called after every write. |

### Key Difference from Hudi Connector

| Aspect | Hudi Connector | Delta Connector |
|---|---|---|
| Write client | `HoodieJavaWriteClient` handles everything | We write Parquet ourselves + commit via `DeltaLog` |
| Parquet writing | Hudi handles internally | `AvroParquetWriter` (Arrow -> Avro -> Parquet) |
| Log format | Avro timeline with instants | JSON commits in `_delta_log/` |
| Deletes | `writeClient.delete(keys)` | File rewrite (RemoveFile + AddFile) — see TODO |
| Upsert | `writeClient.upsert()` with index lookup | File rewrite with merge — see TODO |
| Read complexity | MOR requires log merging | All files are self-contained Parquet |

---

## What's Implemented

| Feature | Status | Notes |
|---|---|---|
| **CTAS (write)** | ✅ Working | `STORE AS (type => 'delta_write')` — commits data with correct `_delta_log/` |
| **SELECT (read)** | ✅ Working | No manual promotion required — auto-discovered via `DeltaFormatMatcher` |
| **Auto-discovery** | ✅ Working | Tables with `_delta_log/` directories are detected automatically on metadata refresh |
| **Snapshot-consistent reads** | ✅ Working | Stale/REMOVEd Parquet files are skipped; only active snapshot files are read |
| **Partition-aware writes** | ✅ Working | Output Parquet files use correct `col=val/` directory structure |
| **Column statistics** | ✅ Working | `numRecords`, `minValues`, `maxValues`, `nullCount` written to Delta log |
| **Large file splits** | ✅ Working | Rolls to a new Parquet file when target size threshold is reached |
| **Schema evolution** | ✅ Working | Detects new columns and updates Delta Metadata if enabled |
| **Snapshot caching** | ✅ Working | Active file list cached with TTL + version check; writes always invalidate immediately |
| **Configurable batch size** | ✅ Working | `readBatchSize` controls Arrow rows per batch (default 4096) |
| **INSERT INTO** | ❌ Not supported | Dremio 26.x limitation — see below |
| **DELETE FROM / MERGE INTO** | ❌ Not supported | Dremio 26.x DML is Iceberg-only |

## Dremio 26.x Architectural Constraints

These are constraints in Dremio itself, not in the connector.

### DML (INSERT INTO, DELETE FROM, MERGE INTO) is Iceberg-only

In Dremio 26.x, `WriterCommitterOperator` uses an `IcebergCommitOpHelper` that only
registers catalog entries and enables DML for Iceberg-backed tables. Non-Iceberg tables
(including Delta Lake) are bypassed.

**Impact:** `CTAS` is the only supported write path. `INSERT INTO`, `DELETE FROM`, and
`MERGE INTO` will fail with *"not configured to support DML operations"*.

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

1. **DML support via Iceberg metadata sync** — expose Delta tables as Iceberg tables to
   Dremio so that `INSERT INTO` and `MERGE INTO` work natively
2. **Deletion Vectors (Delta 2.x+)** — bitmaps for row-level deletes without full file rewrites; requires `delta-kernel` library
3. **Custom GroupScan for partition pruning** — implement `DeltaGroupScan` + `DeltaSubScan` for partition-level file skipping during reads
4. **Change Data Feed** — `DeltaSnapshotUtils.getAddedFilesSince()` already supports incremental reads; needs surface-level wiring

## Performance Configuration

| Setting | Default | Notes |
|---|---|---|
| `Read Batch Size` (`readBatchSize`) | 4096 | Rows per Arrow batch during SELECT. Increase for large analytical queries; decrease for memory-constrained environments. |
| `Snapshot Cache TTL` (`metadataCacheTtlSeconds`) | 60 | Seconds to cache the active file list per Delta table. `0` disables caching. Connector writes always invalidate the cache immediately. A version check (`fs.exists(_delta_log/{v+1}.json)`) catches external writes before TTL expires. |

---

## Building

```bash
# Build the plugin fat JAR (bundles delta-standalone + parquet-avro)
mvn clean package -DskipTests

# Run unit tests
mvn test

# The deployable JAR is:
target/dremio-delta-connector-1.0.0-SNAPSHOT-plugin.jar
```

## Deployment (Dremio Software)

1. Copy the fat JAR to **all** Dremio nodes:
   ```
   $DREMIO_HOME/jars/3rdparty/dremio-delta-connector-1.0.0-SNAPSHOT-plugin.jar
   ```

2. Restart Dremio:
   ```bash
   $DREMIO_HOME/bin/dremio restart
   ```

3. In Dremio UI: **Sources → Add Source → Delta Lake**

4. Set the root path and write settings, then Save.

## Deployment (Dremio Cloud / BYOC)

Upload the fat JAR to the engine node's plugin directory as documented in the
Dremio Cloud BYOC configuration guide. Restart the engine pod after deployment.

---

## SQL Usage (once deployed)

> **Format type is `delta_write`, not `delta`.**
> Dremio has a built-in read-only `DeltaLakeFormatPlugin` registered under the name
> `delta` whose `getWriter()` returns null. Using `STORE AS (type => 'delta')` would
> silently route through that plugin and fail. Our write plugin is registered as
> `delta_write` to avoid this conflict.

```sql
-- Create a new Delta table via CTAS
-- MUST use STORE AS (type => 'delta_write') — NOT 'delta'
CREATE TABLE delta_source.my_table
STORE AS (type => 'delta_write')
AS SELECT id, name, region, amount
FROM staging.raw_data;

-- After CTAS: DeltaFormatMatcher auto-discovers the table via _delta_log/.
-- No manual promotion required. Query immediately:
SELECT COUNT(*) FROM delta_source.my_table;
SELECT * FROM delta_source.my_table LIMIT 100;

-- Discover Delta tables at the source root
SELECT * FROM INFORMATION_SCHEMA."TABLES"
WHERE TABLE_SCHEMA = 'delta_source';
```

### What does NOT work (Dremio 26.x architectural constraints)

```sql
-- ❌ STORE AS (type => 'delta') — routes to Dremio's built-in read-only plugin → NPE
CREATE TABLE delta_source.t STORE AS (type => 'delta') AS SELECT ...;

-- ❌ INSERT INTO — Dremio 26.x DML is Iceberg-only
INSERT INTO delta_source.my_table SELECT * FROM staging.new_rows;
```

---

## Key Design Decisions

**Why `delta-standalone` and not `delta-kernel`?**
`delta-standalone` has a stable, well-documented write API (`OptimisticTransaction`,
`AddFile`, `RemoveFile`, `Metadata`). `delta-kernel` (3.x) is newer and Java-first
but its write API is still evolving. For a production connector today,
`delta-standalone` is the right choice. Migrating to `delta-kernel` in future
is straightforward since the log format is identical.

**Why Arrow -> Avro -> Parquet and not Arrow -> Parquet directly?**
`parquet-avro` (`AvroParquetWriter`) is a stable, well-tested bridge between
Avro GenericRecords and Parquet column format. It reuses the `ArrowToAvroConverter`
already implemented in the Hudi connector. A direct Arrow -> Parquet path would
use `parquet-arrow` which adds another dependency and a different API surface.
The Avro intermediary adds minimal overhead compared to actual I/O.

**Why extend `FileSystemPlugin`?**
`FileSystemPlugin` provides all filesystem plumbing (S3, ADLS, HDFS, NAS),
metadata refresh, and `SchemaMutability` enforcement. We inherit all of this
and only implement the Delta-specific format detection and write path.

---

## References

### Dremio Plugin Internals (the interfaces this connector implements)

These are the authoritative sources for understanding the extension points used in this connector.
Always check these at the **exact version tag** matching your deployed Dremio instance.

- [FileSystemPlugin.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/dfs/FileSystemPlugin.java) — base class we extend; handles filesystem connections, metadata refresh, catalog integration
- [FormatPlugin.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/dfs/FormatPlugin.java) — interface implemented by `DeltaFormatPlugin`; governs format detection and write factory
- [RecordWriter.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/RecordWriter.java) — interface implemented by `DeltaRecordWriter`; defines `setup()`, `writeBatch()`, `close()` lifecycle
- [FileSystemConf.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/dfs/FileSystemConf.java) — base class for `DeltaPluginConfig`; drives UI form registration and source serialization
- [WriterOptions.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/physical/base/WriterOptions.java) — passed to `getWriter()`; carries schema, partition info, and DML type hints
- [SchemaMutability.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/dfs/SchemaMutability.java) — enum controlling which DML operations are allowed on a source
- [IcebergFormatPlugin.java](https://github.com/dremio/dremio-oss/blob/master/plugins/iceberg/src/main/java/com/dremio/plugins/iceberg/IcebergFormatPlugin.java) — best reference implementation for GroupScan + SubScan + RecordReader pattern (follow this for partition pruning)
- [Dremio Delta Lake native support docs](https://docs.dremio.com/current/data-formats/delta-lake/) — documents what Dremio CE already does natively for Delta reads; informs which read-path work is additive vs. redundant

### Note on ARP (Advanced Relational Pushdown)

The Dremio [ARP Connector Framework](https://docs.dremio.com/current/developer/arp-connector/) is **not applicable** to this connector. ARP is designed exclusively for JDBC-based data sources that accept SQL as a query language (e.g. MySQL, PostgreSQL, SAP HANA). Delta Lake is a filesystem-based open table format — it has no JDBC driver and does not accept SQL. The correct extension points are `FileSystemPlugin` + `FormatPlugin`, as used here.

### Delta Lake

- [delta-standalone GitHub](https://github.com/delta-io/delta/tree/master/connectors/standalone) — the Java library used for transaction log reads and commits (no Spark required)
- [Delta Lake Transaction Log Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md) — defines AddFile, RemoveFile, Metadata, CommitInfo actions; essential reading for the write path
- [Delta Lake Table Properties](https://docs.delta.io/latest/table-properties.html) — configurable table behaviors including schema evolution, Change Data Feed, and Deletion Vectors

### Apache Parquet

- [AvroParquetWriter Javadoc](https://www.javadoc.io/doc/org.apache.parquet/parquet-avro/latest/org/apache/parquet/avro/AvroParquetWriter.html) — the writer used in `DeltaRecordWriter` to produce Parquet output from Avro GenericRecords
- [Apache Parquet File Format](https://parquet.apache.org/docs/file-format/) — column encoding, row groups, and statistics (relevant for `buildFileStats()`)

### Apache Arrow

- [Arrow Java ValueVector](https://arrow.apache.org/docs/java/vector.html) — reference for reading typed values from Arrow vectors in `ArrowToAvroConverter`
- [Arrow Java Type System](https://arrow.apache.org/docs/java/datatype.html) — Arrow type IDs and their Java representations
