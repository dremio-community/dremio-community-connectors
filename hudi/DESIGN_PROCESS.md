# Dremio Hudi Connector — Design & Development Process

## Overview

This document captures the thought process, architectural reasoning, and implementation sequence that went into building the `dremio-hudi-connector` — a custom Dremio storage plugin that gives Dremio Community Edition full read and write support for Apache Hudi tables.

---

## 1. The Problem We Needed to Solve

Dremio's open-source edition has excellent native support for Delta Lake (via its own metadata layer) and a growing Iceberg integration. Apache Hudi, the third major open table format, has **zero native support** in Dremio CE. That's a meaningful gap — Hudi is widely deployed in real-time streaming lakehouse patterns, particularly where record-level upserts and deletes matter (CDC pipelines, fact table corrections, GDPR compliance).

The goal was simple: make it possible to write SQL like this against a Hudi table from inside Dremio, without any Spark jobs involved:

```sql
INSERT INTO hudi_source.orders SELECT * FROM staging.new_orders;

MERGE INTO hudi_source.customers t
USING updates u ON t.id = u.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
```

---

## 2. Understanding Dremio's Plugin Architecture

Before writing a single line of code, we had to understand how Dremio's extension points actually work. This required studying the `dremio-oss` source code on GitHub.

### Two-Layer Plugin Model

Dremio's storage layer is organized into two distinct plugin types:

```
StoragePlugin   → "Where is the data?" (S3, HDFS, NAS, databases)
FormatPlugin    → "What format is it?" (Parquet, JSON, Iceberg, Delta, CSV)
```

These are separate interfaces that compose together. A `StoragePlugin` manages the connection to a storage system; a `FormatPlugin` handles reading and writing a specific file format within that system.

### FileSystemPlugin as the Foundation

The critical discovery was `FileSystemPlugin` — Dremio's built-in base class for all filesystem-backed sources (S3, ADLS, HDFS, NAS). It already handles:

- Filesystem connections and credential management
- Directory listing and table discovery
- Schema mutability enforcement (which DML operations are allowed)
- Metadata refresh cycles
- Integration with Dremio's catalog

This meant we did **not** need to build a source plugin from scratch. We could extend `FileSystemPlugin` and inherit all of that infrastructure. The only custom work needed was the Hudi-specific layer on top.

### FormatPlugin's Role

`FormatPlugin` has two responsibilities in this system:

1. **Detection** — Auto-discovery is handled by `HudiFormatMatcher`, returned via `HudiFormatPlugin.getMatcher()`. During metadata refresh, Dremio calls `HudiFormatMatcher.matches()` on each directory; we return `true` when we find a `.hoodie/` subdirectory (Hudi's canonical table marker). This replaces the default `BasicFormatMatcher` (extension-based) and enables automatic catalog registration without manual promotion.

2. **Writing** — `getWriter()` returns a `RecordWriter` instance. This is where all the Hudi write logic lives.

---

## 3. Key Architectural Decisions

### Decision 1: Extend FileSystemPlugin (not build from scratch)

**Why:** Building a full `StoragePlugin` from scratch would require reimplementing filesystem connections, metadata refresh, catalog integration, and partition handling — thousands of lines of Dremio internals. By extending `FileSystemPlugin`, we inherited all of that and only had to implement the Hudi-specific parts.

**Trade-off:** We're tightly coupled to `FileSystemPlugin`'s API, which changes between Dremio versions. Method signatures (particularly `createNewTable()`) must exactly match the target version (26.0.5). This is a known risk that requires verification during the build step.

---

### Decision 2: Use HoodieJavaWriteClient, Not Spark

This was the most important design decision in the entire project.

**The problem:** Hudi's primary write API is Spark-based. Most Hudi examples, documentation, and client code assume Spark is running. But Dremio has its own distributed execution engine (Sabot) running on the JVM. Introducing Spark into a Dremio plugin would mean:
- Massive JAR size and classpath conflicts
- Two JVMs (Dremio's Sabot + Spark's executor) running in the same process
- Operational complexity that would make deployment impractical

**The solution:** Hudi provides `HoodieJavaWriteClient` — a pure-Java write client with `HoodieJavaEngineContext` that requires no Spark whatsoever. It is designed exactly for embedding Hudi writes inside non-Spark applications. This is the right tool for this job.

```java
HoodieJavaEngineContext engineContext =
    new HoodieJavaEngineContext(new HadoopStorageConfiguration(hadoopConf));

this.writeClient = new HoodieJavaWriteClient<>(engineContext, hudiWriteConfig);
```

**Trade-off:** `HoodieJavaWriteClient` has less community documentation than the Spark client. It also means compaction for MOR tables must be scheduled and executed separately (there's no Spark compaction service to lean on).

---

### Decision 3: INMEMORY Index for the Write Path

Hudi's write client requires an index to perform record-level upserts — it uses the index to determine which existing file a given record lives in, so it can write the update to the right place.

Available index types:
- **BLOOM** — fast, uses a bloom filter per file, stored in the Parquet footer
- **HBASE** — external HBase cluster required
- **SIMPLE** — slow full-scan index
- **INMEMORY** — in-process hash map, no external dependencies

**We chose INMEMORY for the initial implementation.** The reasons:
- No external infrastructure (no ZooKeeper, no HBase, no persistent metadata)
- Works on any filesystem (S3, HDFS, NAS)
- Zero configuration — it just works out of the box

**Honest trade-off:** INMEMORY index cannot de-duplicate across files that were written in previous sessions. This means it is reliable for `INSERT` but may not correctly de-duplicate for large-scale `UPSERT` workloads across many files. For upsert-heavy patterns at scale, the index should be changed to `BLOOM`, which stores filter metadata inside the Parquet files themselves.

---

### Decision 4: Arrow → Avro Conversion as a Separate Class

Dremio's execution engine (Sabot) is built entirely on Apache Arrow — all in-process data movement is Arrow columnar format. Hudi's Java client expects Apache Avro `GenericRecord` objects. These are two completely different data models.

The conversion layer (`ArrowToAvroConverter`) was designed with a specific constraint: **no Dremio imports**. This allowed it to be compiled and unit-tested independently, with only Arrow and Avro on the classpath.

```
Dremio internals  ──────────────────────  ArrowToAvroConverter  ──────────────────────  Hudi
(VectorAccessible, BatchSchema)           (Arrow Fields + ValueVectors)                 (GenericRecord, Schema)
```

The separation also means: if the Arrow or Avro versions change, this class can be updated and tested in isolation without needing a full Dremio environment.

The type mapping covers the full Arrow type system:

| Arrow Type | Avro Type | Notes |
|---|---|---|
| INT8/16/32 | INT | upcast |
| INT64 | LONG | |
| FLOAT | FLOAT | |
| DOUBLE | DOUBLE | |
| UTF8 / LargeUTF8 | STRING | `.toString()` from VarCharVector |
| BOOL | BOOLEAN | |
| BINARY | BYTES | wrapped in ByteBuffer |
| DATE32 | INT (date logical) | epoch days |
| TIMESTAMP | LONG (timestamp-micros logical) | epoch microseconds |
| DECIMAL | BYTES (decimal logical) | BigDecimal → unscaled bytes |
| LIST | ARRAY | recursive for element type |
| STRUCT | RECORD (nested) | recursive for children |

All fields are emitted as nullable unions `[null, type]` to support schema evolution.

---

### Decision 5: Single Atomic Commit Per Write Operation

A non-obvious design choice in `HudiRecordWriter` is the commit strategy:

```
writeBatch() called many times  →  intermediate flushes (no commit)
                                             ↓
close() called once             →  single atomic commit of ALL flushed data
```

Hudi's timeline model means each commit creates a new instant on the table's history. If we committed after every `writeBatch()` call, we would create many small instants, which hurts query planning and timeline compaction.

By buffering WriteStatus objects from every flush and committing them all at once in `close()`, the entire SQL statement (whether it processes 100 rows or 100 million) lands as exactly **one Hudi timeline instant**. This is the correct behavior for a transactional write.

The `autoCommit(false)` configuration on `HoodieWriteConfig` enables this pattern.

---

### Decision 6: Schema-Marker Columns for DML Type Detection

Dremio's planner communicates the DML type (INSERT, UPSERT, DELETE) through `WriterOptions`, but the exact API is version-dependent and changes between Dremio releases. Rather than create a version-specific coupling, we chose a portable approach:

- If the incoming schema contains a column named `_hoodie_is_deleted` (boolean), route rows where it's `true` to `writeClient.delete()`
- If the schema contains `_hoodie_upsert` (boolean), use `writeClient.upsert()` instead of `insert()`

This convention is compatible with Hudi's own conventions (Hudi's Spark DataSource uses `_hoodie_is_deleted` for deletes). It also means the detection logic works identically across Dremio versions without any reflection or API-version checking.

A version-specific `WriterOptions` API hook point is documented in the code for when a tighter integration is needed.

---

### Decision 7: COW Read Path Delegates to Dremio's Parquet Scanner

COPY_ON_WRITE Hudi tables store their data as standard Parquet files. Dremio already has an extremely well-optimized Parquet reader with predicate pushdown, column pruning, and Arrow-native output. Re-implementing Parquet reading inside the connector would be redundant and slower.

Instead, the read path:
1. Uses `HudiSnapshotUtils.getLatestBaseFilePaths()` to identify only the correct Parquet files (not stale historical ones) via Hudi's timeline
2. Returns those paths to Dremio, which reads them with its native Parquet scanner

The key insight here is that Hudi's timeline, not the raw filesystem listing, defines what files represent the current snapshot. Without reading the timeline, Dremio would incorrectly read old file versions or compaction artifacts.

---

## 4. Implementation Sequence

The project was built in distinct phases, each building on the last.

### Phase 1: Project Scaffold and Build System

Before any logic was written, the Maven project structure was established:
- `pom.xml` with correct dependency scopes (`provided` for Dremio + Arrow, bundled for Hudi + Avro)
- Maven Shade plugin configured to produce a fat JAR, with Avro relocated to avoid conflicts with Dremio's own Avro version
- Service loader registration (`META-INF/services/com.dremio.exec.store.dfs.FormatPlugin`) so Dremio discovers the plugin automatically

The dependency scope strategy was critical:
- `provided` scope for Dremio JARs and Arrow → they exist in Dremio's classpath at runtime; including them in the fat JAR would cause conflicts
- `compile` scope for Hudi and Avro → they are NOT in Dremio's classpath; we must bundle them

### Phase 2: Plugin Registration and Source UI

Next came the two classes needed for the plugin to appear in Dremio's "Add Source" UI:

- `HudiPluginConfig` — extends `FileSystemConf`, annotated with `@SourceType("HUDI")`. Each `@Tag`-annotated field becomes a form field in the Dremio UI. Protostuff serializes this and stores it in Dremio's catalog.
- `HudiFormatPluginConfig` — minimal config annotated with `@JsonTypeName("hudi")`, required for Dremio to deserialize the format plugin from stored source configs.
- `hudi-layout.json` — the JSON UI form definition that controls how the source config form renders.

`getSchemaMutability()` returning `SchemaMutability.USER_TABLE` is a small but critical detail — without this, Dremio refuses to allow any DML (INSERT, UPDATE, DELETE, CREATE TABLE) against the source.

### Phase 3: Format Detection and Auto-Discovery

Format detection is implemented via the `FormatMatcher` pattern — the correct hook for automatic table discovery in Dremio's plugin model.

`HudiFormatMatcher` extends Dremio's `FormatMatcher` abstract class and overrides `matches()`:

```java
public boolean matches(FileSystem fs, FileSelection fileSelection,
    CompressionCodecFactory codecFactory) throws IOException {
    String root = fileSelection.getSelectionRoot();
    Path hoodiePath = Path.of(root + "/" + HOODIE_DIR);
    return fs.exists(hoodiePath);
}
```

`HudiFormatPlugin` overrides `getMatcher()` to return a `HudiFormatMatcher` instance, replacing the default `BasicFormatMatcher` (which only does extension-based detection). This pattern is modeled after `DeltaLakeFormatMatcher` (which checks for `_delta_log/`) and `IcebergFormatMatcher` (which checks for `metadata/`).

The key implementation detail: `CompressionCodecFactory` in the method signature is **Dremio's own class** (`com.dremio.io.CompressionCodecFactory`), not Hadoop's (`org.apache.hadoop.io.compress.CompressionCodecFactory`). Using the wrong import causes a compile error.

The presence of a `.hoodie/` directory is the canonical indicator of a Hudi table regardless of COW/MOR type or table version. When Dremio's metadata refresh scans a directory and `HudiFormatMatcher.matches()` returns `true`, the directory is automatically registered as a Hudi dataset — no manual "Format Folder" step required.

`HudiTableDiscovery` complements this with a recursive Hadoop FS walker (up to configurable depth, default 5) for bulk discovery of all Hudi tables under a source root.

**End-to-end verified:** CTAS writes a table, immediate `SELECT` query resolves it automatically via `HudiFormatMatcher` — no promotion step needed.

### Phase 4: The Arrow → Avro Conversion Layer

`ArrowToAvroConverter` was built and tested in isolation before touching any Hudi write code. The schema conversion (`toAvroSchema`) and record conversion (`toGenericRecord` / `readValue`) were developed together with unit tests that verified each Arrow type maps correctly to its Avro equivalent.

Testing this in isolation (without Dremio JARs) was made possible by the deliberate design choice to keep all Dremio imports out of this class.

### Phase 5: The Write Path (HudiRecordWriter)

With the conversion layer proven, `HudiRecordWriter` was built to wire everything together:

1. `setup()` — reads the incoming Arrow schema, derives the Avro schema, detects operation mode, initializes `HoodieJavaWriteClient`, and starts a new commit instant
2. `writeBatch()` — iterates each row, converts Arrow → Avro via `ArrowToAvroConverter`, builds a `HoodieAvroRecord`, and buffers it; flushes to storage when the buffer is full
3. `close()` — flushes any remaining buffer, commits all pending `WriteStatus` objects atomically, reports stats back to Dremio's query profile

The rollback logic in `checkStatuses()` ensures that if any records fail during a write, the entire Hudi instant is rolled back — leaving the table in a clean state.

### Phase 6: The Read Path

The read path was built in two tiers:

**Tier 1 — Timeline-aware file listing (`HudiSnapshotUtils`)**

This utility class uses `HoodieTableMetaClient` and `HoodieTableFileSystemView` to read the Hudi timeline and return only the correct, current-version files:
- `getLatestBaseFilePaths()` — for COW tables, returns the latest base Parquet files
- `getLatestFileSlices()` — for MOR tables, returns (base file + log files) per file group

Without this, Dremio would see all historical Parquet files in the table directory and either fail or return incorrect results.

**Tier 2 — MOR log merging (`HudiMORRecordReader`)**

For MERGE_ON_READ tables, `HoodieMergedLogRecordScanner` reads all delta log files and builds a merge map: `recordKey → Option<GenericRecord>`. The map semantics are:
- Present with value → the record was updated; use the log version
- Present but empty → the record was deleted; skip it
- Absent → the record is unchanged; use the base Parquet version

`HudiMORRecordReader` wraps this scanner and exposes a clean API for the full-read and merge-apply patterns.

### Phase 7: Wiring It All Together (HudiFormatPlugin + HudiStoragePlugin)

The final assembly connected all the pieces:

- `HudiStoragePlugin` extends `FileSystemPlugin<HudiPluginConfig>`, primarily to expose `getHudiConfig()` so the format plugin can access write settings
- `HudiFormatPlugin` holds references to the `SabotContext` and exposes `getWriter()`, `getBaseFilePaths()`, `requiresMORMerge()`, and `buildMORMergeReader()` as the integration points for both read and write paths

---

## 5. Challenges and How They Were Resolved

### Challenge: Dremio's Internal API Is Not Public

Dremio does not publish a formal "plugin SDK." The interfaces we implement (`FileSystemPlugin`, `FormatPlugin`, `RecordWriter`) are internal APIs that change between versions. Understanding them required reading the `dremio-oss` GitHub source code at the specific version tag, not documentation.

**Resolution:** Pinned all Dremio versions to `26.0.5` in the POM. Added comments throughout the code flagging which method signatures are version-sensitive and how to verify them.

### Challenge: Dremio JARs Are Not on Maven Central

`dremio-sabot-kernel` and related artifacts are not published to Maven Central. The `pom.xml` references `https://maven.dremio.com/public/` as a fallback, but the most reliable approach is to build `dremio-oss` locally at the target version tag and install to the local `.m2` repository.

**Resolution:** The `pom.xml` documents both paths. The `test-only` Maven profile allows running unit tests for `ArrowToAvroConverter` without any Dremio JARs at all.

### Challenge: Avro Version Conflicts

Dremio ships with its own version of Apache Avro. Bundling a different Avro version in the fat JAR would cause `ClassCastException` at runtime when Hudi passes Avro objects across the boundary.

**Resolution:** The Maven Shade plugin is configured to relocate Hudi's Avro usage to `com.dremio.plugins.hudi.shaded.avro`, isolating it from Dremio's Avro. This is a standard shading pattern for fat JARs deployed into managed runtimes.

### Challenge: Full MOR Read Requires Dremio GroupScan Integration

Applying log-merged records back to a Parquet scan inside Dremio's distributed execution engine is non-trivial. It requires implementing `AbstractFileGroupScan`, `AbstractSubScan`, and a Dremio `RecordReader` — roughly three more classes with deep Dremio internal API dependencies.

**Resolution:** The COW read path (which covers most production Hudi deployments) was completed. The MOR read path was implemented at the utility layer (`HudiMORRecordReader`, `HudiSnapshotUtils`), and the GroupScan integration is documented as the next step with clear hook points and guidance in `HudiFormatPlugin.buildMORGroupScan()`.

---

## 6. What Was Deliberately Left Out (and Why)

**Bloom index for upserts at scale** — INMEMORY index is correct for the initial implementation. Bloom index requires validating that Hudi's bloom filter files are written correctly in the Parquet footers and that cross-session upserts de-duplicate properly. This is important for production upsert workloads but not for initial deployment.

**MOR compaction scheduling** — MOR tables accumulate log files between compactions. `HoodieJavaWriteClient.scheduleCompaction()` can schedule async compaction. This was omitted because it requires a background thread or a separate scheduled job, and the correct integration point inside Dremio's lifecycle wasn't clear without a running test environment.

**Hudi metadata table** — Hudi's internal metadata table improves file listing performance by caching the table's file system state. Enabling it adds complexity to the initial setup and requires careful coordination with the `HoodieTableFileSystemView`. Left disabled for now, with a one-line config change to enable it when needed.

**Dremio Cloud BYOC** — Cloud deployment was noted in the README but not specifically designed for. The plugin JAR should work in a BYOC executor configuration, but this has not been tested.

---

## 7. Verified Against Dremio 26.0.5

The following items have been end-to-end tested against `Dremio 26.0.5-202509091642240013-f5051a07`:

1. ✅ **Build with Dremio 26.0.5 JARs** — Compiles cleanly. `createNewTable()` signature verified against `FileSystemPlugin.java` at this version.

2. ✅ **`VectorWrapper` import** — Resolves correctly in `dremio-sabot-kernel-26.0.5` JAR.

3. ✅ **`outputEntryListener.recordsWritten()` signature** — Verified compatible. `HudiParquetRecordReader.allocate()` had a `throws OutOfMemoryException` clause removed (class not accessible in the classpath despite existing in Dremio's custom Arrow build).

4. ✅ **Deploy and smoke test** — Full end-to-end test completed in Docker container `try-dremio`:
   - CTAS to `hudi_local.output.ctas_test2` → 100 rows committed ✅
   - Auto-discovery via `HudiFormatMatcher` → immediate query after CTAS without promotion ✅
   - Cross-source CTAS (read Hudi, write Delta) → 20 rows verified ✅
   - SLF4J `StaticLoggerBinder` conflict resolved — excluded from fat JAR via shade filter ✅

**Additional known issue resolved:** Dremio's `StaticLoggerBinder` conflicts with Hudi's bundled SLF4J binding. Fixed by adding shade filter exclusions for `org/slf4j/impl/StaticLoggerBinder.class`, `StaticMDCBinder.class`, and `StaticMarkerBinder.class`, plus excluding `logback-classic` and `logback-core` artifacts entirely from the fat JAR.

---

## 8. Design Principles That Guided the Work

**Stay close to Dremio's own patterns.** We looked at how Dremio's Delta Lake and Parquet plugins work internally and followed the same structural patterns. This makes the plugin more likely to survive version upgrades and easier for any Dremio developer to understand.

**No Spark, ever.** The entire reason for building on `HoodieJavaWriteClient` instead of Hudi's Spark client is that Dremio is not Spark and should not be treated as such. Injecting Spark dependencies into Dremio would create operational nightmares.

**Isolate what can be isolated.** `ArrowToAvroConverter` has no Dremio imports, making it testable without a running Dremio instance. `HudiSnapshotUtils` has no Dremio imports either — only Hudi and Hadoop. These classes can be developed, tested, and maintained independently.

**Document the version-sensitive points clearly.** The places where Dremio's API is most likely to break between versions are flagged with explicit comments in the code and in this document. Anyone picking this up should know exactly where to look first.

**Write defensively.** Hudi write errors trigger a rollback before throwing, so the table is always left in a consistent state. Log messages at every lifecycle boundary (setup, each flush, commit, close) make it possible to diagnose issues without a debugger.
