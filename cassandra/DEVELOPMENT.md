# Dremio Apache Cassandra Connector — Development Walkthrough

This document is a complete narrative of how the connector was designed and built,
from the first line of code to the working, tested, distributable plugin. It covers
every major decision, every interface that had to be implemented, every bug hit along
the way, and how each problem was resolved.

---

## Table of Contents

1. [Goal and Constraints](#1-goal-and-constraints)
2. [Understanding Dremio's Plugin Architecture](#2-understanding-dremios-plugin-architecture)
3. [Project Skeleton and Maven Setup](#3-project-skeleton-and-maven-setup)
4. [Step 1 — Source Configuration (`CassandraStoragePluginConfig`)](#4-step-1--source-configuration)
5. [Step 2 — The Plugin Lifecycle (`CassandraStoragePlugin`)](#5-step-2--the-plugin-lifecycle)
6. [Step 3 — Connecting to Cassandra (`CassandraConnection`)](#6-step-3--connecting-to-cassandra)
7. [Step 4 — Type Mapping (`CassandraTypeConverter`)](#7-step-4--type-mapping)
8. [Step 5 — Dataset Discovery and Metadata](#8-step-5--dataset-discovery-and-metadata)
9. [Step 6 — Planning Layer](#9-step-6--planning-layer)
10. [Step 7 — Execution Layer](#10-step-7--execution-layer)
11. [Step 8 — Predicate Pushdown](#11-step-8--predicate-pushdown)
12. [Step 9 — Parallel Token-Range Splits](#12-step-9--parallel-token-range-splits)
13. [Step 10 — Auto-Reconnect](#13-step-10--auto-reconnect)
14. [Bugs Encountered and Fixed](#14-bugs-encountered-and-fixed)
15. [Build and Packaging](#15-build-and-packaging)
16. [Testing](#16-testing) — unit test suite + live integration tests
17. [The Installer](#17-the-installer)
18. [Final File Structure](#18-final-file-structure)
19. [Post-Release Enhancements](#19-post-release-enhancements)

---

## 1. Goal and Constraints

**Goal:** Add native read support for Apache Cassandra tables to Dremio 26.x — no
JDBC, no Spark. Users should be able to add a Cassandra source in the Dremio UI,
browse all tables automatically, run SQL SELECTs, and join Cassandra tables with
any other data source registered in Dremio (Hudi, Delta Lake, S3/Parquet, etc.).

**Hard constraints:**

- Must work with Dremio 26.0.5 (exact JAR versions matter; build-time dependencies
  must be installed from the running Dremio container because they are not on Maven Central).
- Use DataStax Java Driver 4.x via the CQL native binary protocol (port 9042).
- Read-only. No INSERT, no CTAS, no UPDATE.
- The connector JAR must be a single fat ("shaded") JAR dropped into
  `$DREMIO_HOME/jars/3rdparty/` — no classpath configuration changes.

---

## 2. Understanding Dremio's Plugin Architecture

Dremio uses a layered plugin architecture that separates concerns across three phases:

### Registration (startup)

Dremio scans its classpath for JARs that contain `sabot-module.conf` and register
packages into the classpath scanner. Our single-line config tells Dremio to scan the
`com.dremio.plugins.cassandra` package:

```
dremio.classpath.scanning.packages += "com.dremio.plugins.cassandra"
```

Dremio's classpath scanner finds classes annotated with `@SourceType` and registers
them as available source types. That annotation on `CassandraStoragePluginConfig`
causes "Apache Cassandra" to appear in the **Add Source** dialog.

### Metadata (catalog) phase

When the user saves the source config, Dremio calls:

1. `ConnectionConf.newPlugin()` → creates the `StoragePlugin`
2. `StoragePlugin.start()` → opens the driver connection
3. `StoragePlugin.listDatasetHandles()` → discovers all tables
4. `StoragePlugin.getDatasetMetadata()` → builds the Arrow schema for each table
5. `StoragePlugin.listPartitionChunks()` → tells Dremio how to split the table for parallel reads

### Query execution phase

When a SQL query arrives:

1. Dremio's planner matches the scan node to the plugin via **planner rules** registered
   in `CassandraRulesFactory`.
2. The logical rule (`CassandraScanRule`) converts a generic `ScanCrel` into a
   `CassandraScanDrel` (logical).
3. The filter rule (`CassandraFilterRule`) extracts pushable predicates from any
   `FilterRel` sitting above the scan and bakes them into the scan spec.
4. The physical rule (`CassandraScanPrule`) converts `CassandraScanDrel` to
   `CassandraScanPrel` (physical).
5. `CassandraScanPrel.getPhysicalOperator()` produces a `CassandraGroupScan`.
6. Dremio's scheduler calls `GroupScan.getSpecificScan()` once per executor fragment,
   producing one `CassandraSubScan` per fragment.
7. `CassandraScanCreator` (the executor-side factory) maps each `SubScan` to a
   `CassandraRecordReader`.
8. The `RecordReader` runs the CQL query and writes Arrow batches.

Understanding this pipeline upfront was essential — getting any link in the chain
wrong causes either a compile error or a runtime `ClassCastException`/`NullPointerException`.

---

## 3. Project Skeleton and Maven Setup

### Directory layout

```
dremio-cassandra-connector/
├── pom.xml
├── install.sh
├── README.md
├── INSTALL.md
├── DEVELOPMENT.md
├── jars/
│   └── dremio-cassandra-connector-1.0.0-SNAPSHOT-plugin.jar  ← pre-built
└── src/
    └── main/
        ├── java/com/dremio/plugins/cassandra/
        │   ├── CassandraStoragePluginConfig.java
        │   ├── CassandraStoragePlugin.java
        │   ├── CassandraConnection.java
        │   ├── CassandraTypeConverter.java
        │   ├── exec/
        │   │   └── CassandraScanCreator.java
        │   ├── planning/
        │   │   ├── CassandraFilterRule.java
        │   │   ├── CassandraRulesFactory.java
        │   │   ├── CassandraScanDrel.java
        │   │   ├── CassandraScanPrel.java
        │   │   ├── CassandraScanPrule.java
        │   │   └── CassandraScanRule.java
        │   └── scan/
        │       ├── CassandraDatasetHandle.java
        │       ├── CassandraDatasetMetadata.java
        │       ├── CassandraGroupScan.java
        │       ├── CassandraPredicate.java
        │       ├── CassandraRecordReader.java
        │       ├── CassandraScanSpec.java
        │       └── CassandraSubScan.java
        └── resources/
            ├── sabot-module.conf
            ├── cassandra-layout.json
            └── APACHE_CASSANDRA.svg
```

### Maven dependencies

All Dremio and Arrow JARs are declared `<scope>provided</scope>` — they live in the
running Dremio container and must not be bundled. Only the DataStax Java Driver 4.x
(`java-driver-core`) is bundled in the fat JAR, with its transitive `netty` dependency
shaded to avoid conflicts.

The critical Dremio JARs used at compile time:

| Artifact | Purpose |
|---|---|
| `dremio-sabot-kernel` | `AbstractGroupScan`, `AbstractRecordReader`, `StoragePlugin` |
| `dremio-connector` | `DatasetHandle`, `PartitionChunk`, `DatasetSplit` |
| `dremio-services-namespace` | `ConnectionConf`, `SourceState`, `NamespaceKey` |
| `dremio-plugin-common` | `PluginSabotContext`, `SchemaMutability` |
| `dremio-sabot-logical` | `ExecutionSetupException` |
| `dremio-sabot-kernel-proto` | `CoreOperatorType` (proto-generated) |
| `dremio-sabot-vector-tools` | `BatchSchema` |
| `arrow-vector` | Arrow vector types (`VarCharVector`, `IntVector`, etc.) |
| `calcite-core` | `RelOptRule`, `RexNode`, `FilterRel` |

**Key challenge:** These JARs are not on Maven Central. They must be installed into
the local Maven repository using `mvn install:install-file`, pointing at the JARs
found in `$DREMIO_HOME/jars/` of the running Dremio instance. The `install.sh` script
automates this step.

### Shading

The maven-shade-plugin bundles the DataStax driver into the plugin JAR and relocates
its package roots to avoid classpath collisions:

```
com.datastax.oss.driver   → shaded.com.datastax.oss.driver
io.netty                  → shaded.io.netty
```

Jackson and SLF4J are explicitly excluded from the fat JAR because Dremio must
control those; including them causes `NPE` in annotation processing and log
configuration conflicts.

---

## 4. Step 1 — Source Configuration

**File:** `CassandraStoragePluginConfig.java`

This is the entry point for the entire plugin. It extends `ConnectionConf`, which
is Dremio's base class for source configurations. Its responsibilities:

1. **Define every configurable field** as a public field annotated with `@Tag` (for
   Protostuff serialization) and `@DisplayMetadata` (for the UI label).
2. **Register the plugin** under a source type name via `@SourceType`.
3. **Instantiate the plugin** via the required `newPlugin()` override.

```java
@SourceType(value = "APACHE_CASSANDRA", label = "Apache Cassandra",
            uiConfig = "cassandra-layout.json")
public class CassandraStoragePluginConfig
    extends ConnectionConf<CassandraStoragePluginConfig, CassandraStoragePlugin> {

  @Tag(1) @DisplayMetadata(label = "Contact Points")
  public String host = "localhost";

  @Tag(5) @Secret @DisplayMetadata(label = "Password")
  public String password;

  // ... 14 fields total ...

  @Override
  public CassandraStoragePlugin newPlugin(PluginSabotContext context,
                                          String name,
                                          Provider<StoragePluginId> pluginIdProvider) {
    return new CassandraStoragePlugin(this, context, name);
  }
}
```

**Design decisions:**

- `@Tag` numbers must be unique and must never change once deployed (Protostuff uses
  them as field IDs in the serialized catalog entry).
- `@Secret` on `password` tells Dremio to encrypt the value at rest.
- `@NotMetadataImpacting` on fields like `readTimeoutMs` and `fetchSize` tells Dremio
  not to invalidate cached metadata when only those fields change — saves unnecessary
  rescans.
- The `uiConfig = "cassandra-layout.json"` reference points to a resource file that
  controls the form layout in the Add Source UI.

---

## 5. Step 2 — The Plugin Lifecycle

**File:** `CassandraStoragePlugin.java`

Implements `StoragePlugin` (lifecycle + metadata API) and `SupportsListingDatasets`
(table enumeration for the catalog browser).

### Lifecycle methods

```java
@Override
public void start() throws IOException {
  connection = new CassandraConnection(config);
}

@Override
public void close() throws Exception {
  if (connection != null) connection.close();
}
```

### Health check

`getState()` is called periodically by Dremio's health-check thread. It performs a
lightweight metadata query to verify the connection is live:

```java
@Override
public SourceState getState() {
  try {
    connection.getSession().getMetadata().getKeyspaces();
    return SourceState.goodState();
  } catch (Exception e) {
    // Auto-reconnect on failure (see Step 10)
    reconnect();
    return SourceState.goodState();
  }
}
```

### Metadata cache

Schema metadata from Cassandra is cached for a configurable TTL (default 60 seconds)
to avoid hitting Cassandra on every planner call. The cache is invalidated on reconnect.

```java
private com.datastax.oss.driver.api.core.metadata.Metadata getCachedMetadata() {
  CachedMetadata cached = metadataCache.get("metadata");
  if (cached == null || cached.isExpired()) {
    // Fetch fresh from Cassandra and cache
    ...
  }
  return cached.metadata;
}
```

### Row count estimation

`listPartitionChunks()` needs a row count estimate to size each split. The plugin
queries `system.size_estimates` (a Cassandra system table that stores per-table
statistics). If no data is available it defaults to 10,000 rows.

---

## 6. Step 3 — Connecting to Cassandra

**File:** `CassandraConnection.java`

Wraps the DataStax `CqlSession`. One instance per plugin; thread-safe.

Key driver configuration applied from the `CassandraStoragePluginConfig`:

- `withLocalDatacenter(config.datacenter)` — required by `DefaultLoadBalancingPolicy`
- `withTimeout(Duration.ofMillis(config.readTimeoutMs))` — CQL request timeout
- Username/password via `PasswordAuthProvider` (only if credentials are configured)
- SSL/TLS via `SslEngineFactory` (if `sslEnabled`)
- Speculative execution via `ConstantSpeculativeExecutionPolicy` (if enabled)

---

## 7. Step 4 — Type Mapping

**File:** `CassandraTypeConverter.java`

Maps every CQL data type to an Arrow type for schema construction. The resulting
Arrow `Field` objects are assembled into a `BatchSchema` in `getDatasetMetadata()`.

Key mappings:

| CQL Type | Arrow Type | Notes |
|---|---|---|
| TEXT / VARCHAR / ASCII | UTF8 | |
| INT | Int(32, signed) | |
| BIGINT / COUNTER | Int(64, signed) | |
| TIMESTAMP | Timestamp(MILLISECOND, UTC) | Stored as epoch millis |
| DATE | Date(DAY) | Stored as days-since-epoch |
| DECIMAL | Decimal(38, 10) | Fixed scale; adjust if needed |
| UUID / TIMEUUID | UTF8 | Serialized as string |
| INET | UTF8 | Dotted-decimal string |
| BLOB | Binary | Raw bytes |
| LIST / SET / MAP / UDT | UTF8 | Serialized via `.toString()` |

All fields are marked nullable because Cassandra does not enforce `NOT NULL`.

---

## 8. Step 5 — Dataset Discovery and Metadata

**Files:** `CassandraDatasetHandle.java`, `CassandraDatasetMetadata.java`,
`CassandraStoragePlugin.java`

### Dataset listing (`listDatasetHandles`)

Iterates the Cassandra schema metadata to enumerate every non-system keyspace and
every table within it. System keyspaces (`system`, `system_auth`, `system_schema`,
`system_distributed`, `system_traces`, etc.) are always excluded. User-configured
`excludedKeyspaces` are also filtered out.

For each table a `CassandraDatasetHandle` is created — a thin wrapper around the
three-component `EntityPath` (`[sourceName, keyspace, tableName]`).

### Dataset metadata (`getDatasetMetadata`)

Fetches the column list from Cassandra's schema metadata and converts each column
to an Arrow `Field` via `CassandraTypeConverter`. Returns a `CassandraDatasetMetadata`
containing the `BatchSchema` and a row-count estimate used by the optimizer.

### Partition chunks (`listPartitionChunks`)

This is where split parallelism is defined. The method divides the Cassandra Murmur3
token ring into `config.splitParallelism` equal segments and creates one
`PartitionChunk` (containing one `DatasetSplit`) per segment.

Each split's extended property bytes encode the token range in pipe-delimited format:

```
keyspace|tableName|pk1,pk2|startToken|endToken
```

This string is read back by `CassandraGroupScan.decodeSplitSpec()` at execution time
to reconstruct the per-fragment `CassandraScanSpec`.

---

## 9. Step 6 — Planning Layer

**Files:** `CassandraScanRule.java`, `CassandraScanDrel.java`, `CassandraScanPrule.java`,
`CassandraScanPrel.java`, `CassandraRulesFactory.java`

### Rules factory

`CassandraRulesFactory` is the entry point for planning. Dremio calls
`getRulesFactoryClass()` on the plugin to discover it. The factory registers two
rule sets:

- **LOGICAL:** `CassandraScanRule` and `CassandraFilterRule`
- **PHYSICAL:** `CassandraScanPrule`

### Scan rule (logical)

`CassandraScanRule` matches any `ScanCrel` whose table path belongs to the Cassandra
plugin and rewrites it into a `CassandraScanDrel`. It creates the initial
`CassandraScanSpec` carrying the keyspace, table name, partition keys, and the
configured split parallelism.

### Scan Drel → Prel (physical rule)

`CassandraScanPrule` matches `CassandraScanDrel` and produces `CassandraScanPrel`.

### Physical operator → GroupScan

`CassandraScanPrel.getPhysicalOperator()` is called at the end of physical planning.
It creates a `CassandraGroupScan` — the planning-layer representation of the scan
that Dremio's scheduler uses to distribute work.

---

## 10. Step 7 — Execution Layer

**Files:** `CassandraGroupScan.java`, `CassandraSubScan.java`,
`CassandraScanCreator.java`, `CassandraRecordReader.java`

### GroupScan

`CassandraGroupScan` sits in the planning layer. It has two key methods:

**`getMaxParallelizationWidth()`** — returns how many parallel fragments Dremio may
create for this scan:

```java
@Override
public int getMaxParallelizationWidth() {
  if (scanSpec.hasPredicates()) {
    return 1; // direct partition lookup — single fragment prevents duplicate rows
  }
  return Integer.MAX_VALUE; // let split count bound parallelism naturally
}
```

Returning `Integer.MAX_VALUE` is the correct approach because the actual degree of
parallelism is bounded by the number of splits created in `listPartitionChunks()`
(= `splitParallelism`). Returning a hard-coded number would cap parallelism even
when the user configures more splits.

**`getSpecificScan(work)`** — called once per executor fragment with the list of
splits assigned to that fragment. Decodes each split's token range from the binary
extended property bytes and produces a `CassandraSubScan`.

### SubScan

`CassandraSubScan` is the executor-side work unit. It is JSON-serialized and sent
to the assigned executor node. It carries:

- The projected column list
- The primary `CassandraScanSpec` (for single-spec scans)
- The full list of `CassandraScanSpec` objects (one per token-range split assigned
  to this fragment)

### ScanCreator

`CassandraScanCreator` is the executor-side factory. Registered in `sabot-module.conf`
(implicitly, via classpath scanning). It maps a `CassandraSubScan` to a
`ScanOperator` backed by one or more `CassandraRecordReader` instances — one per
token-range split.

### RecordReader

`CassandraRecordReader` is the innermost execution component. Its lifecycle:

1. **`setup()`** — Fetches the live Cassandra table schema, allocates one Arrow
   vector per projected column, builds the CQL query string, and executes it.
2. **`next()`** — Reads up to `TARGET_BATCH_SIZE` (4,000) rows from the result
   iterator and writes each cell into the appropriate vector.
3. **`close()`** — Nulls the row iterator (the `CqlSession` is owned by the plugin
   and must not be closed here).

The reader uses a `ColumnInfo` inner class to track the column name, Cassandra type,
and Arrow vector together so the row-writing loop stays clean.

**Null handling:** Arrow vectors are nullable by default. For null cells the reader
simply does not call `setSafe()`, leaving the null bit unset — which Arrow interprets
as null.

**Batch control:** Cassandra paginates via CQL fetch size (`fetchSize` config, default
1,000 rows per page). Arrow batches are bounded at 4,000 rows. The driver handles
CQL paging transparently via its result set iterator.

---

## 11. Step 8 — Predicate Pushdown

**Files:** `CassandraFilterRule.java`, `CassandraPredicate.java`, `CassandraScanSpec.java`,
`CassandraRecordReader.java`

This was the most complex feature to implement correctly. The challenge is that at
planning time we do not have a live database connection — we cannot know which columns
are partition keys until execution time.

### Strategy: optimistic push + residual filter

1. **Planning time (`CassandraFilterRule`):** Extract every top-level AND conjunct
   that is a simple `column op literal` expression. Bake them into the scan spec as
   candidate predicates. **Keep the original filter as a residual above the scan.**
   This is the safety net — Dremio always post-filters, so results are correct even
   if the CQL predicate is wrong or impossible.

2. **Execution time (`CassandraRecordReader.setup()`):** Validate the candidate
   predicates against the live Cassandra schema metadata. If equality predicates
   cover all partition key columns, issue a direct partition-key CQL query. If not,
   fall back to the token-range scan.

### `CassandraFilterRule`

Fires on `Filter(CassandraScanDrel)` pairs in the LOGICAL planning phase. Uses
Calcite's `RelOptUtil.conjunctions()` to split AND conditions, then `tryExtract()`
to convert each conjunct into a `CassandraPredicate`.

Recognizes five operators: `=`, `>`, `>=`, `<`, `<=`. Handles both `col op literal`
and `literal op col` (flipping the operator for the latter).

Handles SQL literal types: VARCHAR/CHAR, INTEGER, BIGINT, DECIMAL, FLOAT, BOOLEAN,
TIMESTAMP, and DATE.

### `CassandraPredicate`

A JSON-serializable record carrying:
- `column` — the column name
- `op` — the operator (`EQ`, `GT`, `GTE`, `LT`, `LTE`)
- `value` — the literal value as a string
- `isString` — whether to wrap the value in CQL single quotes

Includes a `toCqlFragment()` method that renders the predicate as a CQL WHERE clause
fragment: `"column_name" = 'value'` or `"column_name" > 42`.

The `Op` enum includes a `flip()` method for handling `literal op column` expressions.

### Partition key validation in the RecordReader

```java
List<String> pkCols = tableMetadata.getPartitionKey().stream()
    .map(col -> col.getName().asInternal())
    .collect(Collectors.toList());

List<CassandraPredicate> pkEqPredicates = specPredicates.stream()
    .filter(p -> p.getOp() == Op.EQ && pkColSet.contains(p.getColumn()))
    .collect(Collectors.toList());

if (coveredPks.containsAll(pkColSet)) {
  // Safe to push — also collect clustering key range predicates
  Set<String> ckColSet = tableMetadata.getClusteringColumns().keySet().stream()
      .map(col -> col.getName().asInternal())
      .collect(Collectors.toSet());
  ...
  cql = spec.toCqlWithPushdown(colNames, pkEqPredicates, ckPredicates);
}
```

**Critical bug fixed here:** DataStax Driver 4.x's `getClusteringColumns()` returns
`Map<ColumnMetadata, ClusteringOrder>`, not a `List<ColumnMetadata>`. The initial
implementation called `.stream()` directly on the map, which compiles fine (Java streams
on `Map` iterate over `Map.Entry` objects) but produced `ColumnMetadata` cast errors
at runtime. Fixed by using `.keySet().stream()`.

### Single-fragment mode for pushdown

When predicates cover all partition key columns, `getMaxParallelizationWidth()` returns
`1`. This is critical: without it, Dremio would create N fragments, each of which would
issue the same direct partition-key query, producing N copies of the same rows.

---

## 12. Step 9 — Parallel Token-Range Splits

**Files:** `CassandraStoragePlugin.listPartitionChunks()`,
`CassandraGroupScan.decodeSplitSpec()`, `CassandraScanSpec.toCql()`

Cassandra uses the Murmur3 partitioner by default. Every partition key is hashed to
a 64-bit token in the range `[Long.MIN_VALUE, Long.MAX_VALUE]`. By dividing this ring
into N equal segments, N parallel readers can each scan their segment independently
with zero overlap.

### Creating splits

```java
long step = (parallelism <= 1) ? Long.MAX_VALUE
    : (Long.MAX_VALUE / parallelism) * 2 + 1;
long rangeStart = Long.MIN_VALUE;

for (int i = 0; i < parallelism; i++) {
  long start = rangeStart;
  long end = (i == parallelism - 1) ? Long.MAX_VALUE : start + step - 1;
  // Encode: keyspace|tableName|pk1,pk2|start|end
  ...
  rangeStart = end + 1;
}
```

The overflow guard `(next > start) ? next - 1 : Long.MAX_VALUE - 1` prevents integer
overflow when the step is very large.

### Decoding splits at execution time

`CassandraGroupScan.decodeSplitSpec()` reads the extended property bytes from each
`SplitWork`, parses the pipe-delimited string, and reconstructs a `CassandraScanSpec`
with the token range. This is done via reflection to avoid a compile-time dependency
on protobuf.

### CQL token range query

`CassandraScanSpec.toCql()` appends a token range filter when one is set:

```sql
SELECT "col1", "col2"
FROM "keyspace"."table"
WHERE token("partition_key") >= -9223372036854775808
  AND token("partition_key") <= -4611686018427387905
```

The `token()` function is part of standard CQL and works natively with Cassandra's
Murmur3 partitioner.

---

## 13. Step 10 — Auto-Reconnect

**File:** `CassandraStoragePlugin.java`

Dremio's health-check thread calls `getState()` roughly every 30–60 seconds.
If the Cassandra session is broken (network partition, node restart, IP change),
this call will throw. Rather than marking the source as `badState` permanently,
the plugin auto-reconnects:

```java
@Override
public SourceState getState() {
  try {
    connection.getSession().getMetadata().getKeyspaces();
    return SourceState.goodState();
  } catch (Exception e) {
    logger.warn("Health check failed ({}), attempting reconnect...", e.getMessage());
    try {
      reconnect();
      connection.getSession().getMetadata().getKeyspaces(); // verify
      logger.info("Reconnection successful");
      return SourceState.goodState();
    } catch (Exception reconnectEx) {
      return SourceState.badState("Connection lost and reconnect failed: "
          + reconnectEx.getMessage(), reconnectEx);
    }
  }
}

private synchronized void reconnect() throws IOException {
  try { if (connection != null) connection.close(); } catch (Exception ignore) {}
  connection = new CassandraConnection(config);
  metadataCache.clear(); // force fresh schema fetch on next query
}
```

The `synchronized` keyword on `reconnect()` prevents concurrent reconnection attempts
if multiple threads detect the failure simultaneously.

**Important note on IP changes:** If the Cassandra container IP changes between
restarts (a common Docker behavior), the reconnect will fail because the new
`CassandraConnection` uses the `host` from the saved config. In that case the user
must update the source config with the new IP and click Save. The Dremio REST API
can be used for scripted updates — but beware of concurrent update errors caused by
the health-check thread modifying the source's version tag between your GET and PUT.

---

## 14. Bugs Encountered and Fixed

### Bug 1: `getClusteringColumns().stream()` compile error

**Symptom:** `cannot find method stream()` at `tableMetadata.getClusteringColumns().stream()`

**Cause:** DataStax Driver 4.x changed `getClusteringColumns()` to return
`Map<ColumnMetadata, ClusteringOrder>` (column → sort order). In Driver 3.x it
returned a `List<ColumnMetadata>`. Calling `.stream()` directly on a `Map` compiles
(it gives a `Stream<Map.Entry>`) but produces `ClassCastException` when you try to
use the entries as `ColumnMetadata`.

**Fix:**
```java
// Wrong (Driver 3.x style):
tableMetadata.getClusteringColumns().stream()
    .map(col -> col.getName().asInternal())

// Correct (Driver 4.x):
tableMetadata.getClusteringColumns().keySet().stream()
    .map(col -> col.getName().asInternal())
```

### Bug 2: Build failure from stale `target/` directory owned by root

**Symptom:** `mvn clean package` failed with permission errors on files in `target/`

**Cause:** A previous Docker build ran Maven as root inside the container. The
`target/` directory was owned by root and synced back to the host via a Docker bind
mount. Subsequent host-side clean failed.

**Fix:** Remove the stale directory from inside the container before copying fresh
source:
```bash
docker exec -u root try-dremio bash -c "rm -rf /tmp/cassandra-build"
```

### Bug 3: Dremio health-check concurrent update errors

**Symptom:** `curl -X PUT .../source/cassandra_test` returned 409 Conflict with
"concurrent modification" even when the PUT was issued immediately after a GET.

**Cause:** Dremio's health-check thread runs constantly and updates the source
state (including its version tag) every 30–60 seconds. If the health-check ran
between the GET (which fetched the `tag`) and the PUT (which submitted it), the PUT
was rejected because its tag was stale.

**Fix:** Fetch the tag immediately before every PUT, and retry on 409. For the
specific IP-change scenario, delete the old source entirely and recreate it via
the v3 catalog API:

```bash
# Delete
curl -X DELETE "http://localhost:9047/api/v3/catalog/$SOURCE_ID?version=$TAG" \
  -H "Authorization: _dremio$TOKEN"

# Recreate with correct host
curl -X POST "http://localhost:9047/api/v3/catalog" \
  -H "Authorization: _dremio$TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"entityType":"source","type":"APACHE_CASSANDRA","name":"cassandra_test","config":{"host":"172.17.0.2",...}}'
```

### Bug 4: `read` command failure in non-interactive installer

**Symptom:** `install.sh` exited immediately when piped input was used (CI, scripted)

**Cause:** `set -euo pipefail` causes the script to exit when `read` returns exit
code 1 (which it does when stdin is not a tty and the user doesn't type anything).

**Fix:**
```bash
read -rp "Proceed? [Y/n]: " CONFIRM || CONFIRM="Y"
```

### Bug 5: `docker: command not found` in installer

**Symptom:** `install.sh --docker try-dremio` failed with "docker: command not found"
on macOS with Docker Desktop.

**Cause:** macOS Docker Desktop installs the `docker` binary at `/usr/local/bin/docker`.
When `install.sh` is invoked via certain launchers, `PATH` does not include
`/usr/local/bin`.

**Fix:** Add standard binary directories explicitly at the top of the script:
```bash
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"
```

### Bug 6: `getMaxParallelizationWidth()` returning `splitParallelism`

**Symptom:** Setting `splitParallelism = 16` in the source config had no effect —
Dremio always created far fewer than 16 fragments.

**Cause:** The method was returning `scanSpec.getSplitParallelism()` (e.g. 16).
But Dremio interprets this as a hard cap. The actual number of fragments is
`min(maxParallelizationWidth, availableExecutors, numberOfSplits)`. If
`maxParallelizationWidth` happened to be less than the number of splits created,
Dremio would merge splits into fewer fragments.

**Fix:** Return `Integer.MAX_VALUE` to remove the artificial cap. The actual degree
of parallelism is then bounded naturally by the split count from `listPartitionChunks()`.

---

## 15. Build and Packaging

### Building inside the Docker container

The recommended build path executes Maven inside the Dremio Docker container because:

1. The container has the correct JDK (same version Dremio uses).
2. The Dremio and Arrow JARs are already present at known paths.
3. No host-side Maven installation is required.

The build sequence:

```bash
# 1. Copy source into the container
docker cp . try-dremio:/tmp/cassandra-build

# 2. Install Dremio JARs into the container's local Maven repo
#    (17 JARs: dremio-sabot-kernel, dremio-common, arrow-vector, calcite-core, ...)
docker exec try-dremio bash -c "mvn install:install-file -Dfile=... ..."

# 3. Build the fat JAR
docker exec try-dremio bash -c "cd /tmp/cassandra-build && mvn package -DskipTests"

# 4. Copy the built JAR into Dremio's plugin directory
docker exec -u root try-dremio bash -c \
  "cp /tmp/cassandra-build/target/*-plugin.jar /opt/dremio/jars/3rdparty/"

# 5. Restart Dremio to pick up the new JAR
docker restart try-dremio
```

### The fat JAR

The maven-shade-plugin produces two JARs:

- `dremio-cassandra-connector-1.0.0-SNAPSHOT.jar` — thin JAR (source classes only)
- `dremio-cassandra-connector-1.0.0-SNAPSHOT-plugin.jar` — fat JAR (DataStax driver
  + netty bundled and shaded)

Only the `-plugin.jar` is deployed.

### Pre-built JAR

Once a successful build is completed, the plugin JAR is saved to `jars/` in the
project directory. This allows future installs on the same Dremio version to use
`--prebuilt`, skipping the Maven build entirely.

---

## 16. Testing

Testing is split into two tiers: a JUnit unit test suite that runs without any
live services, and a set of integration tests run manually against a live environment.

### Unit test suite

**128 tests** covering the pure-logic and CQL-generation layers. No Dremio, no
Cassandra, no Docker required — `mvn test` is sufficient.

```bash
# Run from inside the Dremio container (Maven + Dremio JARs already present)
docker exec try-dremio bash -c "cd /tmp/build-cassandra && mvn test"
```

| Test class | What it covers |
|---|---|
| `CassandraPredicateTest` | `toCqlFragment()` for all operators (EQ/GT/GTE/LT/LTE/IN), string quoting, apostrophe escaping, `Op.flip()` |
| `CassandraScanSpecTest` | `toCql()` and `toCqlWithPushdown()` CQL generation; LIMIT placement; ALLOW FILTERING order; `withLimit()` immutability; boolean flags |
| `CassandraTypeConverterTest` | Every Cassandra type → Arrow type mapping; LIST/SET/MAP/UDT nested field construction |
| `CassandraConnectionTest` | `resolveConsistency()` LOCAL_* relaxation logic; `detectDatacenterFromSession()` contact-point IP scoring and fallback paths |
| `CassandraStoragePluginTest` | `estimateColumnBytes()` for all type buckets; `estimateRowSizeBytes()` composite table calculation and 32-byte floor |

The test suite uses **JUnit Jupiter 5.10.0** and **Mockito 5.5.0** (already declared
in `pom.xml` as `test`-scope dependencies).

### Integration tests (live environment)

All integration tests were run against a live environment:

- **Dremio 26.0.5** running in Docker (`docker run ... dremio/dremio-oss:26.0.5`)
- **Apache Cassandra 4.x** running in a separate Docker container

### Test data setup

Three keyspaces/tables were created in Cassandra:

```sql
-- test_ks.users: UUID partition key
CREATE TABLE test_ks.users (
  user_id uuid PRIMARY KEY,
  name text, email text, age int, region text, score double
);

-- test_ks.events: composite partition + clustering key
CREATE TABLE test_ks.events (
  device_id text, event_time timestamp, value double,
  PRIMARY KEY (device_id, event_time)
);

-- test_ks.orders: for cross-source JOIN testing
CREATE TABLE test_ks.orders (
  order_id uuid, user_id uuid, amount decimal,
  PRIMARY KEY (order_id)
);
```

### Tests run

| Test | What it verified |
|---|---|
| Full table scan | `SELECT * FROM cassandra_test.test_ks.users LIMIT 100` returned rows |
| Projection pushdown | `SELECT user_id, name FROM ...` sent only those columns in CQL |
| Partition key pushdown | `WHERE user_id = '50babb30-...'` returned exactly 1 row |
| Clustering key pushdown | `WHERE device_id = 'sensor-001' AND event_time >= TIMESTAMP '2026-04-04 00:00:00'` returned 1 row |
| Token-range parallelism | Verified 4 parallel fragments in Dremio job UI when `splitParallelism = 4` |
| Cross-source JOIN (Hudi) | `JOIN hudi_local.sample_users ON region` returned 5 rows |
| Auto-reconnect | Stopped Cassandra container, waited, restarted it; source recovered in ~10 seconds |
| No-auth path | Cassandra default config (no password); source saved and queries worked |

---

## 17. The Installer

**File:** `install.sh`

A Bash script that handles all three deployment modes with a clean interactive UI.

### Modes

| Flag | Behavior |
|---|---|
| `--docker <container>` | Builds/deploys inside a running Docker container |
| `--local <dremio_home>` | Deploys to a bare-metal Dremio installation |
| `--k8s <pod>` | Deploys to a Kubernetes pod via `kubectl cp` + `exec` |

### JAR options

| Flag | Behavior |
|---|---|
| `--prebuilt` | Copies `jars/dremio-cassandra-connector-*-plugin.jar` |
| `--build` | Installs Dremio JARs + runs `mvn package` inside the container, then saves the result to `jars/` |

### Key implementation details

- `set -euo pipefail` for strict error handling.
- `export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"` to find Docker on macOS.
- All `read` prompts use `|| CONFIRM="Y"` to handle non-interactive mode gracefully.
- Docker mode waits for Dremio to pass its health check after restart:
  ```bash
  until curl -sf http://localhost:9047/api/v3/server_status | grep -q '"status":"OK"'; do
    sleep 2
  done
  ```

---

## 18. Final File Structure

```
dremio-cassandra-connector/
├── install.sh                          ← smart multi-mode installer
├── README.md                           ← feature overview + quick start
├── INSTALL.md                          ← detailed install + config reference
├── DEVELOPMENT.md                      ← this file
├── pom.xml                             ← Maven build (shade + provided deps)
├── jars/
│   └── dremio-cassandra-connector-1.0.0-SNAPSHOT-plugin.jar  ← pre-built (12MB)
├── src/main/
│   ├── java/com/dremio/plugins/cassandra/
│   │   ├── CassandraStoragePluginConfig.java   ← @SourceType, 24 config fields (@Tag(1)–@Tag(24))
│   │   ├── CassandraStoragePlugin.java         ← lifecycle, metadata, health-check
│   │   ├── CassandraConnection.java            ← CqlSession wrapper, DC auto-detect, SSL
│   │   ├── CassandraTypeConverter.java         ← CQL types → Arrow types
│   │   ├── exec/
│   │   │   └── CassandraScanCreator.java       ← SubScan → RecordReader factory
│   │   ├── planning/
│   │   │   ├── CassandraFilterRule.java        ← extracts pushdown predicates
│   │   │   ├── CassandraLimitRule.java         ← pushes LIMIT N into scan spec
│   │   │   ├── CassandraRulesFactory.java      ← registers planning rule sets
│   │   │   ├── CassandraScanDrel.java          ← logical scan rel node
│   │   │   ├── CassandraScanPrel.java          ← physical scan rel node
│   │   │   ├── CassandraScanPrule.java         ← Drel → Prel rule
│   │   │   └── CassandraScanRule.java          ← ScanCrel → Drel rule
│   │   └── scan/
│   │       ├── CassandraDatasetHandle.java     ← thin EntityPath wrapper
│   │       ├── CassandraDatasetMetadata.java   ← BatchSchema + stats
│   │       ├── CassandraGroupScan.java         ← planning-layer scan operator
│   │       ├── CassandraPredicate.java         ← serializable filter condition
│   │       ├── CassandraRecordReader.java      ← CQL → Arrow batch writer
│   │       ├── CassandraScanSpec.java          ← scan parameters + CQL builder
│   │       └── CassandraSubScan.java           ← JSON-serializable executor work unit
│   └── resources/
│       ├── sabot-module.conf                   ← registers plugin package with Dremio scanner
│       ├── cassandra-layout.json               ← Add Source UI form layout
│       └── APACHE_CASSANDRA.svg                ← source icon
└── src/test/
    └── java/com/dremio/plugins/cassandra/
        ├── CassandraConnectionTest.java        ← resolveConsistency + detectDatacenterFromSession
        ├── CassandraStoragePluginTest.java     ← estimateColumnBytes + estimateRowSizeBytes
        ├── CassandraTypeConverterTest.java     ← CQL type → Arrow type/field mapping
        └── scan/
            ├── CassandraPredicateTest.java     ← toCqlFragment, Op.flip
            └── CassandraScanSpecTest.java      ← toCql, toCqlWithPushdown, withLimit
```

---

## Summary of What Makes This Non-Trivial

Building a Dremio storage plugin from scratch is not well documented. The key
challenges that required the most research and iteration:

1. **Dremio JARs not on Maven Central.** They must be installed from the running
   container. The exact version strings (including timestamp suffixes) must match.

2. **Dremio's planning pipeline is strict.** The logical → physical rule chain must
   produce exactly the right types at each stage, or the planner throws uncaught
   `NullPointerExceptions` with no useful stack trace.

3. **DataStax Driver 4.x API changes.** `getClusteringColumns()` returns a `Map`
   not a `List` — a silent breakage from Driver 3.x.

4. **Predicate pushdown safety.** The filter rule must keep the original filter as
   a residual. Without this, non-partition-key predicates pushed to CQL would cause
   Cassandra to reject the query with `InvalidQueryException: Cannot execute this
   query as it might involve data filtering`.

5. **Parallel splits and `getMaxParallelizationWidth()`.** Returning a number that's
   too low silently merges splits; returning `Integer.MAX_VALUE` lets the split count
   bound parallelism correctly.

6. **Shading gotchas.** Including Jackson in the fat JAR breaks Dremio's annotation
   processing; including SLF4J breaks log configuration. Both must be excluded.

7. **Docker IP stability.** Cassandra containers get a new IP on every restart in
   bridge networking. The auto-reconnect handles session loss but cannot update the
   `host` config automatically — that requires a manual source edit or a scripted
   Dremio REST API call.

---

## 19. Post-Release Enhancements

These features were added after the initial working connector was validated end-to-end.
Each is a focused, self-contained improvement with no breaking changes to existing sources
(all new `@Tag` fields have safe defaults, so existing serialized configs deserialize correctly).

### Protocol Compression (`@Tag(23)` — `compressionAlgorithm`)

CQL wire-protocol compression applied at the DataStax driver level. Configured in
`CassandraConnection` constructor via `DefaultDriverOption.PROTOCOL_COMPRESSION`.
Accepted values: `NONE` (default), `LZ4`, `SNAPPY`.

Useful for WAN or cross-AZ deployments where network bandwidth is the bottleneck.
Leave as `NONE` on fast local networks — the CPU overhead outweighs the gain.
`LZ4` is the recommended choice when compression is needed: it is already bundled
in the DataStax driver fat JAR.

### Async Page Prefetch (`@Tag(24)` — `asyncPagePrefetch`)

Replaces the synchronous `ResultSet.iterator()` paging path with an async prefetch
pattern using `session.executeAsync()` and `AsyncResultSet.fetchNextPage()`.

The key insight: while `RecordReader.next()` is writing page N's rows into Arrow
vectors (CPU work), the driver is simultaneously fetching page N+1 over the network.
When `next()` is called again, the page is already buffered — zero per-page stall.

Implemented in `CassandraRecordReader` with three fields:
- `Iterator<Row> currentPageIter` — rows from the current `AsyncResultSet`
- `CompletionStage<AsyncResultSet> nextPageFuture` — in-flight fetch of the next page
- `boolean useAsyncPrefetch` — selected from config at `setup()` time

The synchronous path is kept as fallback (`asyncPagePrefetch=false`) for debugging.
Default is `true`. The only downside is holding two pages in memory simultaneously —
negligible at the default 1000-row page size.

### Schema Change Notifications

On every `CassandraRecordReader.setup()` call, a stable column fingerprint is computed
from the live `TableMetadata` (column names sorted alphabetically, colon-joined with
their CQL type strings). This fingerprint is compared against the last-seen value in
`CassandraStoragePlugin.schemaHashCache` (a `ConcurrentHashMap<String, String>`).

On a mismatch:
1. The hash is updated **before** throwing (so concurrent fragments don't all re-detect
   the same change)
2. `plugin.invalidateMetadataCache()` clears the TTL cache so `getDatasetMetadata()`
   returns the fresh schema on the next call
3. `UserException.schemaChangeError()` is thrown — Dremio's signal to re-plan the
   query with the updated schema, transparent to the user

The first read of any table seeds the cache silently (no re-plan).

### Routing Token (`setRoutingToken()`)

For token-range splits, the `tokenRangeStart` long is encoded as a big-endian 8-byte
`ByteBuffer` (Murmur3 token format), converted to a `Token` via `TokenMap.newToken()`,
and set on the `SimpleStatement` with `setRoutingToken()` + `setRoutingKeyspace()`.

This hints the DataStax driver's `DefaultLoadBalancingPolicy` to pick the replica owning
the start of the range as the coordinator, eliminating one extra network hop vs. picking
an arbitrary node. Zero planning overhead — the `TokenMap` is already loaded in memory.

Only set for `spec.hasTokenRange()` queries. PK-pushdown queries carry routing
information implicitly via the partition key value.

### Adaptive Fetch Size

`CassandraRecordReader.setup()` calls `CassandraStoragePlugin.estimateRowSizeBytes()`
(the same estimate used for planner statistics) and caps the fetch size to:

```java
max(50, min(configFetchSize, 2_097_152 / estimatedRowBytes))
```

This keeps each CQL page under 2 MB of raw data regardless of column width. A 40-byte/row
narrow table hits the `configFetchSize` cap (no change). A 5 KB/row wide table gets
~419 rows/page (~2 MB). The floor of 50 rows prevents degenerate micro-pages on
pathologically wide tables.

Only reduces the fetch size — never increases it above the user-configured value.
Logged at DEBUG level only when the fetch size is actually reduced.

### Contact Point Validation

Added to `CassandraConnection` constructor immediately after the `contactPoints` list
is built. `InetSocketAddress(hostname, port)` attempts DNS resolution at construction
time; `isUnresolved() == true` means the lookup failed.

The validation block:
- Throws `RuntimeException` if the `host` field is blank
- Logs `WARN` for each contact point that failed DNS resolution
- Logs `WARN "X/N resolved, proceeding"` on partial failure
- Throws `RuntimeException` if **all** contact points failed (no point opening a session)
- Logs `INFO "All N contact point(s) resolved successfully"` on full success
- Logs `DEBUG "hostname resolved → ip:port"` per resolved entry (useful for NAT/multi-IP)

Fires on both initial `start()` and every `reconnect()` call.

### VECTOR\<float, N\> Type (Cassandra 5.x)

Added support for Cassandra 5.x `VECTOR<float, N>` columns (embedding vectors for ML workloads).

**`CassandraTypeConverter`:**
- New branch before UDT handling: `instanceof VectorType` → `ArrowType.FixedSizeList(N)`
  with a non-nullable `Float4Vector` child named `$data$`
- Import: `com.datastax.oss.driver.api.core.type.VectorType` (DataStax 4.17.0+)

**`CassandraRecordReader`:**
- New dispatcher in `writeCell()`: `instanceof FixedSizeListVector` → `writeVectorCell()`
- New `writeVectorCell()` method: reads `CqlVector<Float>` via `row.getObject(col)`,
  writes each float to child `Float4Vector` at offset `idx * listSize + i`, calls
  `fslv.setNotNull(idx)` on the parent slot
- New case in `arrowTypeToVectorClass()`: `ArrowType.FixedSizeList` → `FixedSizeListVector.class`

**Storage layout:** `FixedSizeListVector` stores all elements contiguously — entry at parent
index `idx` occupies child slots `[idx * listSize, (idx+1) * listSize)`. Unlike `ListVector`
(which uses `startNewValue()/endValue()`), `FixedSizeListVector` writes directly to the
child vector at the computed offset.

Requires Cassandra 5.x. On earlier versions the type simply won't appear in schema
(no code change needed — `CassandraTypeConverter` only encounters `VectorType` if the
Cassandra cluster returns it).

### ARP Framework: Why It Doesn't Apply

Investigated whether Dremio's Advanced Relational Pushdown (ARP) framework could reduce
the rebuild requirement on Dremio version upgrades. Short answer: no.

ARP requires (1) a JDBC driver, and (2) a SQL-speaking source. Cassandra satisfies neither.
ARP connectors (PostgreSQL, Snowflake) have the same per-version rebuild requirement.
The only benefit of ARP is a smaller code surface (~150 lines), not elimination of rebuilds.

Our schema change notification approach (fingerprint on every `setup()` call) is more
sophisticated than anything ARP provides — ARP has no execution-layer code at all.

### Version Upgrade Tooling: rebuild.sh + rebuild-ui.py

Created a one-command (or one-click) upgrade path for users upgrading Dremio:

**`rebuild.sh`:** Detects Dremio/Arrow/Hadoop/Calcite versions from JAR filenames in the
running instance, diffs against `pom.xml`, updates the POM, installs JARs into Maven local
repo, compiles, deploys, and restarts. On build failure: restores `pom.xml.bak` and prints
targeted diagnostic guidance. Supports Docker, bare-metal, and Kubernetes.

**`rebuild-ui.py`:** Self-contained Python 3 web server (stdlib only). Serves a browser UI
on `http://localhost:8765` with mode selection, real-time SSE output streaming, and color-
coded status. Zero dependencies beyond Python 3.

**Platform launchers** (double-click to start):
- `Rebuild Connector.command` — macOS
- `Rebuild Connector.bat` — Windows
- `Rebuild Connector.sh` — Linux / Linux Mint terminal
- `Rebuild Connector.desktop` — Linux Mint Cinnamon desktop icon
