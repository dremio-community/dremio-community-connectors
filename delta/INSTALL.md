# Dremio Delta Lake Connector — Install Guide

Adds **read and write** support for Delta Lake tables to Dremio 26.x.
Dremio can read Delta tables natively, but has no write path. This connector
adds **CTAS write support** by implementing Dremio's internal plugin write interfaces
and wiring them to `delta-standalone` (no Spark required) for transaction log management.

> **Dremio 26.x DML note:** `INSERT INTO`, `DELETE FROM`, and `MERGE INTO` are
> Iceberg-only in Dremio 26.x and are not supported on Delta tables. Use `CTAS` to
> write data. Tables are **auto-discovered after CTAS** — no manual promotion needed.
> See [README.md](README.md) for details.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Dremio | 26.0.5 (match `dremio.version` in `pom.xml`) |
| Java | 11+ |
| Maven | 3.6+ |
| Docker (for the quick-start path) | Any recent version |

---

## Quick Start — Docker (recommended)

This is the fastest path if you are running Dremio in a Docker container.

### 1. Verify Dremio is running

```bash
docker ps | grep try-dremio
```

If not running, start it first and complete the initial Dremio setup (create admin user, etc.).

### 2. Run the installer

From the `dremio-delta-connector/` directory:

```bash
chmod +x install.sh
./install.sh
```

The script:
- Installs Maven inside the container (first run only)
- Installs all required Dremio JAR dependencies into the container's local Maven repo
- Builds the connector inside the container (avoids Mac UID/permission issues)
- Deploys the plugin JAR to `/opt/dremio/jars/3rdparty/`

### 3. Restart Dremio

The installer restarts Dremio automatically. To restart manually:

```bash
docker restart try-dremio
```

Wait ~30 seconds for Dremio to fully start.

> **Deploying both connectors at once?** Run `build-and-deploy.sh` from the parent
> directory instead — it builds and deploys both the Delta and Hudi connectors in one pass.

### 4. Add the source in the Dremio UI

1. Open `http://localhost:9047`
2. Go to **Sources** → **Add Source**
3. Select **Delta Lake**
4. Configure:
   - **Root Path** — the base directory containing your Delta tables
     (e.g. `s3://my-bucket/delta-tables` or `/data/delta` for local/NAS)
   - **Compression Codec** — Parquet compression for writes (`snappy`, `zstd`, etc.)
   - **Default Partition Column** — optional partition column for new tables
   - **Allow Schema Evolution** — whether to permit schema changes on write
5. Click **Save**

---

## Manual Install (non-Docker Dremio)

Use this path if you are deploying to a standalone Dremio installation.

### 1. Install Dremio JAR dependencies into your local Maven repo

The connector depends on Dremio's internal JARs which are **not on Maven Central**.
You need to install them from your running Dremio installation:

```bash
DREMIO_HOME=/opt/dremio   # adjust to your install path
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
HADOOP_VER=3.3.6-dremio-202507241551560856-75923ad5

install_jar() {
  mvn install:install-file \
    -Dfile="$1" -DgroupId="$2" -DartifactId="$3" -Dversion="$4" \
    -Dpackaging=jar -q
}

JARS=$DREMIO_HOME/jars
TP=$JARS/3rdparty

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar          com.dremio dremio-sabot-kernel          $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar                com.dremio dremio-common                $DREMIO_VER
install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar         com.dremio.plugin dremio-plugin-common  $DREMIO_VER
install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar    com.dremio dremio-sabot-kernel-proto    $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar    com.dremio dremio-sabot-vector-tools    $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar    com.dremio dremio-services-namespace    $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar             com.dremio dremio-connector             $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar         com.dremio dremio-sabot-logical         $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar           com.dremio dremio-common-core           $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                    org.apache.arrow arrow-vector           $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar               org.apache.arrow arrow-memory-core      $ARROW_VER
install_jar $TP/arrow-format-${ARROW_VER}.jar                    org.apache.arrow arrow-format           $ARROW_VER
install_jar $TP/hadoop-common-${HADOOP_VER}.jar                  org.apache.hadoop hadoop-common         $HADOOP_VER
install_jar $TP/guava-33.4.0-jre.jar                             com.google.guava guava                  33.4.0-jre
install_jar $TP/javax.inject-1.jar                               javax.inject javax.inject               1
```

### 2. Build the connector

```bash
cd dremio-delta-connector
mvn package -DskipTests
```

The plugin JAR is at `target/dremio-delta-connector-1.0.0-SNAPSHOT-plugin.jar`.

### 3. Deploy the JAR

```bash
cp target/dremio-delta-connector-1.0.0-SNAPSHOT-plugin.jar \
   $DREMIO_HOME/jars/3rdparty/
```

### 4. Restart Dremio

```bash
# Standalone:
$DREMIO_HOME/bin/dremio restart

# Systemd:
sudo systemctl restart dremio
```

---

## After Install: Writing and Reading a Delta Table

### Write (CTAS)

Use `STORE AS (type => 'delta_write')` — **not** `'delta'`:

```sql
CREATE TABLE delta_local.my_table
STORE AS (type => 'delta_write')
AS SELECT * FROM some_source.some_table;
```

This creates:
- `{root}/{my_table}/_delta_log/00000000000000000000.json` — schema + AddFile commit
- `{root}/{my_table}/part-{uuid}.snappy.parquet` — the data file

### Read (no promotion required)

Tables are immediately queryable after CTAS. `DeltaFormatMatcher` auto-detects the
`_delta_log/` directory and registers the table in Dremio's catalog automatically.

```sql
SELECT COUNT(*) FROM delta_local.my_table;
SELECT * FROM delta_local.my_table LIMIT 100;
```

> **Existing Delta tables** (pre-populated environments): point Dremio at the source root
> and run a metadata refresh — all directories containing `_delta_log/` will be
> auto-registered as datasets without any manual promotion.

---

## Upgrading Dremio Version

When upgrading Dremio, update `dremio.version`, `arrow.version`, and `hadoop.version`
in `pom.xml` to match the new version's exact strings (check the JAR filenames in
`$DREMIO_HOME/jars/`), re-install the JARs, rebuild, and redeploy.

---

## Key Version Constraints

**delta-standalone 0.6.0 API notes** (important if updating the dependency):

- `Metadata` constructor: `StructType` schema is the **last** argument, not 5th.
- `snapshot.scan()` returns `io.delta.standalone.DeltaScan`, not `io.delta.standalone.Scan`.
- `DeltaLog.getChanges()` returns `java.util.Iterator<VersionLog>` — it is **not** `AutoCloseable`
  and must not be used in a try-with-resources block.

These differ from the delta-standalone 3.x API — do not blindly follow the upstream docs.

---

## How It Works

| Component | Role |
|---|---|
| `DeltaPluginConfig` | Annotated with `@SourceType("DELTA")` — registers the plugin with Dremio's classpath scanner via `sabot-module.conf` |
| `DeltaStoragePlugin` | Extends `FileSystemPlugin` — handles metadata listing and table detection |
| `DeltaSnapshotUtils` | Reads the `_delta_log/` transaction log to list active Parquet files for the current snapshot |
| `DeltaFormatPlugin` | Teaches Dremio's file scanner to treat Delta table directories as single logical tables |
| `DeltaRecordWriter` | Translates Dremio's Arrow write batches → Avro → Parquet, then commits an `OptimisticTransaction` to the Delta log |
| `ArrowToDeltaSchemaConverter` | Converts Dremio's Arrow schema to delta-standalone's `StructType` |

### Write flow

```
Dremio query engine
  → DeltaRecordWriter.writeBatch() [Arrow VectorAccessible → Avro GenericRecord → Parquet file]
  → DeltaRecordWriter.close()     [OptimisticTransaction.commit(AddFile actions)]
  → _delta_log/ updated           [new JSON commit file written]
```

### Icon
`DELTA.svg` is bundled inside the plugin JAR. Dremio reads it at startup via
`ClassLoader.getResource("DELTA.svg")` and returns the SVG to the UI, which renders
it inline in the Add Source dialog.

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded, the connector JAR must be rebuilt against the new Dremio JARs.
`rebuild.sh` automates the entire process — version detection, `pom.xml` update, compile, deploy, restart:

```bash
# Docker (default container: try-dremio)
./rebuild.sh

# Named container
./rebuild.sh --docker my-dremio

# Bare-metal
./rebuild.sh --local /opt/dremio

# Kubernetes — auto-deploys to coordinator + all executor pods
./rebuild.sh --k8s dremio-master-0
./rebuild.sh --k8s dremio-master-0 --namespace dremio-ns   # with namespace

# Preview what would change without touching anything
./rebuild.sh --dry-run

# Force rebuild even if version strings already match (after source code edits)
./rebuild.sh --force
```

**What it does:**
1. Reads JAR filenames from the running instance to detect `dremio.version`, `arrow.version`, and `hadoop.version`
2. Compares against `pom.xml` — exits cleanly if nothing changed
3. Updates `pom.xml`, backs up the previous version to `pom.xml.bak`
4. Installs Dremio JARs from the running instance into the Maven local repo
5. Builds the connector from source against the new JARs
6. Deploys the new JAR to every pod / container
7. Restarts Dremio (Docker only) or prints restart instructions

If the build fails, `rebuild.sh` restores `pom.xml` automatically and prints a diagnostic guide for common API-change errors.

---

## Troubleshooting

**Source doesn't appear in Add Source**
- Confirm `sabot-module.conf` is in the JAR: `jar tf dremio-delta-connector-*-plugin.jar | grep sabot`
- Check Dremio startup logs: `grep -i delta /opt/dremio/log/server.log`

**Build fails with "artifact not found"**
- The Dremio JARs must be installed into your local Maven repo first (Step 1 above).

**`StructType cannot be converted to List<String>` compile error**
- You have an incorrect `Metadata` constructor argument order. In delta-standalone 0.6.0,
  `StructType` schema is the **last** (8th) parameter. See `DeltaRecordWriter.java` for the
  correct call.

**Write commits succeed but data isn't visible**
- Confirm the table path contains a valid `_delta_log/` directory.
- Try right-clicking the source in Dremio → **Refresh Metadata** → retry the query.
- Tables are auto-discovered via `_delta_log/` detection — no manual promotion needed.
  If a table still isn't visible after a refresh, check Dremio logs: `docker logs try-dremio | grep -i delta`

**CTAS fails with NullPointerException at WriterCommitterPOP**
- You used `STORE AS (type => 'delta')` instead of `STORE AS (type => 'delta_write')`.
- Dremio has a built-in read-only `DeltaLakeFormatPlugin` registered under the name `"delta"` whose `getWriter()` returns null. This connector's write plugin is registered as `"delta_write"` to avoid the naming conflict.

**Correct CTAS syntax:**
```sql
CREATE TABLE delta_source.my_table
STORE AS (type => 'delta_write')
AS SELECT * FROM staging.raw_data;
```
