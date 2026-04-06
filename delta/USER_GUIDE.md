# Dremio Delta Lake Connector — User Guide

Query and write Delta Lake tables directly from Dremio SQL. The connector reads Delta Lake tables using the native Delta protocol (transaction log + Parquet data files) and writes new tables or appends to existing ones with full ACID semantics.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a Delta Lake Source in Dremio](#adding-a-delta-lake-source-in-dremio)
   - [Root Path](#root-path)
   - [Compression Codec](#compression-codec)
   - [Default Partition Column](#default-partition-column)
   - [Allow Schema Evolution](#allow-schema-evolution)
   - [Target File Size](#target-file-size)
   - [Write Buffer Max Rows](#write-buffer-max-rows)
   - [Read Batch Size](#read-batch-size)
   - [Snapshot Cache TTL](#snapshot-cache-ttl)
   - [Connection Properties](#connection-properties)
5. [Writing Queries](#writing-queries)
6. [Writing Data](#writing-data)
7. [Understanding Delta Lake](#understanding-delta-lake)
8. [Performance Considerations](#performance-considerations)
9. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector uses the Delta Lake standalone library to read Delta tables stored on any filesystem Dremio can access (S3, HDFS, Azure Data Lake Storage, local filesystem, etc.).

**Reading:** The connector resolves the current table snapshot by reading the Delta transaction log (`_delta_log/`). The log contains a sequence of JSON commit files that describe which Parquet data files are currently part of the table (adds and removes). The connector reads only the active Parquet files identified by the latest snapshot.

**Writing:** CTAS (`CREATE TABLE … AS SELECT`) and `INSERT INTO` statements write new Parquet files and append a new commit entry to the `_delta_log/`. Each write is atomic — either all files and the log entry are written successfully, or the table is unchanged.

**Partition pruning:** When the table has partition columns, Dremio pushes partition filters into the file listing phase. Only Parquet files from matching partitions are opened, dramatically reducing I/O.

Each table discovered under the **Root Path** is exposed as a Dremio dataset.

---

## Prerequisites

- Delta Lake tables (protocol version 1 or 2) on a filesystem accessible to Dremio executor nodes
- Dremio 26.x
- For S3: Dremio must be configured with AWS credentials or an IAM role for the bucket
- For HDFS: Dremio must be configured with the Hadoop cluster's `core-site.xml` / `hdfs-site.xml`

---

## Installation

```bash
cd dremio-delta-connector

# Interactive installer
./install.sh

# Non-interactive:
./install.sh --docker try-dremio --prebuilt   # pre-built JAR (recommended)
./install.sh --docker try-dremio --build       # build from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

After installation, restart Dremio. The "Delta Lake" source type will appear in **Sources → +**.

---

## Adding a Delta Lake Source in Dremio

Navigate to **Sources → + → Delta Lake**. Fill in the fields below.

---

### Root Path

**Required.** The base filesystem path where your Delta Lake tables are stored. The connector scans this path recursively to discover tables.

```
s3://my-data-bucket/delta-tables
hdfs:///data/delta
/mnt/datalake/delta
file:///tmp/delta-dev
```

- Each subdirectory containing a `_delta_log/` directory is treated as one Delta table.
- Subdirectories without `_delta_log/` are treated as namespaces (Dremio schemas) for organizational purposes — they can contain more Delta table subdirectories.
- For S3: Dremio must have `s3:GetObject` / `s3:ListBucket` permissions on this prefix.
- For HDFS: The path must be reachable from all Dremio executor nodes.

**Example layout:**

```
s3://my-bucket/delta-tables/
  orders/               ← Delta table (has _delta_log/)
  customers/            ← Delta table
  analytics/            ← sub-namespace
    events/             ← Delta table
    sessions/           ← Delta table
```

Dremio would expose: `delta.orders`, `delta.customers`, `delta.analytics.events`, `delta.analytics.sessions`.

---

### Compression Codec

The compression algorithm used when writing new Parquet files.

| Codec | Description |
|-------|-------------|
| **SNAPPY** *(default)* | Fast compression and decompression. Best all-around choice for most workloads. Widely supported by all Parquet readers. |
| **ZSTD** | Higher compression ratio than Snappy with good read/write speed. Recommended for storage-cost-sensitive workloads or high-bandwidth environments. |
| **GZIP** | Highest compression ratio of the three. Slower to compress and decompress. Good for cold-tier or archival data where CPU is not a constraint. |
| **UNCOMPRESSED** | No compression. Maximum read/write throughput when CPU is the bottleneck, or for small development tables. Results in larger files. |

- This setting only affects newly written files. Existing files in the table retain their original compression.
- For cloud object stores (S3, GCS, ADLS), SNAPPY or ZSTD provides the best cost/performance balance.
- For on-premise HDFS with limited network bandwidth, ZSTD or GZIP reduces network transfer during scans.

---

### Default Partition Column

The column name used to partition new Delta tables created via CTAS. Leave blank for unpartitioned tables.

- Partitioning creates subdirectories named `column=value` (Hive-style) under the table root.
- Example: partitioning by `date` creates `date=2024-01-15/`, `date=2024-01-16/`, etc.
- Dremio automatically pushes `WHERE date = '...'` filters into the file listing phase, avoiding full table scans.
- Common choices: `date`, `region`, `country`, `year`
- For multi-level partitioning, use a comma-separated list: `year,month` — this creates nested subdirectories `year=2024/month=01/`.
- Leave blank for small tables or tables where queries always scan the full dataset.

**Reading existing tables:** The connector reads the partition schema from the Delta transaction log, which overrides this default. This setting only affects new tables created by CTAS.

---

### Allow Schema Evolution

When checked, the connector automatically updates the Delta table schema to include new columns when an `INSERT INTO` statement adds rows with extra columns.

- Default: disabled (unchecked)
- When **disabled**: An `INSERT INTO` with extra columns will fail with a schema mismatch error. This is the safe default — it prevents accidental schema drift.
- When **enabled**: If the incoming data contains columns not in the current table schema, those columns are added to the Delta table schema, and existing rows will have `NULL` for the new columns.

**When to enable:**
- ETL pipelines that evolve upstream schemas over time and should propagate changes to Delta tables automatically.
- Development environments where schemas are still in flux.

**When to leave disabled:**
- Production tables with a fixed, validated schema.
- Tables consumed by downstream systems that would break if unexpected columns appear.

---

### Target File Size

Target size in bytes for each Parquet file written by the connector.

- Default: `134217728` (128 MB)
- The connector writes Parquet files as close to this size as possible. Actual file sizes may vary based on row width and compression ratio.
- **Smaller files** (32–64 MB): Better parallelism on read (more splits), but more metadata overhead and more small-file requests to object stores.
- **Larger files** (256–512 MB): Fewer files, better object store efficiency, reduced metadata overhead. But a single task failure requires re-reading more data.
- For S3 and cloud object stores: 128–256 MB is the recommended range.
- For HDFS: Match the HDFS block size (typically 128 MB or 256 MB).
- For development/testing with small datasets: 16–32 MB avoids creating a single large file that's slow to write serially.

---

### Write Buffer Max Rows

Maximum number of rows to buffer in memory per write thread before flushing to a Parquet file.

- Default: `100000`
- A larger buffer allows the Parquet writer to see more data before encoding column statistics and dictionary pages, improving compression and sort quality within each file.
- **Reduce** if you see `OutOfMemoryError` during CTAS operations (e.g., try 25000–50000 for wide tables with many large string columns).
- **Increase** for narrow tables (few columns, mostly numeric) where you want better page-level compression.

---

### Read Batch Size

Number of rows returned per Arrow record batch during a table scan.

- Default: `4096`
- Larger batches (8192–16384) improve vectorized processing throughput for narrow tables in analytical queries.
- Smaller batches (1024–2048) reduce peak memory for tables with many large string or binary columns.
- The default of 4096 is appropriate for most workloads. Adjust only if you observe memory pressure or unsatisfying scan throughput.

---

### Snapshot Cache TTL

How long (in seconds) Dremio caches the resolved Delta table snapshot (the list of active Parquet files) before re-reading the transaction log.

- Default: `60` seconds
- After a write to a Delta table (whether by this connector or by Spark/Databricks/other tools), Dremio will continue serving the cached snapshot until the TTL expires.
- Set to `0` to always read fresh snapshots — useful during active development or when multiple writers are updating the table frequently.
- After a known write, you can also force an immediate refresh: right-click the table in Dremio UI → **Refresh Metadata**.
- In production with infrequent writes, 60–300 seconds reduces unnecessary transaction log reads.

---

### Connection Properties

Additional key-value properties passed to the underlying Hadoop filesystem layer. One property per line, in `key=value` format.

```
fs.s3a.endpoint=https://s3.us-east-1.amazonaws.com
fs.s3a.connection.maximum=50
```

**S3-compatible storage (MinIO, Ceph, etc.):**
```
fs.s3a.endpoint=http://minio:9000
fs.s3a.path.style.access=true
fs.s3a.access.key=minioadmin
fs.s3a.secret.key=minioadmin
```

**HDFS with custom NameNode:**
```
fs.defaultFS=hdfs://namenode:8020
dfs.client.use.datanode.hostname=true
```

**Azure Data Lake Storage Gen2:**
```
fs.azure.account.auth.type=OAuth
fs.azure.account.oauth.provider.type=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
fs.azure.account.oauth2.client.id=<app-id>
fs.azure.account.oauth2.client.secret=<secret>
fs.azure.account.oauth2.client.endpoint=https://login.microsoftonline.com/<tenant>/oauth2/token
```

**Google Cloud Storage:**
```
fs.gs.auth.service.account.enable=true
fs.gs.auth.service.account.json.keyfile=/etc/gcs/service-account.json
```

---

## Writing Queries

### Basic queries

```sql
-- List all tables in the source
SHOW TABLES IN delta;

-- Query the latest snapshot
SELECT * FROM delta.orders LIMIT 100;

-- Filter with partition pruning (automatic when filtering on the partition column)
SELECT order_id, customer_id, total_amount
FROM delta.orders
WHERE date = '2024-01-15'
  AND status = 'SHIPPED';

-- Aggregation
SELECT
    date,
    COUNT(*) AS order_count,
    SUM(total_amount) AS daily_revenue
FROM delta.orders
WHERE date >= '2024-01-01'
GROUP BY date
ORDER BY date DESC;
```

### Time travel queries

Delta Lake maintains a complete history of all commits. You can query any past snapshot:

```sql
-- Query the table as of a specific timestamp
SELECT * FROM delta.orders
  AT TIMESTAMP '2024-01-15 12:00:00'
LIMIT 100;

-- Query as of a specific Delta version number
SELECT * FROM delta.orders
  AT VERSION 42
LIMIT 100;
```

Version numbers are the commit numbers in the `_delta_log/` directory (0, 1, 2, …).

### Change data feed

If the Delta table was created or altered with `delta.enableChangeDataFeed = true`:

```sql
-- Read change events between two versions
SELECT *
FROM table_changes('delta.orders', 10, 20)
WHERE _change_type IN ('insert', 'update_postimage');
```

### Cross-source joins

```sql
-- Join Delta Lake with a Kafka topic
SELECT
    d.order_id,
    d.total_amount,
    k.status AS realtime_status
FROM delta.orders d
JOIN kafka.order_updates k ON d.order_id = k.order_id
WHERE d.date = CURRENT_DATE;

-- Join two Delta tables
SELECT
    o.order_id,
    o.total_amount,
    c.email,
    c.tier
FROM delta.orders o
JOIN delta.customers c ON o.customer_id = c.customer_id
WHERE o.date >= '2024-01-01';
```

---

## Writing Data

### CTAS — Create a new Delta table

```sql
-- Create a partitioned Delta table from a query result
CREATE TABLE delta.orders_archive AS
SELECT order_id, customer_id, total_amount, status, date
FROM postgres.orders
WHERE date < '2024-01-01';

-- The table is created using the source's configured defaults:
-- Compression: SNAPPY (or your configured Compression Codec)
-- Partition: date (or your configured Default Partition Column)
```

### INSERT INTO — Append rows to an existing table

```sql
-- Append new orders to the Delta table
INSERT INTO delta.orders
SELECT order_id, customer_id, total_amount, status, CURRENT_DATE AS date
FROM staging.new_orders
WHERE processing_date = CURRENT_DATE;
```

Delta appends a new commit to the `_delta_log/` directory for each INSERT, preserving full history.

### CTAS into a specific location pattern

If you want to create multiple tables with different partitioning schemes, create separate Dremio sources pointing to different Root Path locations.

---

## Understanding Delta Lake

### The Transaction Log

Every Delta table has a `_delta_log/` directory containing:
- **JSON commit files** (`000000000000000000001.json`, etc.): Each file represents one atomic commit, listing which Parquet files were added or removed.
- **Checkpoint files** (`*.checkpoint.parquet`): Periodic snapshots of all active files — avoids reading the entire log history on every query. Checkpoints are created every 10 commits by default.

When Dremio opens a Delta table, it reads the latest checkpoint (if any) plus all JSON commit files since the last checkpoint to determine the current set of active Parquet files.

### ACID Guarantees

- **Atomicity:** A write either fully completes (all files + log entry written) or has no effect.
- **Snapshot isolation:** Each query reads a consistent snapshot frozen at query start. Concurrent writes by other tools (Spark, Databricks) do not affect in-progress Dremio reads.
- **Append-only log:** The `_delta_log/` is never modified — only new entries are appended. This makes concurrent reads and writes safe without locks.

### Schema

The table schema is stored in the Delta transaction log, not inferred from Parquet file metadata. This means:
- The schema is consistent across all Parquet files, even after schema evolution.
- Dremio reads the authoritative schema from the log, which is always correct.
- Schema changes are versioned — time-travel queries return the schema that was current at that version.

---

## Performance Considerations

**Partition your tables** — Unpartitioned tables require scanning all Parquet files on every query. Partition on the column most used in `WHERE` clauses (typically a date or region). Dremio pushes the partition filter into the file listing step and skips non-matching partitions entirely.

**Match Target File Size to your storage** — For S3 and cloud object stores, target 128–256 MB per file. Object stores have per-request overhead — many small files means many HTTP requests. The Delta transaction log also scales with the number of files (each commit entry references all added/removed files).

**Use ZSTD for better compression** — ZSTD provides 15–30% better compression than SNAPPY with comparable read speed. For cost-sensitive workloads on cloud storage, switching from SNAPPY to ZSTD can meaningfully reduce storage costs.

**Tune Snapshot Cache TTL** — If your Delta tables are written by other tools (Spark, Databricks) on a known schedule, set the TTL slightly longer than the write interval to avoid unnecessary transaction log reads. For tables written once per hour, a TTL of 3600 seconds means Dremio reads the log once per batch cycle rather than on every query.

**Increase Read Batch Size for wide scans** — The default 4096 rows per batch is conservative. For narrow analytical tables (10–20 numeric columns), increasing to 8192 or 16384 improves vectorized processing throughput.

**Z-order clustering (advanced)** — If your queries frequently filter on non-partition columns, consider running Z-order optimization via Spark or Delta Standalone after large writes. Z-order co-locates rows with similar values in the same Parquet files, allowing Dremio to skip files based on Parquet column statistics.

---

## Troubleshooting

### Tables not appearing in catalog

- Verify the **Root Path** is correct and accessible from Dremio executors.
- Each table directory must contain a `_delta_log/` subdirectory. The connector skips directories without it.
- Right-click the source in Dremio UI → **Refresh Metadata**.
- Check Dremio server logs for filesystem access errors (permission denied, bucket not found, etc.).

### Queries return stale data after writes

- Wait for the **Snapshot Cache TTL** to expire, or right-click the table → **Refresh Metadata** to force an immediate re-read of the transaction log.
- Set **Snapshot Cache TTL** to `0` during development to always read the latest snapshot.

### "FileNotFoundException" — Parquet file listed in transaction log is missing

Delta Lake's transaction log lists specific Parquet file paths. If those files are missing (deleted outside of the Delta protocol, or moved), reads will fail.

**Cause:** Parquet data files were deleted or moved manually (outside of a Delta transaction), or an object store lifecycle policy expired files that are still referenced in the log.

**Fix:** Do not delete Parquet files under a Delta table manually. Use `VACUUM` (via Spark or Delta Standalone) to safely remove old files after they are no longer referenced by any live snapshot.

### "Protocol version not supported"

Delta tables with writer version 3+ or reader version 2+ use Delta features (column mapping, deletion vectors, etc.) that may not be supported by this connector version.

**Check the protocol version:**
```bash
cat _delta_log/00000000000000000000.json | python3 -m json.tool | grep protocol
```

**Fix:** If the table was created by Databricks Runtime 11+ or Delta Lake 2.x with advanced features enabled, it may require features not yet implemented. Recreate the table with `delta.minWriterVersion=2` and `delta.minReaderVersion=1` to use the baseline protocol.

### "Permission denied" on S3

- Verify the IAM role or access keys attached to Dremio have `s3:GetObject`, `s3:PutObject`, and `s3:ListBucket` on the bucket and prefix.
- For cross-account buckets, verify the bucket policy also grants access to the Dremio IAM principal.
- Check that the S3 endpoint region matches the bucket region (add `fs.s3a.endpoint` in **Connection Properties**).

### CTAS fails with "OutOfMemoryError"

- Reduce **Write Buffer Max Rows** (e.g., from 100000 to 25000).
- Reduce Dremio query memory limits if the job is allocating too much heap.
- Check if the source query returns an unexpectedly large result — add a `LIMIT` clause to test first.

### Schema mismatch on INSERT INTO

- If **Allow Schema Evolution** is disabled and the incoming data has extra columns, the INSERT will fail with a schema mismatch error.
- Either enable **Allow Schema Evolution** to auto-add new columns, or ensure the SELECT list in your INSERT matches the table schema exactly (no extra columns).
- If the table is missing columns that exist in the incoming data, use CTAS to recreate the table with the new schema.

### Slow reads on large tables

- Check that the **Default Partition Column** is set and the query is filtering on that column — missing the partition filter causes a full table scan.
- View the Dremio job profile (Jobs → [query] → Profile) and look for the Delta scan operator. The "files pruned" statistic shows how many Parquet files were skipped due to partition pruning.
- Increase **Read Batch Size** for better vectorized scan throughput.
- For non-partition column filters, verify that Parquet column statistics are present (not all writers populate `min`/`max` statistics). Dremio uses these for file-level pruning.
