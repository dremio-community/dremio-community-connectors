# Dremio Apache Hudi Connector — User Guide

Query and write Apache Hudi tables directly from Dremio SQL. The connector reads Hudi tables using the native Hudi reader (supporting both Copy-on-Write and Merge-on-Read table types) and writes new tables or inserts/upserts into existing ones.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a Hudi Source in Dremio](#adding-a-hudi-source-in-dremio)
   - [Root Path](#root-path)
   - [Default Table Type](#default-table-type)
   - [Default Record Key Field](#default-record-key-field)
   - [Default Partition Path Field](#default-partition-path-field)
   - [Default Precombine Field](#default-precombine-field)
   - [Write Parallelism](#write-parallelism)
   - [Target File Size](#target-file-size)
   - [Write Buffer Max Rows](#write-buffer-max-rows)
   - [Clustering Commit Interval](#clustering-commit-interval)
   - [Connection Properties](#connection-properties)
   - [Read Batch Size](#read-batch-size)
5. [Writing Queries](#writing-queries)
6. [Writing Data](#writing-data)
7. [Understanding Hudi Table Types](#understanding-hudi-table-types)
8. [Performance Considerations](#performance-considerations)
9. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector uses Apache Hudi's Java library to read and write Hudi tables stored on any filesystem Dremio can access (S3, HDFS, Azure Data Lake Storage, local NFS, etc.).

**Reading:** The connector resolves the latest Hudi snapshot (or a time-travel snapshot) by reading the Hudi timeline from the `.hoodie/` metadata directory. For Copy-on-Write tables it reads Parquet base files directly. For Merge-on-Read tables it merges base files with delta log files to produce the latest view.

**Writing:** CTAS (`CREATE TABLE … AS SELECT`) and `INSERT INTO` statements write new Hudi commits using Hudi's `HoodieJavaWriteClient`. Records are bucketed, sorted by record key, and written as Parquet base files. Compaction for MOR tables is handled automatically after a configurable number of commits.

Each table discovered under the **Root Path** is exposed as a Dremio dataset. The connector walks subdirectories looking for `.hoodie/` metadata directories.

---

## Prerequisites

- Apache Hudi 0.14.x tables on a filesystem accessible to Dremio executor nodes
- Dremio 26.x
- For S3: Dremio must be configured with AWS credentials or an IAM role for the bucket
- For HDFS: Dremio must be configured with the Hadoop cluster's `core-site.xml` / `hdfs-site.xml`

---

## Installation

```bash
cd dremio-hudi-connector

# Interactive installer
./install.sh

# Non-interactive:
./install.sh --docker try-dremio --prebuilt   # pre-built JAR (recommended)
./install.sh --docker try-dremio --build       # build from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

After installation, restart Dremio. The "Apache Hudi" source type will appear in **Sources → +**.

---

## Adding a Hudi Source in Dremio

Navigate to **Sources → + → Apache Hudi**. Fill in the fields below.

---

### Root Path

**Required.** The base filesystem path where your Hudi tables are stored. The connector scans this path recursively to discover tables.

```
s3://my-data-bucket/hudi-tables
hdfs:///data/hudi
/mnt/datalake/hudi
file:///tmp/hudi-dev
```

- Each subdirectory containing a `.hoodie/` metadata directory is treated as one Hudi table.
- Subdirectories without `.hoodie/` are treated as namespaces (Dremio schemas) for organizational purposes — they can contain more Hudi table subdirectories.
- For S3: Dremio must have `s3:GetObject` / `s3:ListBucket` permissions on this prefix.
- For HDFS: The path must be reachable from all Dremio executor nodes. Set `fs.defaultFS` in **Connection Properties** if needed.

**Example layout:**

```
s3://my-bucket/hudi-tables/
  orders/           ← Hudi table (has .hoodie/)
  customers/        ← Hudi table
  analytics/        ← sub-namespace
    events/         ← Hudi table
    sessions/       ← Hudi table
```

Dremio would expose: `hudi.orders`, `hudi.customers`, `hudi.analytics.events`, `hudi.analytics.sessions`.

---

### Default Table Type

The Hudi table type used when creating new tables via CTAS.

| Type | Description |
|------|-------------|
| **COPY_ON_WRITE** *(default)* | Writes rewrite the entire Parquet base file for affected partitions. Reads are fast (pure Parquet scan). Best for read-heavy workloads with infrequent updates. |
| **MERGE_ON_READ** | Writes append to columnar delta log files. Reads merge base + delta files. Best for write-heavy or near-real-time workloads where compaction runs asynchronously. |

- **Reading existing tables:** The table type is read from the Hudi metadata (`.hoodie/hoodie.properties`) and overrides this setting. This default only applies to new tables created via CTAS.
- See [Understanding Hudi Table Types](#understanding-hudi-table-types) for a detailed comparison.

---

### Default Record Key Field

The column name used as the unique record identifier for upsert operations.

- Default: `id`
- This is Hudi's `hoodie.datasource.write.recordkey.field` property.
- Hudi uses this field to identify which records to update or delete on upsert. If a new record has the same key as an existing record, the existing record is overwritten.
- Must be a non-null column that uniquely identifies each logical entity (e.g., `order_id`, `user_uuid`, `sensor_id`).
- For tables without meaningful primary keys (append-only event streams), set this to the same column as **Default Precombine Field** — duplicates will be resolved by the precombine comparison.
- For composite keys, use a comma-separated list: `order_id,line_item_id`

---

### Default Partition Path Field

The column name used to partition the Hudi table on disk. Leave blank for unpartitioned tables.

- This is Hudi's `hoodie.datasource.write.partitionpath.field` property.
- Partitioning physically organizes files by partition value, dramatically improving query performance for partition-prunable queries (e.g., `WHERE date = '2024-01-15'`).
- Common choices: `date`, `region`, `year`/`month`/`day` (date bucket), `country_code`
- For multi-level partitioning, use a comma-separated list: `year,month,day`
- Leave blank for small tables or tables where every query reads the full dataset.

**Partition path encoding:** By default, Hudi writes partition directories as `field=value` (Hive-style), e.g., `date=2024-01-15/`. This is compatible with Spark, Hive, and Dremio's partition pruning.

---

### Default Precombine Field

The column used to resolve duplicate records during upsert. When two records have the same record key, Hudi keeps the one with the **higher** value of this field.

- Default: `ts`
- This is Hudi's `hoodie.datasource.write.precombine.field` property.
- Typical choices: a timestamp column (`updated_at`, `event_time`, `ts`), a version number (`version`, `seq_no`), or any monotonically increasing value.
- For append-only tables where duplicates should not be deduplicated, set this to the same column as the record key.

---

### Write Parallelism

Number of write buckets (output partitions) used during a CTAS or INSERT write operation.

- Default: `2`
- Each bucket writes to one or more Parquet files in parallel. More buckets = more parallelism = faster writes on large datasets, but more small files.
- Rule of thumb: set to the number of Dremio executor cores available, or to `target output size / 128 MB`.
- For small datasets (< 1GB), the default of 2 is fine.
- For large CTAS operations (100GB+), increase to 16–64 to avoid a single-threaded bottleneck.

---

### Target File Size

Target size in bytes for each Parquet output file written by the connector.

- Default: `134217728` (128 MB)
- Hudi will write files close to this size. Smaller files = more files = better parallelism on read but more metadata overhead. Larger files = fewer files = better scan efficiency but less read parallelism.
- For object stores (S3, GCS, ADLS): 128–256 MB is the recommended range. Object stores perform best with fewer, larger files.
- For HDFS: Match your HDFS block size (typically 128 MB or 256 MB).
- Reduce to 32–64 MB for tables with very wide rows (many columns or large text/blob values) to prevent executor OOM during writes.

---

### Write Buffer Max Rows

Maximum number of rows to buffer in memory per write thread before flushing to disk.

- Default: `100000`
- Higher values = larger in-memory sort buffers = better file compaction and data locality, but higher memory usage per executor.
- Reduce if you see `OutOfMemoryError` during large CTAS operations (e.g., to 25000–50000).
- Increase for wide tables (many columns) that benefit from larger sort-merge windows.

---

### Clustering Commit Interval

Number of Hudi commits after which an automatic clustering (small-file compaction) operation is triggered.

- Default: `10`
- Set to `0` to disable automatic clustering entirely.
- Clustering reorganizes small files into larger ones, improving read performance over time for tables with frequent incremental writes. It does not change record content — only physical file organization.
- For MOR tables, this controls compaction frequency (merging delta log files into base files).
- For COW tables, this controls Z-order / linear clustering frequency.
- Lower values (e.g., 5) cluster more aggressively — better read performance, higher write overhead.
- Higher values (e.g., 50) cluster less frequently — lower write overhead, but small files accumulate.

---

### Connection Properties

Additional key-value properties passed to the Hudi/Hadoop filesystem layer. Separated by newlines or semicolons.

```
fs.s3a.endpoint=https://s3.us-west-2.amazonaws.com
fs.s3a.path.style.access=true
fs.s3a.connection.maximum=50
```

Common uses:

**S3-compatible storage (MinIO, Ceph, etc.):**
```
fs.s3a.endpoint=http://minio:9000
fs.s3a.path.style.access=true
fs.s3a.access.key=minioadmin
fs.s3a.secret.key=minioadmin
```

**HDFS with custom config:**
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

---

### Read Batch Size

Number of rows fetched per Arrow record batch during a table scan.

- Default: `4096`
- Larger values improve throughput for full-table scans at the cost of more memory per executor.
- Smaller values reduce memory pressure for tables with very wide rows or large binary columns.
- Values between 2048 and 16384 cover most use cases. The default of 4096 is a safe starting point.

---

## Writing Queries

### Basic queries

```sql
-- Discover tables under the source
SHOW TABLES IN hudi.orders_namespace;

-- Query the latest snapshot
SELECT * FROM hudi.orders LIMIT 100;

-- Filter by partition column (partition pruning is automatic)
SELECT order_id, status, total_amount
FROM hudi.orders
WHERE date = '2024-01-15'
  AND status = 'SHIPPED';

-- Count rows
SELECT COUNT(*) FROM hudi.customers;

-- Aggregation
SELECT region, SUM(total_amount) AS revenue
FROM hudi.orders
GROUP BY region
ORDER BY revenue DESC;
```

### Time travel queries

Hudi maintains a timeline of all commits. You can query any historical snapshot:

```sql
-- Query the table as of a specific timestamp (Hudi commit timestamp format)
SELECT * FROM hudi.orders
  AT TIMESTAMP '2024-01-15 00:00:00'
LIMIT 100;

-- Query as of a specific Hudi instant (from .hoodie/ timeline)
SELECT * FROM hudi.orders
  AT INSTANT '20240115120000000'
LIMIT 100;
```

### Joining Hudi tables

```sql
-- Join two Hudi tables
SELECT
    o.order_id,
    o.total_amount,
    c.email,
    c.tier
FROM hudi.orders o
JOIN hudi.customers c ON o.customer_id = c.customer_id
WHERE o.date >= '2024-01-01';

-- Join Hudi with another Dremio source
SELECT
    h.order_id,
    h.total_amount,
    pg.payment_method
FROM hudi.orders h
JOIN postgres.payments pg ON h.order_id = pg.order_id;
```

---

## Writing Data

### CTAS — Create a new Hudi table

```sql
-- Create a partitioned COW table from a query result
CREATE TABLE hudi.orders_2024 AS
SELECT order_id, customer_id, total_amount, status, date
FROM postgres.orders
WHERE date >= '2024-01-01';

-- The table is created using the source's configured defaults:
-- Table type: COPY_ON_WRITE (or MERGE_ON_READ if configured)
-- Record key: id (or your configured Default Record Key Field)
-- Partition path: date (or your configured Default Partition Path Field)
```

### INSERT INTO — Append or upsert rows

```sql
-- Append new rows (records with existing keys are upserted per Hudi semantics)
INSERT INTO hudi.orders
SELECT order_id, customer_id, total_amount, status, date
FROM staging.new_orders
WHERE ingestion_date = CURRENT_DATE;
```

Hudi automatically handles deduplication during INSERT based on the record key and precombine field configured for the source.

---

## Understanding Hudi Table Types

### Copy-on-Write (COW)

- **How writes work:** On each commit, Hudi rewrites the entire Parquet base file for every affected partition. The new file contains both unchanged and updated/inserted records.
- **How reads work:** Pure Parquet scan — no merge step. Reads are as fast as reading plain Parquet files.
- **Best for:** Read-heavy workloads, BI dashboards, batch ETL with infrequent large updates.
- **Trade-off:** Write amplification — even a single changed record rewrites the entire partition file.

### Merge-on-Read (MOR)

- **How writes work:** New and updated records are appended to columnar Avro delta log files (`.log` files) alongside existing Parquet base files. No existing data is rewritten on each commit.
- **How reads work:** Dremio merges base files with all delta logs at query time to reconstruct the latest view. This merge step adds some read overhead.
- **Best for:** Write-heavy workloads, near-real-time ingestion (streaming → Hudi), workloads where write latency matters more than read latency.
- **Compaction:** Periodically (controlled by **Clustering Commit Interval**), Hudi compacts delta logs back into base files, eliminating the merge overhead. After compaction, reads are as fast as COW.
- **Trade-off:** Read amplification between compaction cycles. Queries may be slower than COW if many delta logs have accumulated.

### Choosing the right type

| Factor | Choose COW | Choose MOR |
|--------|-----------|------------|
| Write frequency | Infrequent (daily, hourly) | Frequent (minutes, streaming) |
| Write latency tolerance | High | Low |
| Read performance priority | High | Medium (between compactions) |
| Data freshness requirement | Batch | Near real-time |
| Cluster write resources | Available | Limited |

---

## Performance Considerations

**Partition your tables** — Without partitioning, every query scans the entire table. Partition on the column most commonly used in `WHERE` clauses (usually a date or region column). Dremio pushes partition filters automatically.

**Match Target File Size to your storage** — For S3 and cloud object stores, target 128–256 MB files. Smaller files cause many small HTTP requests per scan, which is expensive on object stores. Use the **Clustering Commit Interval** to automatically compact small files.

**COW for read-heavy, MOR for write-heavy** — COW tables read as fast as native Parquet. MOR tables are better for high-frequency writes but add merge overhead on reads between compaction cycles.

**Tune Write Parallelism for large CTAS** — The default of 2 is a serial bottleneck for large datasets. For a 100GB CTAS, set Write Parallelism to 16 or more to use multiple executor threads.

**Increase Read Batch Size for wide scans** — The default 4096 rows per batch is conservative. For columnar analytics over narrow tables, increasing to 8192 or 16384 can improve throughput.

---

## Troubleshooting

### Tables not appearing in catalog

- Verify the **Root Path** is correct and accessible from Dremio executors.
- Each table directory must contain a `.hoodie/` subdirectory. Without it, the connector does not recognize the directory as a Hudi table.
- Right-click the source in Dremio UI → **Refresh Metadata**.
- Check Dremio server logs for filesystem access errors (permission denied, bucket not found).

### "FileNotFoundException" or "No such file or directory"

- Verify the Root Path value: confirm it exists and Dremio has read access.
- For S3: confirm the IAM role or access keys attached to Dremio have `s3:GetObject` and `s3:ListBucket` on the bucket and prefix.
- For HDFS: confirm the `fs.defaultFS` property in **Connection Properties** points to the correct NameNode.

### Queries return stale data after writes

- Hudi tables are versioned snapshots. Dremio caches the latest snapshot for catalog entries.
- Right-click the table in Dremio UI → **Refresh Metadata** to force a re-read of the Hudi timeline.
- For MOR tables: data written since the last compaction is stored in delta logs and is read on-the-fly — no metadata refresh needed for those records.

### CTAS fails with "OutOfMemoryError"

- Reduce **Write Buffer Max Rows** (e.g., from 100000 to 25000).
- Reduce **Write Parallelism** to limit concurrent write threads and their combined memory usage.
- Increase Dremio executor heap size if the table is genuinely large.

### Slow reads on MOR tables

- MOR tables merge base files with delta logs at query time. If many commits have accumulated since the last compaction, reads become slower.
- Trigger a compaction by reducing **Clustering Commit Interval** or running a manual compaction via Hudi's `HoodieCompactionAdminTool`.
- After compaction, query performance returns to COW-equivalent speed.

### "Invalid record key" or "Precombine field not found" on write

- Verify that the column specified in **Default Record Key Field** and **Default Precombine Field** exists in the data being written.
- Column names are case-sensitive in Hudi. Ensure the column name exactly matches the field names in the source data.
- For multi-column record keys, verify all columns in the comma-separated list exist.

### S3 connection issues

- Add the S3 endpoint explicitly in **Connection Properties**: `fs.s3a.endpoint=https://s3.amazonaws.com`
- For non-AWS S3-compatible stores, set `fs.s3a.path.style.access=true`
- Verify the bucket region matches the endpoint URL.
