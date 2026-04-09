# Dremio Amazon DynamoDB Connector — User Guide

Query Amazon DynamoDB tables directly from Dremio SQL. The connector uses the
DynamoDB Query API for partition key lookups and Parallel Scan for full-table reads,
pushing predicates, projections, and limits to DynamoDB where possible.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a DynamoDB Source](#adding-a-dynamodb-source)
5. [Writing Queries](#writing-queries)
6. [Understanding DynamoDB Data in SQL](#understanding-dynamodb-data-in-sql)
7. [Performance Considerations](#performance-considerations)
8. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector uses the AWS SDK for Java v2 to access DynamoDB. At source startup it
lists all DynamoDB tables and exposes them directly as Dremio tables (no keyspace/schema
level — DynamoDB tables are at the top level of the source).

At query time, the connector:

1. **Checks for a partition key EQ predicate.** If `WHERE pk = value` is present, it
   issues a DynamoDB **Query** — a targeted lookup that reads only the matching
   partition. This is significantly cheaper than a full Scan.
2. **If a sort key predicate is also present** (`AND sk > value`), adds it as a
   `KeyConditionExpression` range condition.
3. **Pushes remaining predicates** as a DynamoDB `FilterExpression`, reducing the data
   transferred from DynamoDB to Dremio.
4. **Pushes column projections** as a `ProjectionExpression`, requesting only the
   columns that SQL actually needs.
5. **Falls back to Parallel Scan** when no partition key predicate is present,
   splitting the table into `splitParallelism` segments read concurrently.

Schema is inferred by sampling up to `sampleSize` items from each table. The inferred
schema is cached for `metadataCacheTtlSeconds` seconds.

---

## Prerequisites

- Amazon DynamoDB (any region) or `amazon/dynamodb-local` for development
- Dremio 26.x
- Network access from Dremio executor nodes to DynamoDB (or DynamoDB Local endpoint)
- AWS credentials with at least `dynamodb:Scan`, `dynamodb:Query`, `dynamodb:DescribeTable`,
  and `dynamodb:ListTables` permissions — or leave credentials blank to use the default
  credential chain (IAM role, instance profile, environment variables)

---

## Installation

```bash
cd dremio-dynamodb-connector

# Interactive installer
./install.sh

# Non-interactive:
./install.sh --docker try-dremio --prebuilt   # pre-built JAR (recommended)
./install.sh --docker try-dremio --build       # build from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

After installation, restart Dremio. The "Amazon DynamoDB" source type will appear
in **Sources → +**.

---

## Adding a DynamoDB Source

Navigate to **Sources → + → Amazon DynamoDB**. Fill in each section below.

### Connection

#### AWS Region
The AWS region where your DynamoDB tables are located (e.g. `us-east-1`, `eu-west-2`).
Required even if you set an endpoint override.

#### Endpoint Override
Leave blank to connect to the real AWS DynamoDB endpoint.
For DynamoDB Local: `http://dynamodb-local:8000` (Docker) or `http://localhost:8000` (local).

### Credentials

#### Access Key ID / Secret Access Key
Leave both blank to use the default AWS credential chain:
IAM role → instance profile → environment variables (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`)
→ `~/.aws/credentials`.

Set explicitly for static credentials (not recommended for production).

### Advanced

#### Schema Sample Size (default: 100)
DynamoDB is schemaless. The connector infers a fixed Arrow schema by scanning up to
this many items from each table. Larger values improve accuracy at the cost of slower
metadata refresh. The schema is cached — see Schema Cache TTL.

#### Split Parallelism (default: 4)
Number of parallel DynamoDB Scan segments per table. DynamoDB's Parallel Scan
divides the table into `splitParallelism` non-overlapping segments, each read by a
separate Dremio executor fragment. Higher values increase throughput on large tables.

| Table size | Recommended |
|---|---|
| < 10 GB | `2` – `4` |
| 10–100 GB | `4` – `16` |
| > 100 GB | `16` – `64` |

#### Read Timeout (seconds) (default: 30)
AWS SDK timeout for individual DynamoDB API calls. Increase for very large result
pages or high-latency connections.

#### Page Size (default: 1000)
Maximum number of items per DynamoDB Scan/Query response page. Each page is one
network round-trip. Reduce if individual items are large (DynamoDB's 1 MB page limit
applies regardless of this setting).

#### Schema Cache TTL (seconds) (default: 60)
How long to cache the inferred Arrow schema per table. During the TTL, schema
inference is skipped and the cached schema is reused. Set to `0` to always re-sample
(slower but always reflects the current item shape).

---

## Writing Queries

### Basic reads

```sql
-- Full table scan — all rows and columns
SELECT * FROM dynamodb_source.users LIMIT 100;

-- Projection pushdown — only user_id, name, email sent in DynamoDB request
SELECT user_id, name, email FROM dynamodb_source.users;
```

### Partition key EQ → DynamoDB Query (efficient)

When `WHERE` provides equality on the **partition key column**, the connector uses
the DynamoDB **Query API** instead of Scan. Query reads only the target partition —
much cheaper and faster.

```sql
-- Single partition key
SELECT * FROM dynamodb_source.users WHERE user_id = 'u1';

-- With projection — only these columns fetched from DynamoDB
SELECT name, email FROM dynamodb_source.users WHERE user_id = 'u1';
```

> **How to find the partition key:** look at your table in the DynamoDB console
> under "Table details → Primary key". The connector infers it during metadata refresh
> and stores it in Dremio's namespace — you don't need to declare it.

### Partition key + sort key range → KeyConditionExpression

For tables with a **composite key** (partition key + sort key), adding a sort key
predicate alongside the partition key EQ lets the connector push both into a single
`KeyConditionExpression` — only matching items are read.

```sql
-- orders: PK=order_id, SK=user_id
-- Returns all orders for order_id='o1' where user_id >= 'u2'
SELECT * FROM dynamodb_source.orders
WHERE order_id = 'o1' AND user_id >= 'u2';

-- Exact sort key value
SELECT * FROM dynamodb_source.orders
WHERE order_id = 'o1' AND user_id = 'u1';
```

Supported sort key operators: `=`, `<`, `<=`, `>`, `>=`.

> **Tip:** The sort key pushdown only fires when there is also a PK EQ predicate.
> Without a PK EQ, the query falls back to Scan + FilterExpression.

### Non-key filter → FilterExpression

Non-key predicates are pushed as a DynamoDB `FilterExpression`. DynamoDB evaluates
the filter server-side, but the full matching partition is still read (Scan still
touches all items; filter reduces what's returned across the wire).

```sql
SELECT * FROM dynamodb_source.users WHERE country = 'US';
SELECT * FROM dynamodb_source.products WHERE in_stock = true AND price < 100;
```

### LIMIT pushdown

LIMIT is sent to DynamoDB as a page-level limit, reducing the total items DynamoDB
reads before stopping.

```sql
SELECT * FROM dynamodb_source.orders LIMIT 10;
```

### Aggregations

Aggregations are computed by Dremio's engine after fetching the data.

```sql
SELECT country, COUNT(*) AS cnt, AVG(score) AS avg_score
FROM dynamodb_source.users
GROUP BY country
ORDER BY cnt DESC;
```

### Cross-source JOINs

```sql
-- DynamoDB ↔ DynamoDB
SELECT u.name, o.total
FROM dynamodb_source.users u
JOIN dynamodb_source.orders o ON u.user_id = o.user_id;

-- DynamoDB ↔ S3 Parquet
SELECT d.name, p.tier
FROM dynamodb_source.users d
JOIN s3_source.reports.user_tiers p ON d.user_id = p.user_id;
```

---

## Understanding DynamoDB Data in SQL

### Type mapping

| DynamoDB type | Arrow / Dremio SQL type | Notes |
|---|---|---|
| S (String) | VARCHAR | |
| N (Number) | BIGINT | if all sampled values are integers |
| N (Number) | DOUBLE | if any sampled value has a decimal point |
| BOOL | BOOLEAN | |
| B (Binary) | VARBINARY | |
| SS (String Set) | ARRAY\<VARCHAR\> | Each element is a separate array entry |
| NS (Number Set) | ARRAY\<DOUBLE\> | Each element is a separate array entry |
| L (List) | VARCHAR | Serialized as JSON |
| M (Map) | VARCHAR | Serialized as JSON |
| BS (Binary Set) | VARCHAR | Serialized as JSON |
| NULL | null | Arrow null / SQL NULL |
| (missing attribute) | null | DynamoDB items don't need all attributes |

### Schema inference and evolution

DynamoDB tables are schemaless — each item can have different attributes. The connector
infers a **fixed** Arrow schema by sampling the first `sampleSize` items. Attributes
not present in the sample are not included in the schema; attributes that appear in
some items but not others will have null values for items that omit them.

If your table schema evolves (new attributes added), set **Schema Cache TTL** to `0`
and perform a metadata refresh to force re-sampling.

### String Sets and Number Sets

SS and NS fields are returned as Arrow List arrays. In Dremio SQL you can use
`FLATTEN` to unnest them:

```sql
-- Get all tags for each catalog item (one row per tag)
SELECT item_id, f.tag
FROM dynamodb_source.catalog
CROSS JOIN UNNEST(tags) AS f(tag);
```

### Nested data (Lists and Maps)

DynamoDB L (list) and M (map) types are serialized as JSON strings in Dremio.
Use Dremio's `CONVERT_FROM` function to parse them if needed:

```sql
SELECT item_id,
       CONVERT_FROM(nested_field, 'json') AS parsed
FROM dynamodb_source.my_table;
```

---

## Performance Considerations

### Use partition key predicates

The single biggest performance optimization is providing a partition key EQ predicate.
This routes to the DynamoDB **Query API** and reads only a single partition:

```sql
-- Efficient: Query API, reads one partition
SELECT * FROM dynamodb_source.orders WHERE order_id = 'o42';

-- Expensive: Parallel Scan, reads every partition
SELECT * FROM dynamodb_source.orders WHERE status = 'pending';
```

### Tune Split Parallelism

For large tables (> 10 GB), increase `splitParallelism`. Each segment becomes a
separate Dremio executor fragment reading from DynamoDB concurrently. The effective
parallelism is bounded by the number of Dremio executor nodes.

### Use LIMIT for exploratory queries

Always include `LIMIT` when exploring data — DynamoDB charges for every item read.

```sql
SELECT * FROM dynamodb_source.large_table LIMIT 100;
```

### Schema Cache TTL

Leave the default (60 seconds) for most workloads. Setting it to `0` re-samples
on every metadata refresh, which adds a `Scan(limit=sampleSize)` call per table
per refresh cycle.

### Page Size

The default page size of 1000 items works well for most tables. For tables with very
wide items (many attributes or large values), DynamoDB may return fewer items per page
due to the 1 MB page limit — reducing Page Size won't help here.

---

## Troubleshooting

**Tables don't appear in catalog**
- DynamoDB tables must exist before the names refresh runs
- Trigger a refresh by temporarily reducing `namesRefreshMillis` (see INSTALL.md)
- Or query a specific table path — PREFETCH_QUERIED mode promotes it on first query

**Partition key filter not using Query mode**
- Verify the partition key name is stored in metadata (requires at least one metadata
  refresh after source creation)
- The predicate must be equality (`=`), not a range (`>`, `LIKE`, etc.)
- Check the Dremio job profile — look for DynamoDBQuery vs DynamoDBScan operator

**ALL columns returned even with SELECT on subset**
- If the projection contains very few columns, the connector may not push it
  (DynamoDB has limits on ProjectionExpression with reserved words)
- Non-pushed projections are filtered by Dremio after fetching

**Schema doesn't include expected columns**
- The column wasn't present in any of the sampled items
- Increase `sampleSize` or set `metadataCacheTtlSeconds=0` and refresh metadata
- Manually refresh: Sources → your_source → ⚙ → Refresh Metadata

**SELECT returns fewer rows than expected**
- Check if a `LIMIT` clause or split-level limit was applied
- For Parallel Scan: verify all segments complete — check the Dremio job profile
  for fragment counts

**Source shows "bad" state**
- Verify DynamoDB is reachable from Dremio: `docker exec try-dremio curl -s http://dynamodb-local:8000`
- Verify credentials are correct (or use IAM / instance profile)
- Check `docker logs try-dremio 2>&1 | grep -i dynamodb`
