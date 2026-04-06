# Dremio Apache Cassandra Connector — User Guide

Query Apache Cassandra keyspaces and tables directly from Dremio SQL. The connector translates SQL predicates into CQL (Cassandra Query Language) partition key filters where possible and reads data in parallel across all partitions.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a Cassandra Source in Dremio](#adding-a-cassandra-source-in-dremio)
   - [Connection](#connection)
   - [Credentials](#credentials)
   - [SSL / TLS](#ssl--tls)
   - [Advanced](#advanced)
5. [Writing Queries](#writing-queries)
6. [Understanding Cassandra Data in SQL](#understanding-cassandra-data-in-sql)
7. [Performance Considerations](#performance-considerations)
8. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector uses the DataStax Java driver to connect to Cassandra. It reads table schemas from Cassandra's system tables (`system_schema.keyspaces`, `system_schema.tables`, `system_schema.columns`) and exposes each keyspace as a Dremio schema and each table as a Dremio table.

At query time, the connector:
1. Translates SQL WHERE clauses on partition key columns into CQL token range queries.
2. Splits the full token range into `splitParallelism` slices and reads them in parallel.
3. Applies remaining filters (clustering key predicates, non-key predicates) in Dremio after reading.

Scans are always **full partition range** queries under the hood — Cassandra is not a SQL database and does not support arbitrary WHERE clauses efficiently. For best performance, design queries that filter on partition keys.

---

## Prerequisites

- Apache Cassandra 3.x or 4.x (or DataStax Enterprise 6.x)
- Dremio 26.x
- Network access from Dremio executor nodes to all Cassandra contact points on the native transport port (default 9042)
- A Cassandra user with `SELECT` permission on the keyspaces you want to query (or superuser access for development)

---

## Installation

```bash
cd dremio-cassandra-connector

# Interactive installer
./install.sh

# Non-interactive:
./install.sh --docker try-dremio --prebuilt   # pre-built JAR (recommended)
./install.sh --docker try-dremio --build       # build from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

After installation, restart Dremio. The "Apache Cassandra" source type will appear in **Sources → +**.

---

## Adding a Cassandra Source in Dremio

Navigate to **Sources → + → Apache Cassandra**. Fill in each section below.

---

### Connection

#### Contact Points
**Required.** Comma-separated list of Cassandra node hostnames or IP addresses that Dremio will use to bootstrap the cluster connection.

```
cassandra-node1.example.com,cassandra-node2.example.com,cassandra-node3.example.com
```

- You do not need to list every node — the driver discovers the full topology from the contact points.
- List 2–3 nodes for redundancy: if one is temporarily down during startup, the driver tries the others.
- For single-node development: `localhost`

#### Native Transport Port
The port Cassandra listens on for the native CQL binary protocol.

- Default: `9042`
- Change only if your Cassandra cluster uses a non-standard port (configured via `native_transport_port` in `cassandra.yaml`)
- This is **not** the JMX port (7199) or the Thrift port (9160)

#### Local Datacenter
The name of the datacenter where Dremio's executor nodes are located. Used by the DataStax driver's DC-aware round-robin load balancing policy.

- **Leave blank** to auto-detect. The driver will use the datacenter reported by the first contact point it successfully connects to.
- **Set explicitly** if auto-detection picks the wrong datacenter, or if you want to ensure all queries go to a specific datacenter.
- Find your datacenter name with: `SELECT data_center FROM system.local;` in `cqlsh`

Common names: `datacenter1` (default for single-DC), `us-east`, `dc-west`, `AWS_US_EAST_1`

#### Fallback Datacenters
Comma-separated list of datacenter names to use if the local datacenter is unavailable.

```
dc-west,dc-eu
```

- Leave blank if you have a single datacenter or do not want cross-DC fallback.
- The driver will only route to fallback DCs when all nodes in the local DC are unreachable.

---

### Credentials

#### Username
Cassandra username for authentication.

- Leave blank if Cassandra authentication is disabled (`authenticator: AllowAllAuthenticator` in `cassandra.yaml`)
- For production, create a dedicated read-only role:

```cql
CREATE ROLE dremio WITH PASSWORD = 'strong-password' AND LOGIN = true;
GRANT SELECT ON ALL KEYSPACES TO dremio;
```

#### Password
Password for the Cassandra user. Stored encrypted in Dremio's secret store.

---

### SSL / TLS

#### Enable SSL/TLS
When checked, the connector uses TLS for all connections to Cassandra.

Requires Cassandra to be configured with `client_encryption_options.enabled: true` in `cassandra.yaml`.

#### Truststore Path
Path to a JKS or PKCS12 truststore file containing the CA certificate used to verify the Cassandra node certificates.

```
/etc/dremio/certs/cassandra-truststore.jks
```

- The path must be accessible from **all Dremio executor nodes** (not just the coordinator).
- Leave blank to use the JVM default trust store.

#### Truststore Password
Password for the truststore file.

#### Truststore Type
Truststore file format: `JKS` (default) or `PKCS12`.

#### mTLS Keystore Path
Path to a JKS or PKCS12 keystore containing the **client's** private key and certificate.

Only required when Cassandra is configured to require client authentication (`require_client_auth: true`). Leave blank for standard one-way TLS.

#### mTLS Keystore Password / mTLS Keystore Type
Password and format for the mTLS keystore file.

#### TLS Hostname Verification
When checked *(default)*, the driver verifies that the Cassandra node's TLS certificate matches its hostname.

- **Uncheck** only for development environments where nodes use self-signed certificates with mismatched hostnames.
- Always leave checked in production.

---

### Advanced

#### Read Timeout (ms)
Maximum time to wait for a Cassandra response after sending a CQL query.

- Default: `30000` (30 seconds)
- Increase for large tables where a single page fetch takes longer than 30 seconds.
- Cassandra's server-side timeout (`read_request_timeout_in_ms`) must also be large enough.

#### Fetch Size (rows per page)
Number of rows fetched per CQL page. Cassandra returns results in pages; this controls the page size.

- Default: `1000`
- Larger values (5000–10000) reduce round-trips for large table scans but increase memory per executor.
- Smaller values reduce memory pressure for very wide rows (many columns or large text/blob values).

#### Excluded Keyspaces
Comma-separated list of keyspace names to hide from the Dremio catalog.

```
system,system_auth,system_schema,system_distributed,system_traces
```

- Default: empty (all keyspaces visible)
- Cassandra's internal keyspaces (`system*`) contain metadata tables not useful in Dremio — adding them here reduces catalog clutter.
- Patterns are exact name matches (not regex).

#### Consistency Level
The CQL read consistency level used for all queries.

| Level | Description |
|-------|-------------|
| `LOCAL_ONE` *(default)* | One replica in the local DC must respond. Fastest, lowest consistency guarantee. |
| `LOCAL_QUORUM` | Majority of replicas in the local DC must respond. Balanced performance and consistency. |
| `ONE` | Any single replica cluster-wide. May read stale data from remote DCs. |
| `QUORUM` | Majority of replicas cluster-wide. Cross-DC latency impacts performance. |
| `ALL` | All replicas must respond. Highest consistency, lowest availability. |
| `TWO` / `THREE` | 2 or 3 replicas must respond. |

For analytics workloads reading recent data, `LOCAL_QUORUM` provides a good balance. For best performance when eventual consistency is acceptable, use `LOCAL_ONE`.

#### Split Parallelism
Number of token range slices to divide the Cassandra ring into for parallel scanning.

- Default: `8`
- Each slice becomes a separate Dremio scan split, which can be assigned to different executor threads.
- Increase for large tables to improve scan throughput on multi-node clusters (e.g., 32 for a 12-node cluster).
- Decrease if you see too many small tasks in the Dremio job profile.
- Rule of thumb: `(Cassandra node count) × 2` to `× 4`.

#### Metadata Cache TTL (seconds)
How long to cache Cassandra keyspace and table metadata (schema) locally in Dremio.

- Default: `60` seconds
- Set to `0` to always fetch fresh metadata (useful after adding tables during development)
- After a schema change in Cassandra, right-click the source in Dremio UI → **Refresh Metadata**, or wait for the TTL to expire.

#### Enable Speculative Execution
When checked, the driver sends a second copy of a slow query to another node if the first node hasn't responded within the speculative execution delay.

- Default: disabled
- Useful for latency-sensitive workloads on clusters with occasional slow nodes (GC pauses, etc.)
- Increases load on the cluster — only enable if you have spare capacity and experience p99 latency issues.

#### Speculative Execution Delay (ms)
Time to wait before sending the speculative retry.

- Default: `500` ms
- Only relevant when **Enable Speculative Execution** is checked.
- Set to the p99 read latency of your cluster for best effect.

#### Protocol Compression
Network-level compression for the Cassandra native protocol wire format.

| Value | Description |
|-------|-------------|
| `NONE` *(default)* | No compression |
| `LZ4` | Fast compression. Recommended for high-bandwidth links where CPU is not the bottleneck. |
| `SNAPPY` | Similar to LZ4, slightly less efficient. Supported on older Cassandra versions. |

Enable compression when Dremio and Cassandra are on different machines and data is highly compressible (text, JSON). Leave as `NONE` when they're on the same host.

#### Async Page Prefetch
When checked *(default)*, the connector prefetches the next page from Cassandra while processing the current one.

- Improves throughput by hiding round-trip latency for multi-page scans.
- Disable only if you observe memory pressure issues during large scans.

---

## Writing Queries

### Basic queries

```sql
-- List all tables in a keyspace
SHOW TABLES IN cassandra.my_keyspace;

-- Select all rows from a table
SELECT * FROM cassandra.my_keyspace.users LIMIT 100;

-- Filter by partition key (most efficient — pushed to Cassandra as token range)
SELECT * FROM cassandra.my_keyspace.orders
WHERE user_id = 'user-123'
LIMIT 50;

-- Aggregation (executed in Dremio after reading)
SELECT status, COUNT(*) AS order_count
FROM cassandra.my_keyspace.orders
GROUP BY status;
```

### Working with Cassandra collections

Cassandra `list`, `set`, and `map` columns are exposed as Dremio ARRAY or struct types:

```sql
-- Flatten a list column
SELECT order_id, FLATTEN(tags) AS tag
FROM cassandra.ecommerce.orders;

-- Access map entries
SELECT user_id, preferences['theme'] AS theme_preference
FROM cassandra.user_profiles.settings;
```

### Joining Cassandra with other sources

```sql
-- Join Cassandra data with a CSV file loaded into Dremio
SELECT
    o.order_id,
    o.total_amount,
    c.email
FROM cassandra.ecommerce.orders o
JOIN dremio_storage."customers.csv" c ON o.user_id = c.id
WHERE o.created_date >= '2024-01-01';
```

### Time series queries

Cassandra is commonly used for time series data. Partition key usually includes a time bucket:

```sql
-- Query a time series table (partition key: sensor_id + date bucket)
SELECT sensor_id, event_time, temperature, humidity
FROM cassandra.iot.sensor_readings
WHERE sensor_id = 'sensor-42'
ORDER BY event_time DESC
LIMIT 1000;
```

---

## Understanding Cassandra Data in SQL

### Column types mapping

| Cassandra type | Dremio type |
|----------------|-------------|
| `uuid`, `timeuuid` | VARCHAR |
| `text`, `varchar`, `ascii` | VARCHAR |
| `int` | INT |
| `bigint`, `counter`, `varint` | BIGINT |
| `float` | FLOAT |
| `double` | DOUBLE |
| `decimal` | DECIMAL |
| `boolean` | BOOLEAN |
| `timestamp` | TIMESTAMP |
| `date` | DATE |
| `time` | TIME |
| `blob` | VARBINARY |
| `list<T>` | LIST |
| `set<T>` | LIST |
| `map<K,V>` | MAP |
| `tuple<…>` | STRUCT |
| `frozen<T>` | (same as T) |

### NULL handling

Cassandra does not distinguish between a `NULL` value and an absent column — both appear as `NULL` in Dremio. Tombstones (deleted values that haven't been compacted yet) are returned as `NULL`.

### Clustering order

Cassandra tables have a natural sort order defined by their clustering keys and clustering order (`ASC`/`DESC` in the `WITH CLUSTERING ORDER BY` clause). Dremio does not rely on this order and may return rows in any sequence unless you add an explicit `ORDER BY`.

---

## Performance Considerations

**Filter on partition keys** — Cassandra is optimized for partition key lookups. SQL `WHERE` clauses on partition key columns are translated into CQL token filters. Filters on non-key columns are applied by Dremio after reading and do not reduce the amount of data read from Cassandra.

**Tune Split Parallelism** — A higher value allows more Dremio executor threads to scan in parallel, improving throughput for large tables. Start with `Cassandra node count × 2`.

**Tune Fetch Size** — Larger page sizes reduce the number of round-trips. For wide rows, smaller sizes prevent memory exhaustion. The default of 1000 works well for typical schemas.

**Use `LOCAL_QUORUM` for analytics** — `LOCAL_ONE` may return stale data during node failures or compaction. `LOCAL_QUORUM` provides consistent results at a small latency cost.

**Excluded Keyspaces** — Add Cassandra's internal keyspaces (`system`, `system_auth`, `system_schema`, `system_distributed`, `system_traces`) to the exclusion list to prevent Dremio from scanning them during catalog refresh.

---

## Troubleshooting

### "No host available" or "All host(s) tried for query failed"

The driver cannot connect to any contact point.

**Check:**
```bash
# Test connectivity from the Dremio container:
docker exec try-dremio bash -c "nc -zv cassandra-host 9042"
```

**Common causes:**
- Wrong hostname or IP in Contact Points
- Firewall blocking port 9042
- Cassandra's `listen_address` / `rpc_address` not reachable from Dremio hosts (common in Docker where Cassandra binds to the container's internal IP)

### "Authentication failed" or "Unauthorized"

- Verify credentials with `cqlsh -u username -p password cassandra-host`
- Check that the user has SELECT permission: `LIST ALL PRIVILEGES OF dremio;` in cqlsh

### "No datacenter specified and the driver could not auto-detect"

- Set **Local Datacenter** explicitly. Find the name by running `SELECT data_center FROM system.local;` in cqlsh on any Cassandra node.

### Keyspaces not appearing in catalog

- Add Cassandra system keyspaces to **Excluded Keyspaces** and refresh metadata.
- Check **Metadata Cache TTL** — wait for it to expire or right-click source → **Refresh Metadata**.

### Queries are slow

- Check **Split Parallelism** — too low means the scan is not parallelized enough.
- Check **Fetch Size** — too small causes excessive round-trips.
- Confirm the Cassandra table's partition key columns are in the WHERE clause to avoid full-table scans.
- Check Cassandra's read latency metrics: `nodetool tpstats` or DataStax OpsCenter.

### SSL handshake failure

- Confirm Cassandra is configured with `client_encryption_options.enabled: true`
- Confirm the truststore contains the correct CA certificate
- If the node certificate's hostname doesn't match, uncheck **TLS Hostname Verification** for development, or fix the certificate for production
- Check Dremio server logs for `SSLHandshakeException` — it usually includes the specific mismatch reason
