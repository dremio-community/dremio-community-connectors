# Dremio ClickHouse Connector — User Guide

Query ClickHouse tables directly from Dremio SQL with full predicate, projection, and aggregation pushdown. Dremio translates SQL queries into native ClickHouse SQL and executes them on the ClickHouse server, returning only the result set to Dremio.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a ClickHouse Source in Dremio](#adding-a-clickhouse-source-in-dremio)
   - [General Tab — Connection](#general-tab--connection)
   - [General Tab — Credentials](#general-tab--credentials)
   - [Advanced Options — SSL / TLS](#advanced-options--ssl--tls)
   - [Advanced Options — Connection Pool](#advanced-options--connection-pool)
   - [Advanced Options — Performance](#advanced-options--performance)
   - [Advanced Options — Catalog](#advanced-options--catalog)
   - [Advanced Options — Advanced](#advanced-options--advanced)
5. [Writing Queries](#writing-queries)
6. [What Gets Pushed Down](#what-gets-pushed-down)
7. [Supported Data Types](#supported-data-types)
8. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector uses Dremio's **ARP (Advanced Relational Pushdown)** framework with the ClickHouse JDBC driver (`clickhouse-jdbc 0.4.6`). ARP translates Dremio's logical query plan into native ClickHouse SQL, sends it over HTTP/HTTPS, and streams the result back.

This means:
- Filters, projections, aggregations, joins, and LIMIT clauses are executed **on ClickHouse**, not in Dremio.
- Only result rows cross the network, not full table scans.
- ClickHouse's columnar storage and vectorized execution handle the heavy lifting.

The connector exposes every ClickHouse database (except excluded ones) as a Dremio schema, and every table and view within as a Dremio table.

---

## Prerequisites

- ClickHouse server 22.x or later (HTTP interface enabled on port 8123, or HTTPS on 8443)
- Dremio 26.x running in Docker, bare-metal, or Kubernetes
- Network access from Dremio executor nodes to the ClickHouse HTTP port
- A ClickHouse user with `SELECT` permission on the databases you want to expose

---

## Installation

```bash
cd dremio-clickhouse-connector

# Interactive installer
./install.sh

# Non-interactive:
./install.sh --docker try-dremio --prebuilt   # use included pre-built JAR
./install.sh --docker try-dremio --build       # compile from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

> **Important:** The connector requires **two JARs** in `jars/3rdparty/`:
> 1. `dremio-clickhouse-connector-*.jar` — the connector plugin
> 2. `clickhouse-jdbc-0.4.6-nospi.jar` — the JDBC driver (included in `jars/`)
>
> `install.sh` deploys both automatically.

After installation, restart Dremio. The "ClickHouse" source type will appear in **Sources → +**.

---

## Adding a ClickHouse Source in Dremio

### Option A — Script (recommended)

```bash
# Simplest — local ClickHouse with defaults
./add-clickhouse-source.sh --name clickhouse --host localhost

# Named database + credentials
./add-clickhouse-source.sh \
  --name clickhouse_prod \
  --host clickhouse.example.com \
  --database analytics \
  --ch-user dremio_reader \
  --ch-pass mypassword

# ClickHouse Cloud (auto-sets port 8443 + SSL)
./add-clickhouse-source.sh \
  --name ch_cloud \
  --host abc123.us-east-1.aws.clickhouse.cloud \
  --cloud \
  --ch-pass "$CH_CLOUD_PASSWORD"

# Self-hosted HTTPS
./add-clickhouse-source.sh \
  --name clickhouse_ssl \
  --host clickhouse.example.com \
  --port 8443 --ssl \
  --ssl-truststore /certs/truststore.jks --ssl-ts-pass tspass

# Preview the JSON payload (dry-run)
./add-clickhouse-source.sh --name clickhouse --host localhost --dry-run
```

Run `./add-clickhouse-source.sh --help` for the full list of options covering all 16 configuration fields.

### Option B — Dremio UI

Navigate to **Sources → + → ClickHouse**. The dialog has two tabs: **General** and **Advanced Options**.

---

### General Tab — Connection

#### Host
**Required.** Hostname or IP address of the ClickHouse server.

```
clickhouse.example.com     # production hostname
192.168.1.100              # IP address
localhost                  # local development
```

For ClickHouse Cloud: `<instance-id>.<region>.aws.clickhouse.cloud`

#### Port
**Required.** ClickHouse HTTP interface port.

- Default: `8123` (plain HTTP)
- Use `8443` when SSL is enabled

> ClickHouse's native TCP port (9000/9440) is **not** used — the connector communicates via HTTP/HTTPS.

#### Database
The default database to connect to. This affects which database is used when no schema prefix is specified in queries.

- Default: `default`
- You can still query tables in other databases using `database.table` notation regardless of this setting

#### ClickHouse Cloud
Check this box when connecting to **ClickHouse Cloud** (managed service).

When enabled, the connector applies Cloud-specific JDBC settings:
- Forces HTTPS (port 8443)
- Sets appropriate connection parameters for ClickHouse Cloud's load balancer behavior

If you're using a self-hosted ClickHouse, leave this unchecked.

---

### General Tab — Credentials

#### Username
ClickHouse username for authentication.

- Default: `default` (ClickHouse's built-in superuser)
- For production, create a dedicated read-only user with access only to the databases you need

#### Password
Password for the ClickHouse user. Stored encrypted in Dremio's secret store.

To create a read-only Dremio user in ClickHouse:
```sql
CREATE USER dremio_user IDENTIFIED BY 'strong-password';
GRANT SELECT ON my_database.* TO dremio_user;
```

---

### Advanced Options — SSL / TLS

#### Enable SSL / TLS
When checked, the connector uses HTTPS to communicate with ClickHouse.

- Change **Port** to `8443` when enabling SSL
- ClickHouse Cloud requires SSL — enabling this is mandatory for Cloud connections

#### SSL Trust Store Path
Path to a JKS or PKCS12 truststore file containing the CA certificate used to verify ClickHouse's TLS certificate.

```
/etc/dremio/certs/clickhouse-truststore.jks
```

- Leave blank to use the JVM's default trust store (works for publicly trusted certificates including ClickHouse Cloud)
- The path must be accessible from all Dremio executor nodes

#### SSL Trust Store Password
Password for the truststore file. Leave blank if the truststore has no password.

---

### Advanced Options — Connection Pool

#### Max Idle Connections
Maximum number of idle JDBC connections to keep open per Dremio node.

- Default: `8`
- Range: 1–100
- Increase if you see connection acquisition delays under heavy query load
- Decrease to reduce resource usage on ClickHouse server

#### Connection Timeout (seconds)
Maximum time to wait when establishing a new JDBC connection to ClickHouse.

- Default: `30` seconds
- Range: 1–600
- Increase for ClickHouse Cloud or cross-region connections with higher latency

#### Socket Timeout (seconds)
Maximum time to wait for data after a query has started executing.

- Default: `300` seconds (5 minutes)
- Range: 1–3600
- Increase for long-running analytical queries that return large result sets
- For ClickHouse queries that run longer than 5 minutes, increase this value

---

### Advanced Options — Performance

#### Enable HTTP Result Compression
When checked, ClickHouse compresses the HTTP response body using LZ4 before sending it to Dremio.

- Default: enabled
- Reduces network transfer for large result sets with compressible data
- Disable only if CPU on the ClickHouse server is the bottleneck (decompression overhead)

#### Fetch Block Size (rows)
Number of rows fetched per JDBC block from ClickHouse.

- Default: `65536` rows
- Range: 1024–1,000,000
- Larger values reduce round-trips for queries that return many rows, but use more memory
- Smaller values reduce memory pressure for very wide rows

---

### Advanced Options — Catalog

#### Excluded Databases
Comma-separated list of ClickHouse database names to hide from the Dremio catalog.

```
system,information_schema,INFORMATION_SCHEMA
```

- Default: empty (all databases visible)
- `system` contains ClickHouse internal tables (logs, metrics, settings) — typically not useful in Dremio
- Use this to reduce catalog clutter and avoid exposing internal databases to Dremio users

---

### Advanced Options — Advanced

#### Additional JDBC Properties
Extra key=value pairs appended to the JDBC connection URL, separated by `&`.

```
compress=1&socket_timeout=600000
```

Use this for ClickHouse-specific settings not covered by the fields above. Refer to the [ClickHouse JDBC driver documentation](https://github.com/ClickHouse/clickhouse-java) for supported parameters.

Common uses:
- `http_connection_provider=HTTP_URL_CONNECTION` — use standard Java HTTP instead of Apache HttpClient
- `custom_http_headers=X-ClickHouse-User:my-user` — pass custom headers

---

## Writing Queries

### Basic queries

```sql
-- Query a ClickHouse table
SELECT * FROM clickhouse.my_database.events LIMIT 100;

-- Count rows
SELECT COUNT(*) FROM clickhouse.my_database.orders;

-- Filter and project
SELECT order_id, customer_name, total_amount
FROM clickhouse.my_database.orders
WHERE status = 'SHIPPED'
  AND order_date >= '2024-01-01'
ORDER BY order_date DESC
LIMIT 1000;
```

### Aggregations

```sql
-- Revenue by month
SELECT
    toStartOfMonth(order_date) AS month,
    COUNT(*) AS order_count,
    SUM(total_amount) AS revenue
FROM clickhouse.sales.orders
GROUP BY month
ORDER BY month;

-- Top 10 customers by spend
SELECT customer_id, SUM(total_amount) AS total_spend
FROM clickhouse.sales.orders
GROUP BY customer_id
ORDER BY total_spend DESC
LIMIT 10;
```

### Joining ClickHouse with other sources

```sql
-- Join ClickHouse events with a Dremio-managed table
SELECT
    e.user_id,
    e.event_type,
    u.email,
    e.created_at
FROM clickhouse.analytics.events e
JOIN my_catalog.users u ON e.user_id = u.id
WHERE e.created_at >= '2024-01-01'
LIMIT 10000;
```

### CTAS — Create Table As Select

```sql
-- Materialize a ClickHouse query result into Dremio's storage
CREATE TABLE my_space.clickhouse_summary AS
SELECT
    region,
    product_category,
    SUM(revenue) AS total_revenue
FROM clickhouse.sales.orders
GROUP BY region, product_category;
```

---

## What Gets Pushed Down

The ARP framework pushes the following operations to ClickHouse:

| Operation | Pushed down |
|-----------|-------------|
| Column projection (SELECT cols) | ✅ Yes |
| WHERE filters (=, !=, <, >, LIKE, IN, IS NULL) | ✅ Yes |
| ORDER BY | ✅ Yes |
| LIMIT / OFFSET | ✅ Yes |
| GROUP BY | ✅ Yes |
| COUNT, SUM, MIN, MAX, AVG | ✅ Yes |
| JOIN (when both sides are ClickHouse tables) | ✅ Yes |
| DISTINCT | ✅ Yes |
| Subqueries | ✅ Yes |
| String functions (UPPER, LOWER, SUBSTRING, etc.) | ✅ Yes |

When Dremio joins a ClickHouse table with a table from a different source, the ClickHouse portion is still pushed down as a subquery, and Dremio handles the cross-source join.

---

## Supported Data Types

| ClickHouse type | Dremio type |
|-----------------|-------------|
| `UInt8`, `Int8` | INT |
| `UInt16`, `Int16` | INT |
| `UInt32`, `Int32` | INT |
| `UInt64`, `Int64` | BIGINT |
| `Float32` | FLOAT |
| `Float64` | DOUBLE |
| `Decimal(p,s)` | DECIMAL |
| `String`, `FixedString` | VARCHAR |
| `Date` | DATE |
| `DateTime`, `DateTime64` | TIMESTAMP |
| `UUID` | VARCHAR |
| `Array(T)` | LIST |
| `Nullable(T)` | Nullable T |
| `LowCardinality(T)` | T |
| `Enum8`, `Enum16` | VARCHAR |

---

## Troubleshooting

### Source shows "bad state" — connection refused

**Check:** Is the ClickHouse HTTP port accessible?

```bash
# From the Dremio container:
docker exec try-dremio curl -s "http://clickhouse-host:8123/?query=SELECT+1"
# Expected: 1
```

**Common causes:**
- Wrong port. ClickHouse uses HTTP port 8123, not the native TCP port 9000.
- SSL mismatch — ClickHouse expects HTTPS but connector is using HTTP (or vice versa). If SSL is enabled in the connector, change the port to 8443.
- Firewall blocking the HTTP port.

### "Authentication failed" or "Access denied"

- Verify username and password in ClickHouse directly: `clickhouse-client -u dremio_user --password secret -q "SELECT 1"`
- Check that the user has `SELECT` permission on the target database: `SHOW GRANTS FOR dremio_user`

### Tables not appearing in catalog

- The **Excluded Databases** list may be too broad. Check if the database containing your tables is excluded.
- Right-click the source in Dremio UI → **Refresh Metadata** to trigger a catalog rescan.

### Queries timeout with "socket timeout"

- Increase **Socket Timeout** in Advanced Options. For long-running aggregations, 600+ seconds may be needed.
- Check ClickHouse server logs for `Query execution time exceeded maximum` — this is a ClickHouse-side timeout, not a connector timeout. Adjust `max_execution_time` in ClickHouse settings.

### ClickHouse Cloud connection fails

1. Ensure **ClickHouse Cloud** checkbox is checked.
2. Ensure **Enable SSL / TLS** is checked and port is `8443`.
3. Username is typically `default` and password is the Cloud instance password.
4. The hostname format is `<instance-id>.<region>.aws.clickhouse.cloud`.

### Slow queries despite pushdown

- Enable query profiling in Dremio to verify pushdown is happening: **Jobs → [query] → Profile** — look for "ClickHouse" scan operator with filter pushdown shown.
- Ensure the relevant ClickHouse table has appropriate indexes (MergeTree ORDER BY keys, materialized views).
- Try increasing **Fetch Block Size** to reduce round-trips.
