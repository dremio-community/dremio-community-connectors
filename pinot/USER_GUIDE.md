# Apache Pinot Connector — User Guide

## How It Works

1. Dremio connects to the **Pinot Controller** REST API via the Apache Pinot JDBC driver.
2. Tables are discovered from Pinot's table registry and exposed in the Dremio catalog.
3. When you run a SQL query, Dremio's ARP planner translates it to Pinot SQL and pushes down as much as possible:
   - **Pushed down to Pinot**: aggregations (COUNT/SUM/AVG/MIN/MAX), WHERE filters, projections, ORDER BY, LIMIT
   - **Handled by Dremio**: JOINs (across Pinot and other sources), subqueries, window functions, UNION
4. Results stream back through the JDBC driver into Dremio's execution layer.

## Configuration Reference

### Connection

| Field | Default | Description |
|---|---|---|
| Controller Host | `localhost` | Hostname or IP of the Pinot Controller |
| Controller Port | `9000` | Pinot Controller REST API port |
| Username | _(blank)_ | Basic auth username; leave blank for unauthenticated clusters |
| Password | _(blank)_ | Basic auth password |

### Advanced Options

| Field | Default | Description |
|---|---|---|
| Use TLS / HTTPS | `false` | Enables `jdbc:pinot+ssl://` for encrypted connections |
| Broker List | _(blank)_ | Optional comma-separated broker addresses (host:port). When set, queries go directly to these brokers instead of being routed via the controller. |
| Record Fetch Size | `500` | Rows fetched per page from the JDBC driver |
| Maximum Idle Connections | `8` | JDBC connection pool size |
| Connection Idle Time (s) | `60` | Seconds before idle connections are closed |

## SQL Examples

```sql
-- Simple scan with filter
SELECT event_type, user_id, ts
FROM pinot_source.user_events
WHERE event_type = 'purchase'
  AND ts >= '2024-01-01 00:00:00'
LIMIT 1000;

-- Aggregation (pushed down to Pinot)
SELECT
  region,
  COUNT(*) AS event_count,
  SUM(revenue)  AS total_revenue,
  AVG(latency_ms) AS avg_latency
FROM pinot_source.transactions
WHERE status = 'COMPLETED'
GROUP BY region
ORDER BY total_revenue DESC
LIMIT 20;

-- JOIN Pinot with an Iceberg table (Dremio handles the join)
SELECT
  p.user_id,
  p.page_views,
  u.email
FROM pinot_source.page_view_counts p
JOIN iceberg_catalog.users u
  ON p.user_id = u.id
WHERE p.page_views > 100;

-- COUNT DISTINCT
SELECT COUNT(DISTINCT user_id) AS unique_users
FROM pinot_source.sessions
WHERE session_start >= '2024-01-01 00:00:00';
```

## Pushdown Behaviour

### What gets pushed to Pinot

- **Projections** (`SELECT col1, col2`) — only requested columns are fetched
- **Filters** (`WHERE col = val`, `WHERE col > val`, `WHERE col IN (...)`) — reduces data scanned
- **Aggregations** (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `COUNT DISTINCT`) — computed inside Pinot
- **GROUP BY** — grouping done inside Pinot
- **ORDER BY + LIMIT** — sorting and row cap applied at Pinot, not in Dremio

### What Dremio handles

- **JOINs** — Dremio fetches from both sides and performs the join in memory
- **Subqueries** — rewritten by Dremio's planner
- **Window functions** — computed by Dremio after fetching aggregated results
- **UNION / UNION ALL** — performed by Dremio

### Checking pushdown

Use `EXPLAIN PLAN` to see what was pushed:

```sql
EXPLAIN PLAN FOR
SELECT region, COUNT(*) FROM pinot_source.events GROUP BY region;
```

Look for `PinotScanRel` in the plan — that indicates a pushed-down scan.

## Data Type Mapping

| Pinot Type | Dremio Type |
|---|---|
| INT / INTEGER | INTEGER |
| LONG / BIGINT | BIGINT |
| FLOAT / DOUBLE | DOUBLE |
| BIG_DECIMAL | DOUBLE |
| BOOLEAN | BOOLEAN |
| STRING / VARCHAR | VARCHAR |
| JSON | VARCHAR |
| TIMESTAMP | TIMESTAMP |
| BYTES | VARBINARY |

## Pinot Multi-Value Columns

Pinot supports multi-value columns (arrays). The connector maps these to `VARCHAR` (serialized as a comma-separated string). Use Dremio's `SPLIT` function to further process them:

```sql
SELECT user_id, SPLIT(tags, ',') AS tag_list
FROM pinot_source.user_profiles;
```

## Connecting to Pinot Cloud / Managed Pinot

For StarTree Cloud or other managed Pinot services:
1. Set **Controller Host** to the managed endpoint
2. Set **Controller Port** to `443`
3. Enable **Use TLS / HTTPS**
4. Set **Username** and **Password** to your service credentials

## Troubleshooting

| Problem | Solution |
|---|---|
| `Connection refused` on port 9000 | Verify the Pinot controller is running and the port is open; check firewall rules |
| `Table not found` | Ensure the Pinot table exists and is not in an OFFLINE state |
| Slow queries | Add a WHERE filter to reduce the scan range; check if Pinot indexes cover your filter columns |
| `No suitable driver` | JAR not deployed correctly — run `./install.sh` to rebuild and redeploy |
| Auth error 401 | Verify username/password; check Pinot's access control configuration |
