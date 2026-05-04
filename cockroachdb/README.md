# Dremio CockroachDB Connector

A Dremio storage plugin that exposes [CockroachDB](https://www.cockroachlabs.com/) databases as SQL tables using Dremio's ARP (Advanced Relational Pushdown) framework.

CockroachDB is wire-compatible with PostgreSQL, so this connector uses the standard PostgreSQL JDBC driver — no CockroachDB-specific client needed.

## Features

- Full predicate, aggregation, sort, LIMIT, and JOIN pushdown (including FULL OUTER JOIN)
- Standard PostgreSQL JDBC driver bundled (no separate installation)
- Default port 26257 with optional SSL/TLS
- Hides system schemas (`crdb_internal`, `pg_catalog`, `pg_extension`, `information_schema`)
- 60+ pushed-down functions: EXTRACT, interval arithmetic, string functions, math, CASE
- Tables accessible as `source_name.public.table_name`

## Requirements

- Dremio 26.x (use `rebuild.sh` for other versions)
- CockroachDB 22.x or later (single-node or multi-region)

## Install

```bash
./install.sh --docker try-dremio --prebuilt   # Docker, pre-built JAR
./install.sh --docker try-dremio --build      # Docker, build from source
./install.sh --local /opt/dremio --prebuilt   # bare-metal
./install.sh --k8s dremio-0 --prebuilt        # Kubernetes
```

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| Host | `localhost` | CockroachDB host |
| Port | `26257` | CockroachDB SQL port |
| Database | `defaultdb` | Database to connect to |
| Username | `root` | SQL user |
| Password | _(empty)_ | SQL user password |
| Enable SSL | `false` | Enable TLS |
| SSL Mode | `require` | `require`, `verify-ca`, `verify-full`, `disable` |
| SSL Root Cert Path | _(empty)_ | Path to CA cert (in-container path) |
| Max Idle Connections | `8` | JDBC pool size |
| Connection Timeout | `30` | Seconds |
| Socket Timeout | `300` | Seconds |
| Excluded Schemas | _(empty)_ | Comma-separated schema names to hide |
| Additional JDBC Properties | _(empty)_ | Key=value pairs (one per line or semicolon-separated) |

## Example Queries

```sql
-- List tables
SELECT * FROM cockroach_source.public.users LIMIT 10;

-- Filter pushdown
SELECT name, country, score
FROM cockroach_source.public.users
WHERE country IN ('US', 'UK') AND score > 80
ORDER BY score DESC;

-- Aggregation pushdown
SELECT region, COUNT(*) AS orders, SUM(total) AS revenue
FROM cockroach_source.public.orders
GROUP BY region ORDER BY revenue DESC;

-- FULL OUTER JOIN (unique to CockroachDB vs MySQL-based connectors)
SELECT u.name, o.order_id, o.total
FROM cockroach_source.public.users u
FULL OUTER JOIN cockroach_source.public.orders o ON u.user_id = o.user_id;

-- Date extraction
SELECT EXTRACT(YEAR FROM order_date) AS yr,
       EXTRACT(MONTH FROM order_date) AS mo,
       COUNT(*) AS cnt
FROM cockroach_source.public.orders
GROUP BY yr, mo ORDER BY yr, mo;

-- DATE_TRUNC (computed in Dremio, not pushed down)
SELECT DATE_TRUNC('month', order_date) AS month_start, SUM(total)
FROM cockroach_source.public.orders
GROUP BY month_start;

-- Cross-source JOIN: CockroachDB + Iceberg
SELECT u.name, u.email, i.segment
FROM cockroach_source.public.users u
JOIN iceberg_catalog.analytics.segments i ON u.user_id = i.user_id;
```

## Known Quirks

- **DECIMAL aggregates:** PostgreSQL JDBC returns `precision=0` for aggregate result metadata on DECIMAL columns, which Dremio's Gandiva engine rejects. `SUM` and `AVG` on DECIMAL columns are not pushed down — Dremio computes them in-memory after fetching the raw values. Results are correct.
- **DATE_TRUNC:** With double-quote identifier quoting, Dremio's ARP SQL generator incorrectly converts the string literal `'month'` to the identifier `"month"`. DATE_TRUNC is therefore not pushed down; Dremio handles it in-memory.

## Building from Source

```bash
./rebuild.sh --docker try-dremio        # Detect Dremio version, rebuild, redeploy
./rebuild.sh --docker try-dremio --dry-run   # Preview version detection only
./rebuild.sh --force                    # Force rebuild even if version matches
```
