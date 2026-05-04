# Dremio SingleStore Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read support for SingleStore databases**.

Uses Dremio's **ARP (Advanced Relational Pushdown)** framework — a declarative
YAML-driven approach where SQL operations (filters, aggregations, sorts, joins,
scalar functions) are pushed down to SingleStore automatically. The official
SingleStore JDBC driver (`singlestore-jdbc-client`) is bundled directly in the
plugin JAR — no separate driver deployment required.

---

## Architecture

```
SQL:  SELECT region, COUNT(*), SUM(total)
      FROM singlestore_source.mydb.orders
      GROUP BY region ORDER BY SUM(total) DESC

Dremio Planner (ARP)
  └── ArpDialect (singlestore-arp.yaml)
        ├── Predicate pushdown  → WHERE col = ? pushed to SingleStore SQL
        ├── Aggregation pushdown → GROUP BY, COUNT, SUM, AVG, MIN, MAX pushed down
        ├── Sort pushdown       → ORDER BY pushed down
        ├── Limit pushdown      → LIMIT / OFFSET pushed down
        └── Function mapping    → UPPER, SUBSTR, DATE_ADD, EXTRACT, … pushed down

SingleStoreConf (JdbcPluginConfig)
  └── SingleStoreDialect (ArpDialect)
        └── unparseDateTimeLiteral()  ← rewrites ANSI DATE/TIMESTAMP literals
                                         to plain string literals ('YYYY-MM-DD')
                                         which SingleStore accepts

JDBC → singlestore-jdbc-client (jdbc:singlestore://host:port/database)
  └── SingleStore server
```

### Key Classes

| Class | Role |
|---|---|
| `SingleStoreConf` | Source config shown in Dremio's "Add Source" UI. Extends `AbstractArpConf`. Defines host, port, database, auth, SSL, pool, and timeout fields. Hides system databases (`information_schema`, `memsql`, `cluster`). |
| `SingleStoreDialect` | Extends `ArpDialect`. Overrides `unparseDateTimeLiteral` to emit `'YYYY-MM-DD'` / `'YYYY-MM-DD HH:MM:SS'` instead of ANSI `DATE '...'` / `TIMESTAMP '...'` syntax that SingleStore (MySQL 5.7-compatible) rejects. |
| `singlestore-arp.yaml` | Full ARP dialect definition. Drives all type mappings, cast rewrites, and function pushdown. |

### Single-JAR Deployment

`singlestore-jdbc-client` has no ServiceLoader race condition, so the driver is
bundled directly inside the plugin fat JAR via `maven-shade-plugin`. One JAR, no extra steps.

---

## What's Implemented

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ Working | Full pushdown via ARP YAML |
| **Predicate pushdown** | ✅ Working | WHERE filters pushed to SingleStore |
| **Aggregation pushdown** | ✅ Working | COUNT, SUM, AVG, MIN, MAX pushed down |
| **GROUP BY pushdown** | ✅ Working | Single and multi-column GROUP BY |
| **ORDER BY + LIMIT** | ✅ Working | Pushed to SingleStore SQL |
| **JOIN pushdown** | ✅ Working | INNER, LEFT, CROSS JOIN |
| **WHERE IN / BETWEEN / LIKE** | ✅ Working | Pushed to SingleStore |
| **CASE WHEN** | ✅ Working | Pushed to SingleStore |
| **INTEGER / BIGINT / TINYINT** | ✅ Working | All mapped with SIGNED cast rewrite |
| **DECIMAL** | ✅ Working | Full precision/scale support |
| **FLOAT / DOUBLE** | ✅ Working | |
| **VARCHAR / TEXT / JSON** | ✅ Working | Mapped to VARCHAR |
| **BOOLEAN / TINYINT(1)** | ✅ Working | Mapped to BOOLEAN; cast rewrite to SIGNED |
| **DATE / DATETIME / TIMESTAMP** | ✅ Working | Date literals rewritten to string form |
| **EXTRACT** | ✅ Working | YEAR, MONTH, DAY, HOUR, MINUTE, SECOND |
| **TIMESTAMPDIFF** | ✅ Working | DAY, MONTH, YEAR, HOUR, MINUTE, SECOND |
| **DATE_ADD / DATE_SUB** | ✅ Working | INTERVAL arithmetic pushed down |
| **String functions** | ✅ Working | UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LENGTH, LPAD, RPAD, REPLACE, LOCATE |
| **Math functions** | ✅ Working | ABS, CEIL, FLOOR, ROUND, SQRT, POWER, LOG, MOD, SIGN |
| **SSL / TLS** | ✅ Working | Truststore path + password |
| **System schema hiding** | ✅ Working | `information_schema`, `memsql`, `cluster` hidden from catalog |
| **INSERT / UPDATE / DELETE** | ❌ Not supported | ARP is read-only |

---

## Quick Start

### 1. Install the connector

```bash
# Docker (default container: try-dremio)
./install.sh --docker try-dremio --prebuilt

# Bare-metal
./install.sh --local /opt/dremio --prebuilt

# Kubernetes
./install.sh --k8s dremio-master-0
```

### 2. Add a SingleStore source in Dremio

Open `http://localhost:9047` → **Sources → + → SingleStore** and fill in:

| Field | Value |
|---|---|
| Host | `localhost` or your SingleStore hostname |
| Port | `3306` |
| Database | `mydb` (leave blank to see all non-system databases) |
| Username | `root` |
| Password | your password |

### 3. Query

```sql
-- Basic read with filter pushdown
SELECT name, email, country
FROM singlestore.mydb.users
WHERE country = 'US' AND active = true;

-- Aggregation pushed to SingleStore
SELECT region, COUNT(*) AS orders, SUM(total) AS revenue
FROM singlestore.mydb.orders
GROUP BY region ORDER BY revenue DESC;

-- JOIN across tables
SELECT u.name, o.total, o.status
FROM singlestore.mydb.users u
JOIN singlestore.mydb.orders o ON u.user_id = o.user_id
WHERE o.status = 'delivered';

-- Date extraction
SELECT EXTRACT(YEAR FROM order_date) AS yr,
       EXTRACT(MONTH FROM order_date) AS mo,
       COUNT(*) AS cnt
FROM singlestore.mydb.orders
GROUP BY yr, mo ORDER BY yr, mo;

-- TIMESTAMPDIFF
SELECT order_id,
       TIMESTAMPDIFF(DAY, order_date, '2024-12-31') AS days_remaining
FROM singlestore.mydb.orders WHERE status = 'processing';

-- CASE expression
SELECT name,
       CASE WHEN score >= 90 THEN 'high'
            WHEN score >= 70 THEN 'mid'
            ELSE 'low' END AS tier
FROM singlestore.mydb.users ORDER BY score DESC;

-- Cross-source JOIN: SingleStore + Iceberg
SELECT u.name, u.email, i.segment
FROM singlestore.mydb.users u
JOIN iceberg_minio.analytics.segments i ON u.user_id = i.user_id;
```

---

## ARP Design Notes

### Date literal rewrite

SingleStore reports as MySQL 5.7-compatible and does not support ANSI SQL date
literal syntax (`DATE '2024-12-31'`). Dremio/Calcite generates these literals by
default. `SingleStoreDialect.unparseDateTimeLiteral()` rewrites them to plain
string literals (`'2024-12-31'`) which SingleStore accepts in date contexts.

### Catalog/schema namespace

Same as MariaDB: `supports_catalogs: true, supports_schemas: false`. SingleStore
databases are JDBC catalogs, not schemas. Without this, JDBC metadata calls are
unscoped and system tables from `memsql`/`information_schema` can contaminate
user table metadata.

### CAST rewrites

SingleStore follows MySQL's restricted CAST targets (SIGNED, UNSIGNED, CHAR,
DATE, DATETIME, DECIMAL, DOUBLE, FLOAT). The ARP YAML has explicit rewrites for
`integer → integer` → `CAST(x AS SIGNED)` and `boolean → boolean` → `CAST(x AS SIGNED)`.

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| Host | — | SingleStore hostname or IP |
| Port | 3306 | SingleStore port |
| Database | _(blank)_ | Default database. Leave blank to see all non-system databases. |
| Username | — | SingleStore username |
| Password | — | SingleStore password |
| Use SSL | false | Enable TLS encryption |
| SSL Truststore Path | _(blank)_ | Path to JKS truststore |
| SSL Truststore Password | _(blank)_ | Truststore password |
| Max Idle Connections | 8 | JDBC connection pool size |
| Connection Timeout | 30 s | Time to establish a TCP connection |
| Socket Timeout | 300 s | Time to wait for query results |
| Excluded Databases | _(blank)_ | Comma-separated database names to hide |
| Additional JDBC Properties | _(blank)_ | Extra `key=value` driver properties |

---

## Building

```bash
mvn clean package -DskipTests
# Deployable JAR:
target/dremio-singlestore-connector-1.0.0-SNAPSHOT-plugin.jar
```

---

## Smoke Tests

40 tests (13 standard + 27 SingleStore-specific) in the companion test harness:

```bash
cd dremio-connector-tests
python3 -m pytest connectors/test_singlestore.py -v
```

**Result: 40/40 tests passing.**

---

## References

- [SingleStore JDBC Driver](https://docs.singlestore.com/cloud/developer-resources/connect-with-application-development-tools/connect-with-java/jdbc-driver/)
- [SingleStore SQL Reference](https://docs.singlestore.com/cloud/reference/sql-reference/)
- [Dremio ARP Connector Documentation](https://docs.dremio.com/current/developer/arp-connector/)
