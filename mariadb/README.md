# Dremio MariaDB Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read support for MariaDB databases**.

Uses Dremio's **ARP (Advanced Relational Pushdown)** framework — a declarative
YAML-driven approach where SQL operations (filters, aggregations, sorts, joins,
scalar functions) are pushed down to MariaDB automatically. The official
MariaDB Connector/J JDBC driver is bundled directly in the plugin JAR — no
separate driver deployment required.

> **Why not use Dremio's built-in MySQL connector?**
> Dremio's MySQL connector uses `com.mysql.cj.jdbc.Driver` (MySQL Connector/J),
> which connects unreliably to MariaDB 10.6+ due to protocol divergence. This
> connector uses `org.mariadb.jdbc.Driver` (MariaDB Connector/J) with the native
> `jdbc:mariadb://` URL scheme — the correct driver for MariaDB.

---

## Architecture

```
SQL:  SELECT country, AVG(score), COUNT(*)
      FROM mariadb_source.testdb.users
      GROUP BY country ORDER BY COUNT(*) DESC

Dremio Planner (ARP)
  └── ArpDialect (mariadb-arp.yaml)
        ├── Predicate pushdown  → WHERE col = ? pushed to MariaDB SQL
        ├── Aggregation pushdown → GROUP BY, COUNT, SUM, AVG, MIN, MAX pushed down
        ├── Sort pushdown       → ORDER BY pushed down
        ├── Limit pushdown      → LIMIT / OFFSET pushed down
        └── Function mapping    → UPPER, SUBSTR, DATE_ADD, EXTRACT, … pushed down

MariaDBConf (JdbcPluginConfig)
  └── MariaDBDialect (ArpDialect)
        └── mariadb-arp.yaml: CAST rewrites, type mappings, function signatures

JDBC → mariadb-java-client (jdbc:mariadb://host:port/database)
  └── MariaDB server
```

### Key Classes

| Class | Role |
|---|---|
| `MariaDBConf` | Source config shown in Dremio's "Add Source" UI. Extends `AbstractArpConf`. Defines host, port, database, auth, SSL, pool, and timeout fields. Hides system schemas (`information_schema`, `performance_schema`, `mysql`, `sys`). |
| `MariaDBDialect` | Extends `ArpDialect`. Minimal — MariaDB uses standard ANSI SQL syntax with backtick quoting, so no literal-rewrite overrides are needed. |
| `mariadb-arp.yaml` | Full ARP dialect definition. Drives all type mappings, cast rewrites, and function pushdown. |

### Single-JAR Deployment

Unlike the ClickHouse connector (which requires a separate patched driver JAR),
MariaDB Connector/J has no ServiceLoader race condition. The driver is bundled
directly inside the plugin fat JAR via `maven-shade-plugin`. One JAR, no extra steps.

---

## What's Implemented

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ Working | Full pushdown via ARP YAML |
| **Predicate pushdown** | ✅ Working | WHERE filters pushed to MariaDB |
| **Aggregation pushdown** | ✅ Working | COUNT, SUM, AVG, MIN, MAX pushed down |
| **GROUP BY pushdown** | ✅ Working | Single and multi-column GROUP BY |
| **ORDER BY + LIMIT** | ✅ Working | Pushed to MariaDB SQL |
| **LIMIT + OFFSET** | ✅ Working | Pagination pushed down |
| **JOIN pushdown** | ✅ Working | INNER, LEFT, CROSS JOIN across tables in the same database |
| **WHERE IN / BETWEEN / LIKE** | ✅ Working | Pushed to MariaDB |
| **WHERE IS NULL / IS NOT NULL** | ✅ Working | Pushed to MariaDB |
| **CASE WHEN** | ✅ Working | Pushed to MariaDB |
| **INTEGER / BIGINT / TINYINT** | ✅ Working | All mapped to Dremio INTEGER/BIGINT with SIGNED cast rewrite |
| **DECIMAL** | ✅ Working | Full precision/scale support |
| **FLOAT / DOUBLE** | ✅ Working | FLOAT32/FLOAT64 |
| **VARCHAR / TEXT / JSON** | ✅ Working | All mapped to VARCHAR |
| **BOOLEAN / TINYINT(1)** | ✅ Working | Mapped to BOOLEAN; cast rewrite to SIGNED |
| **DATE** | ✅ Working | Full date type support |
| **DATETIME / TIMESTAMP** | ✅ Working | Mapped to TIMESTAMP |
| **EXTRACT (YEAR/MONTH/DAY/HOUR/…)** | ✅ Working | Full EXTRACT pushdown |
| **TIMESTAMPDIFF** | ✅ Working | DAY, MONTH, YEAR, HOUR, MINUTE, SECOND |
| **DATE_ADD / DATE_SUB** | ✅ Working | INTERVAL arithmetic pushed down |
| **String functions** | ✅ Working | UPPER, LOWER, CONCAT, SUBSTRING, TRIM, LENGTH, LPAD, RPAD, REPLACE, LOCATE |
| **Math functions** | ✅ Working | ABS, CEIL, FLOOR, ROUND, SQRT, POWER, LOG, MOD, SIGN |
| **SSL / TLS** | ✅ Working | Truststore path + password |
| **System schema hiding** | ✅ Working | `information_schema`, `performance_schema`, `mysql`, `sys` hidden from catalog |
| **INSERT / UPDATE / DELETE** | ❌ Not supported | ARP is read-only |

---

## Data Type Mapping

| MariaDB Type | Dremio Type | Notes |
|---|---|---|
| `TINYINT`, `SMALLINT`, `INT`, `INTEGER`, `MEDIUMINT` | `INTEGER` | CAST rewrite: `CAST(x AS SIGNED)` |
| `BIGINT` | `BIGINT` | |
| `TINYINT UNSIGNED`, …, `INT UNSIGNED` | `INTEGER` | Unsigned variants |
| `BIGINT UNSIGNED` | `BIGINT` | |
| `FLOAT` | `FLOAT` | |
| `DOUBLE`, `REAL` | `DOUBLE` | |
| `DECIMAL`, `NUMERIC` | `DECIMAL` | Full precision/scale |
| `BIT`, `BOOLEAN` | `BOOLEAN` | CAST rewrite: `CAST(x AS SIGNED)` |
| `TINYINT(1)` | `BOOLEAN` | Treated as boolean |
| `VARCHAR`, `CHAR`, `TEXT`, `MEDIUMTEXT`, `LONGTEXT`, `JSON` | `VARCHAR` | |
| `DATE` | `DATE` | |
| `DATETIME`, `TIMESTAMP` | `TIMESTAMP` | |
| `TIME` | `VARCHAR` | Returned as string |
| `BINARY`, `VARBINARY`, `BLOB` | `VARBINARY` | |
| `ENUM`, `SET` | `VARCHAR` | |

---

## ARP Design Notes

### Why `supports_catalogs: true, supports_schemas: false`

MariaDB's namespace model is: server → database → table. The MariaDB JDBC driver
exposes databases as JDBC **catalogs** (not schemas). The ARP YAML must match:

```yaml
syntax:
  supports_catalogs: true   # databases are JDBC catalogs
  supports_schemas: false   # no schema layer within a database
```

Using `supports_catalogs: false` causes JDBC metadata calls without a catalog
filter, allowing `performance_schema.users` columns to pollute `testdb.users`
metadata — causing DATA_READ errors on virtually every query.

### CAST rewrites for MariaDB

MariaDB's `CAST()` only accepts a limited set of type targets: `SIGNED`, `UNSIGNED`,
`CHAR`, `DATE`, `DATETIME`, `DECIMAL`, `DOUBLE`, `FLOAT`, `JSON`, `BINARY`.

Dremio/Calcite generates `CAST(x AS TINYINT)` for small integer literals and
`CAST(x AS BIT)` for boolean columns — both of which MariaDB rejects. The ARP
YAML adds explicit rewrites:

```yaml
- args: ["integer"]
  return: "integer"
  rewrite: "CAST({0} AS SIGNED)"

- args: ["boolean"]
  return: "boolean"
  rewrite: "CAST({0} AS SIGNED)"
```

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
./install.sh --k8s dremio-master-0 --namespace dremio-ns
```

### 2. Add a MariaDB source in Dremio

Open `http://localhost:9047` → **Sources → + → MariaDB** and fill in:

| Field | Value |
|---|---|
| Name | `mariadb` (or any label) |
| Host | `localhost` or your MariaDB hostname |
| Port | `3306` |
| Database | `mydb` (leave blank to see all databases) |
| Username | `root` (or your user) |
| Password | your password |

Click **Save** — MariaDB tables appear in the catalog.

### 3. Query

```sql
-- Basic read with filter pushdown
SELECT name, email, country
FROM mariadb.mydb.users
WHERE country = 'US' AND active = true;

-- Aggregation pushed to MariaDB
SELECT region, COUNT(*) AS orders, SUM(total) AS revenue
FROM mariadb.mydb.orders
GROUP BY region
ORDER BY revenue DESC;

-- JOIN across tables in the same database
SELECT u.name, o.total, o.status
FROM mariadb.mydb.users u
JOIN mariadb.mydb.orders o ON u.user_id = o.user_id
WHERE o.status = 'delivered'
ORDER BY o.total DESC;

-- Date extraction
SELECT EXTRACT(YEAR FROM order_date) AS yr,
       EXTRACT(MONTH FROM order_date) AS mo,
       COUNT(*) AS cnt
FROM mariadb.mydb.orders
GROUP BY yr, mo
ORDER BY yr, mo;

-- TIMESTAMPDIFF
SELECT order_id,
       TIMESTAMPDIFF(DAY, order_date, DATE '2024-12-31') AS days_remaining
FROM mariadb.mydb.orders
WHERE status = 'processing';

-- CASE expression
SELECT name,
       CASE WHEN score >= 90 THEN 'high'
            WHEN score >= 70 THEN 'mid'
            ELSE 'low' END AS tier
FROM mariadb.mydb.users
ORDER BY score DESC;

-- String functions
SELECT UPPER(country), CONCAT(name, ' — ', country) AS label,
       SUBSTRING(name, 1, 5) AS prefix
FROM mariadb.mydb.users
WHERE name LIKE '%Smith%';

-- Cross-source JOIN: MariaDB + Iceberg
SELECT u.name, u.email, i.segment
FROM mariadb.mydb.users u
JOIN iceberg_minio.analytics.segments i ON u.user_id = i.user_id;
```

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| Host | — | MariaDB server hostname or IP |
| Port | 3306 | MariaDB port |
| Database | _(blank)_ | Default database. Leave blank to see all non-system databases. |
| Username | — | MariaDB username |
| Password | — | MariaDB password |
| Use SSL | false | Enable TLS encryption |
| SSL Truststore Path | _(blank)_ | Path to JKS truststore for server cert verification |
| SSL Truststore Password | _(blank)_ | Truststore password |
| Max Idle Connections | 8 | JDBC connection pool size |
| Connection Timeout | 30 s | Time to establish a TCP connection |
| Socket Timeout | 300 s | Time to wait for query results. Increase for long analytical queries. |
| Excluded Databases | _(blank)_ | Comma-separated database names to hide from the catalog |
| Additional JDBC Properties | _(blank)_ | Extra `key=value` driver properties, one per line |

---

## Building

```bash
# Build from source (produces plugin JAR)
mvn clean package -DskipTests

# The deployable JAR is:
target/dremio-mariadb-connector-1.0.0-SNAPSHOT-plugin.jar
```

The fat JAR includes `mariadb-java-client-3.3.3.jar` — no separate driver deployment needed.

**Build note:** Dremio JDBC plugin JARs (`dremio-ce-jdbc-plugin`, `dremio-ce-jdbc-fetcher-api`)
must be installed into your local Maven repo from the running Dremio instance before building.
`install.sh --build` handles this automatically. For manual builds, find the JARs in
`$DREMIO_HOME/jars/` and use `mvn install:install-file`.

---

## Deployment

### Option A — Automated (recommended)

```bash
./install.sh                                    # guided interactive mode
./install.sh --docker try-dremio --prebuilt     # fast non-interactive deploy
./install.sh --docker try-dremio --build        # full build from source + deploy
```

The installer handles: build (optional), copy, deploy to `jars/3rdparty/`, restart, and health check.

### Option B — Manual

1. Copy the plugin JAR to `$DREMIO_HOME/jars/3rdparty/`:
   ```
   dremio-mariadb-connector-1.0.0-SNAPSHOT-plugin.jar
   ```
2. Restart Dremio.
3. **Sources → Add Source → MariaDB**
4. Fill in Host, Port (`3306`), Database, Username, Password.
5. Click **Save**.

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded, run `rebuild.sh` to detect the new version, update `pom.xml`,
rebuild against the new Dremio JARs, and redeploy:

```bash
./rebuild.sh                                    # Docker (default: try-dremio)
./rebuild.sh --docker my-dremio                 # Named Docker container
./rebuild.sh --local /opt/dremio                # Bare-metal
./rebuild.sh --k8s dremio-master-0              # Kubernetes
./rebuild.sh --dry-run                          # Preview only, no changes
```

---

## Known Limitations

### ARP is read-only
`INSERT INTO`, `UPDATE`, `DELETE`, and `CREATE TABLE AS SELECT` on MariaDB sources
are not supported. ARP provides read-only access.

### Cross-database JOINs
MariaDB supports cross-database queries (e.g. `JOIN other_db.table`) but Dremio's
ARP planner will typically execute them with Dremio-side hash joins rather than
pushing them to MariaDB. This is correct behaviour — results are accurate.

### TIME columns returned as VARCHAR
MariaDB `TIME` columns are returned as their string representation (`HH:MM:SS`).
Arithmetic on TIME values is not pushed down.

### Reserved words
Dremio/Calcite reserves several words as SQL keywords. Avoid column names like
`value`, `time`, `date`, `key` — or quote them with double-quotes in your queries:
```sql
SELECT "value", "time" FROM mariadb.mydb.events;
```

---

## Smoke Tests

A full suite of 40 smoke tests (13 standard base tests + 27 MariaDB-specific) is
included in the companion test harness:

```bash
cd dremio-connector-tests
python3 -m pytest connectors/test_mariadb.py -v
```

Test coverage: schema discovery, row counts, SELECT/PROJECT/WHERE/ORDER/GROUP/AGG/LIMIT/JOIN,
integer/decimal/boolean/date/datetime/text types, AVG/SUM/MIN/MAX, WHERE BETWEEN/IN/LIKE/IS NULL,
EXTRACT year/month, TIMESTAMPDIFF, UPPER/LOWER/CONCAT/SUBSTRING, CASE WHEN, events time-series.

**Result: 40/40 tests passing.**

---

## References

### Dremio ARP Framework
- [ARP Connector Documentation](https://docs.dremio.com/current/developer/arp-connector/)
- [AbstractArpConf.java](https://github.com/dremio/dremio-oss/blob/master/plugins/jdbc-plugin/src/main/java/com/dremio/exec/store/jdbc/conf/AbstractArpConf.java)
- [ArpDialect.java](https://github.com/dremio/dremio-oss/blob/master/plugins/jdbc-plugin/src/main/java/com/dremio/exec/store/jdbc/dialect/arp/ArpDialect.java)

### MariaDB
- [MariaDB Data Types](https://mariadb.com/kb/en/data-types/)
- [MariaDB Built-in Functions](https://mariadb.com/kb/en/built-in-functions/)
- [MariaDB Connector/J (JDBC Driver)](https://mariadb.com/kb/en/mariadb-connector-j/)
- [JDBC Driver Configuration](https://mariadb.com/kb/en/about-mariadb-connector-j/)
