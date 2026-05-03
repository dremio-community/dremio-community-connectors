# Dremio InfluxDB Connector

*Built by Mark Shainman*

A native Dremio storage plugin that adds **read support for InfluxDB 3 Core and Enterprise** via the InfluxDB 3 HTTP SQL API. No JDBC driver required — pure Java 11 `java.net.http.HttpClient` with Bearer token authentication.

Dremio has no built-in InfluxDB connector. This plugin bridges that gap by exposing every measurement in a configured InfluxDB 3 database as a SQL table in the Dremio catalog, enabling time-series analytics alongside lakehouse data with standard SQL.

---

## Features

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ | SQL queries via InfluxDB 3 HTTP API |
| **Auto-discovery** | ✅ | Measurements listed via `information_schema.tables` |
| **Schema inference** | ✅ | Column types from `information_schema.columns` |
| **Pagination** | ✅ | SQL `LIMIT`/`OFFSET` for large measurements |
| **Bearer token auth** | ✅ | InfluxDB 3 API token |
| **InfluxDB 3 Core (OSS)** | ✅ | Local Docker or self-hosted |
| **InfluxDB 3 Enterprise** | ✅ | Cloud or on-prem |
| **Multiple databases** | ✅ | Add one source per database |
| **Cross-source JOIN** | ✅ | Join InfluxDB with Iceberg, DynamoDB, Salesforce, etc. |
| **WHERE / predicate pushdown** | ❌ | Dremio applies filters after fetch (v1) |
| **CTAS / INSERT INTO** | ❌ | Read-only |

> **Column naming note:** `time` and `value` are reserved words in Dremio SQL. Quote them with double-quotes in queries: `SELECT "time", "value" FROM influxdb.temperature`.

---

## Quick Install

```bash
# Docker — use the included pre-built JAR (no Maven needed)
./install.sh --docker try-dremio --prebuilt

# Bare-metal Dremio
./install.sh --local /opt/dremio --prebuilt

# Kubernetes pod
./install.sh --k8s dremio-0 --prebuilt

# Interactive (prompts for all options)
./install.sh
```

After restart, **InfluxDB** will appear under **Sources → +**.

---

## Local Testing with Docker

Start InfluxDB 3 Core locally:

```bash
docker run -d \
  --name influxdb3 \
  -p 8181:8181 \
  --user root \
  -v "$HOME/influxdb3-data:/var/lib/influxdb3/data" \
  influxdb:3-core \
  influxdb3 serve \
    --node-id host01 \
    --object-store file \
    --data-dir /var/lib/influxdb3/data

# Create admin token (shown only once — save it)
docker exec influxdb3 influxdb3 create token --admin

# Create a database
docker exec influxdb3 influxdb3 create database mydb --token YOUR_TOKEN

# Write test data
docker exec influxdb3 influxdb3 write \
  --database mydb \
  --token YOUR_TOKEN \
  'temperature,location=office temp=21.5,humidity=49.5'
```

Or use the included convenience scripts:

```bash
# Start InfluxDB via Docker Compose
docker compose up -d influxdb

# Provision token, databases, and test data
./setup-influxdb.sh
```

---

## Adding a Source

### Via Dremio UI

1. Go to **Sources → +** → select **InfluxDB**
2. Enter the Host URL (e.g. `http://localhost:8181`)
3. Enter the Database name
4. Paste the API token
5. Click **Save**

### Via Command Line

```bash
./add-influxdb-source.sh \
  --name influxdb_sensors \
  --host http://localhost:8181 \
  --database sensors \
  --token apiv3_xxx...
```

Non-interactive (CI/CD):
```bash
./add-influxdb-source.sh \
  --name influxdb_sensors \
  --host http://influxdb3:8181 \
  --database sensors \
  --token "$INFLUXDB_TOKEN" \
  --user dremio_admin \
  --password "$DREMIO_PASS"
```

---

## Upgrading Dremio

```bash
# Detect version + rebuild + redeploy — Docker
./rebuild.sh --docker try-dremio

# Force rebuild even if version matches
./rebuild.sh --docker try-dremio --force

# Bare-metal
./rebuild.sh --local /opt/dremio

# Preview changes only
./rebuild.sh --dry-run
```

---

## SQL Usage

> Quote `"time"` and `"value"` — they are reserved words in Dremio SQL.

```sql
-- All rows from a measurement
SELECT * FROM influxdb_sensors.temperature LIMIT 100;

-- Filter by tag
SELECT "time", "value", location
FROM influxdb_sensors.temperature
WHERE location = 'server_room'
ORDER BY "time" DESC
LIMIT 50;

-- Aggregation by tag
SELECT location, AVG("value") AS avg_temp, MAX("value") AS max_temp
FROM influxdb_sensors.temperature
GROUP BY location;

-- Time-range filter (Dremio applies after fetch)
SELECT "time", "value"
FROM influxdb_sensors.temperature
WHERE "time" >= TIMESTAMP '2026-01-01 00:00:00'
  AND "time" < TIMESTAMP '2026-02-01 00:00:00';

-- Cross-source JOIN: InfluxDB sensors + Iceberg asset registry
SELECT t."time", t."value" AS temp, a.asset_name, a.building
FROM influxdb_sensors.temperature t
JOIN iceberg_catalog.assets.registry a
  ON t.sensor_id = a.sensor_id
WHERE t."value" > 25.0;

-- Cross-source JOIN: InfluxDB metrics + DynamoDB config
SELECT m."time", m.host, m.usage_user, d.tier
FROM influxdb_metrics.cpu m
JOIN dynamodb_source.host_config d
  ON m.host = d.host_id
WHERE m.usage_user > 80;

-- Multiple InfluxDB sources (one source per database)
SELECT s."time", s."value" AS temp, m.usage_user AS cpu
FROM influxdb_sensors.temperature s
JOIN influxdb_metrics.cpu m
  ON s."time" = m."time";
```

---

## Architecture

```
SQL Query
  └── ScanCrel  (Dremio catalog scan)
        └── InfluxDBScanRule          [LOGICAL phase]
              └── InfluxDBScanDrel
                    └── InfluxDBScanPrule  [PHYSICAL phase]
                          └── InfluxDBScanPrel
                                └── getPhysicalOperator()
                                      └── InfluxDBGroupScan
                                            └── getSpecificScan()
                                                  └── InfluxDBSubScan
                                                        └── InfluxDBScanCreator
                                                              └── InfluxDBRecordReader
                                                                    └── SQL → HTTP API → Arrow batches
```

### Key Classes

| Class | Role |
|---|---|
| `InfluxDBConf` | Source config (host, database, token, pageSize). Shown in "Add Source" UI. |
| `InfluxDBStoragePlugin` | Plugin lifecycle, measurement listing, schema inference, split generation. |
| `InfluxDBConnection` | HTTP client — `POST /api/v3/query_sql` with Bearer auth. Lists measurements and columns via `information_schema`. |
| `InfluxDBScanSpec` | Carries measurement name through planning to execution. |
| `InfluxDBGroupScan` | Planning-layer scan; single-split for now (no parallelism in v1). |
| `InfluxDBSubScan` | JSON-serializable executor work unit. |
| `InfluxDBRecordReader` | Executes SQL, paginates via `LIMIT`/`OFFSET`, writes Arrow vector batches. |
| `InfluxDBRulesFactory` | Registers `LOGICAL` and `PHYSICAL` planner rule sets. |
| `InfluxDBScanRule` | `ScanCrel` → `InfluxDBScanDrel` (LOGICAL phase). |
| `InfluxDBScanPrule` | `InfluxDBScanDrel` → `InfluxDBScanPrel` (PHYSICAL phase). |
| `InfluxDBScanCreator` | Executor factory: maps `InfluxDBSubScan` → `InfluxDBRecordReader`. |

---

## InfluxDB 3 Type Mapping

| InfluxDB 3 Type | Arrow / Dremio Type | Notes |
|---|---|---|
| `Timestamp` | TIMESTAMP_MILLI | RFC3339 string or epoch nanos |
| `Int64` | INT64 | Integer fields |
| `Float64` | FLOAT64 | Float fields |
| `Boolean` | BIT | Boolean fields |
| `Utf8` | UTF8 | String fields |
| `Dictionary(Int32, Utf8)` | UTF8 | Tags (indexed string dimensions) |
| Unknown | UTF8 | Fallback |

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| `host` | `http://localhost:8181` | InfluxDB 3 host URL including scheme and port |
| `database` | _(required)_ | InfluxDB database name. Each database maps to one Dremio source. |
| `token` | _(required)_ | InfluxDB API token for Bearer authentication. Create with `influxdb3 create token --admin`. |
| `pageSize` | `1000` | Rows fetched per SQL round-trip via `LIMIT`/`OFFSET`. |
| `queryTimeoutSeconds` | `120` | HTTP request timeout. |

---

## InfluxDB 3 Data Model

InfluxDB 3 organises data as:

- **Database** → maps to one Dremio source
- **Measurement** → maps to one Dremio table
- **Tags** → string columns (indexed, appear as `Dictionary(Int32, Utf8)` in schema)
- **Fields** → value columns (`Float64`, `Int64`, `Boolean`, `Utf8`)
- **time** → always-present `Timestamp` column

Line protocol example:
```
temperature,location=office,sensor_id=S001 value=21.5,humidity=49.5 1746230400000000000
│           │                              │                          │
measurement  tags                           fields                     unix timestamp (ns)
```

---

## Requirements

- **Dremio OSS** 26.x (tested on 26.0.5)
- **InfluxDB** 3 Core (OSS) or 3 Enterprise
- **Java** 11+ (provided by the Dremio container)
- **Maven** 3.8+ (only required if building from source)

---

## References

- [InfluxDB 3 Core Documentation](https://docs.influxdata.com/influxdb3/core/)
- [InfluxDB 3 HTTP API Reference](https://docs.influxdata.com/influxdb3/core/reference/api/)
- [InfluxDB 3 SQL Reference](https://docs.influxdata.com/influxdb3/core/reference/sql/)
- [Line Protocol](https://docs.influxdata.com/influxdb3/core/reference/syntax/line-protocol/)
- [Dremio StoragePlugin Interface](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/StoragePlugin.java)
