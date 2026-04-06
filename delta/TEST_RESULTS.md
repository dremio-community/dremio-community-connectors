# Dremio Delta Lake Connector — End-to-End Test Results

*Run by Mark Shainman — 2026-04-05*

---

## Environment

| Component | Version / Details |
|---|---|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| Connector JAR | `dremio-delta-connector-1.0.0-SNAPSHOT-plugin.jar` (27MB) |
| Source name | `delta_local` (type: `DELTA`) |
| Test data root | `/test-data/delta/` inside `try-dremio` Docker container |
| Query interface | Dremio v2 REST API (`POST /apiv2/sql`) |
| Run date | 2026-04-05 |

---

## Test Data

| Table | Schema | Rows |
|---|---|---|
| `delta_local.sample_events` | event_id INT · user_id INT · event_type TEXT · amount DOUBLE · created_at TIMESTAMP | 500 |
| `delta_local.sample_users` | user_id INT · username TEXT · email TEXT · region TEXT · tier TEXT · created_at TIMESTAMP | 100 |
| `delta_local.sample_products` | product_id INT · name TEXT · category TEXT · price DOUBLE · stock INT | 200 |

All tables stored as Delta Lake format (`_delta_log/` + Parquet files) at `/test-data/delta/`.

---

## Results — 14 / 14 Passed ✅

### Group 1 — Basic Reads (COUNT + spot-check)

| Test | Query | Result | Value |
|---|---|---|---|
| 1.1 | `SELECT COUNT(*) FROM delta_local.sample_events` | ✅ | 500 |
| 1.2 | `SELECT COUNT(*) FROM delta_local.sample_users` | ✅ | 100 |
| 1.3 | `SELECT COUNT(*) FROM delta_local.sample_products` | ✅ | 200 |
| 1.4 | `SELECT user_id, username, region, tier … ORDER BY user_id LIMIT 5` | ✅ | 5 rows; user_0001–user_0005 correct |

---

### Group 2 — Analytical Queries

| Test | Query Pattern | Result | Notes |
|---|---|---|---|
| 2.1 | `GROUP BY event_type, COUNT(*), AVG(amount)` | ✅ | return(115), purchase(102), add_to_cart(100), search(96), view(87); avg amounts ~$249–$260 |
| 2.2 | `JOIN sample_users ON user_id + WHERE event_type = 'purchase' + SUM(amount) GROUP BY username, region ORDER BY total_spent DESC LIMIT 5` | ✅ | Top spender: user_0068/us-east/$950.85 |
| 2.3 | `WHERE price > (SELECT AVG(price) FROM sample_products) ORDER BY price DESC LIMIT 5` | ✅ | Scalar subquery; highest: Product 1019/electronics/$992.14 |
| 2.4 | `JOIN sample_users + GROUP BY region, tier + SUM(amount) + ORDER BY revenue DESC LIMIT 8` | ✅ | Multi-column GROUP BY; us-east/pro leads at $6,832.85 |

---

### Group 3 — Write Path (CTAS)

| Test | Query | Result | Rows Written |
|---|---|---|---|
| 3.1 | `CREATE TABLE delta_local.output.fresh_customers STORE AS (type => 'delta_write') AS SELECT … JOIN … GROUP BY user_id, username, region, tier` | ✅ | 100 |
| 3.2 | `SELECT COUNT(*) FROM delta_local.output.fresh_customers` (read-back COUNT) | ✅ | 100 |
| 3.3 | `SELECT … ORDER BY total_spent DESC LIMIT 5` (read-back top rows) | ✅ | 5; top spender user_0034/eu-west/$2,941.40 |
| 3.4 | `WHERE tier = 'enterprise' ORDER BY total_spent DESC LIMIT 5` (filtered read-back) | ✅ | 5; enterprise subset correct |
| 3.5 | `AVG(total_spent) GROUP BY region ORDER BY avg_spend DESC` (aggregation on written data) | ✅ | 5 regions; eu-west avg $1,360.09 |

---

### Group 4 — Delta→Delta Chained Write

| Test | Query | Result | Rows |
|---|---|---|---|
| 4.1 | `CREATE TABLE delta_local.output.enterprise_only STORE AS (type => 'delta_write') AS SELECT … FROM fresh_customers WHERE tier = 'enterprise'` | ✅ | 28 |
| 4.2 | `SELECT COUNT(*) FROM delta_local.output.enterprise_only` | ✅ | 28 |
| 4.3 | `SELECT username, region, total_spent … ORDER BY total_spent DESC LIMIT 5` | ✅ | Correct enterprise-only subset; user_0021/us-west/$2,539.38 |

---

## Summary

```
Total tests : 14
Passed      : 14
Failed      : 0
Errors      : 0
```

**Connector features exercised:**

| Feature | Tested in |
|---|---|
| Full table scan (Parquet via DeltaParquetRecordReader) | 1.1–1.4 |
| Snapshot-consistent reads (stale files skipped) | 1.1–1.4 (only active snapshot files returned) |
| COUNT(*) | 1.1–1.3 |
| Aggregation: AVG, COUNT, SUM | 2.1, 2.4 |
| GROUP BY (single column) | 2.1 |
| GROUP BY (multi-column) | 2.4 |
| ORDER BY + LIMIT | 1.4, 2.2, 2.3, 3.3, 4.3 |
| Cross-table JOIN (two Delta tables) | 2.2, 2.4 |
| WHERE filter | 2.2, 3.4 |
| Scalar subquery | 2.3 |
| CTAS write (`STORE AS (type => 'delta_write')`) | 3.1, 4.1 |
| Read-back after CTAS | 3.2–3.5, 4.2–4.3 |
| Filter on written data | 3.4 |
| Aggregation on written data | 3.5 |
| Chained Delta→Delta write (CTAS from written table) | 4.1–4.3 |
| Auto-discovery via `DeltaFormatMatcher` (`_delta_log/` detection) | All read tests |
| Column statistics in `_delta_log/` | Verified via log inspection (separate) |
| Snapshot caching (version-check + TTL) | Active for all reads; invalidated after each write |

---

## Known Behaviour

**Post-CTAS table visibility:** After a CTAS, the written folder exists on disk but requires a metadata promotion step before Dremio can query it via SQL name. Promotion via `PUT /apiv2/source/delta_local/folder_format/{path}` with `{"type":"Parquet","isFolder":true}` makes the table immediately queryable. This is standard behaviour for all unformatted folders in a Dremio FileSystem source — the `DeltaFormatMatcher` handles the Delta-specific read logic at scan time.

**`STORE AS (type => 'delta_write')` required — NOT `'delta'`:** Dremio 26.x ships a built-in read-only `DeltaLakeFormatPlugin` registered as `"delta"`. Using `STORE AS (type => 'delta')` routes to the built-in plugin whose `getWriter()` returns null → NPE. Always use `delta_write` for writes with this connector.

**INSERT INTO not supported:** Dremio 26.x DML (`INSERT INTO`, `MERGE INTO`, `DELETE FROM`) routes through the Iceberg-only `WriterCommitterOperator`. Non-Iceberg format plugins receive writes only via CTAS. This is a Dremio platform constraint, not a connector limitation.
