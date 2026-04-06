# Dremio Apache Hudi Connector — End-to-End Test Results

*Run by Mark Shainman — 2026-04-05*

---

## Environment

| Component | Version / Details |
|---|---|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| Apache Hudi | 0.15.0 (`hudi-java-client`) |
| Connector JAR | `dremio-hudi-connector-1.0.0-SNAPSHOT-plugin.jar` (157MB) |
| Source names | `hudi_local` (COW, record key: `user_id`) |
| Test data root | `/test-data/hudi/` inside `try-dremio` Docker container |
| Query interface | Dremio v2 REST API (`POST /apiv2/sql`) |
| Run date | 2026-04-05 |

---

## Test Data

| Table | Schema | Rows | Type |
|---|---|---|---|
| `hudi_local.sample_users` | user_id INT · username TEXT · email TEXT · region TEXT · tier TEXT · created_at TIMESTAMP | 100 | COW (pre-existing) |
| `hudi_local.rw_enriched_users` | user_id INT · username TEXT · region TEXT · tier TEXT · event_count INT · total_spent DOUBLE | 100 | COW (written during test) |

Source also used for cross-connector tests: `delta_local.sample_events` (500 rows) and `delta_local.sample_users` (100 rows) as input sources.

---

## Results — 11 / 11 Passed ✅

### Group 1 — Basic Reads (existing table)

| Test | Query | Result | Value |
|---|---|---|---|
| 1.1 | `SELECT COUNT(*) FROM hudi_local.sample_users` | ✅ | 100 |
| 1.2 | `SELECT user_id, username, email, region, tier … ORDER BY user_id LIMIT 5` | ✅ | 5 rows; user_0001–user_0005 with correct email, region, tier |
| 1.3 | `SELECT region, COUNT(*) GROUP BY region ORDER BY cnt DESC` | ✅ | 5 regions; us-west(28), ap-south(22), ap-east(19), us-east(19), eu-west(12) |
| 1.4 | `SELECT region, tier, COUNT(*) GROUP BY region, tier` | ✅ | 19 distinct region/tier combinations; correct distribution |

---

### Group 2 — Cross-Source Write (Delta → Hudi)

| Test | Query | Result | Rows Written |
|---|---|---|---|
| 2.1 | `CREATE TABLE hudi_local.rw_enriched_users STORE AS (type => 'hudi') AS SELECT u.user_id, u.username, u.region, u.tier, COUNT(e.event_id), SUM(e.amount) FROM delta_local.sample_events e JOIN delta_local.sample_users u ON e.user_id = u.user_id GROUP BY …` | ✅ | 100 |
| 2.2 | `SELECT COUNT(*) FROM hudi_local.rw_enriched_users` (read-back COUNT) | ✅ | 100 |
| 2.3 | `SELECT … ORDER BY total_spent DESC LIMIT 5` (read-back top rows) | ✅ | 5; top spender user_0034/eu-west/$2,941.40 |
| 2.4 | `WHERE tier = 'enterprise' ORDER BY total_spent DESC LIMIT 5` (filtered read-back) | ✅ | 5; enterprise subset correct; user_0021/us-west/$2,539.38 |
| 2.5 | `AVG(total_spent) GROUP BY region ORDER BY avg_spend DESC` (aggregation on written data) | ✅ | 5 regions; eu-west avg $1,360.09 (matches Delta equivalent) |

---

### Group 3 — Cross-Connector: Hudi → Delta

| Test | Query | Result | Rows |
|---|---|---|---|
| 3.1 | `CREATE TABLE delta_local.output.hudi_to_delta STORE AS (type => 'delta_write') AS SELECT … FROM hudi_local.sample_users ORDER BY user_id` | ✅ | 100 |
| 3.2 | `SELECT COUNT(*) FROM delta_local.output.hudi_to_delta` | ✅ | 100 |
| 3.3 | `JOIN delta_local.output.hudi_to_delta h WITH delta_local.sample_events e ON h.user_id = e.user_id WHERE e.event_type = 'purchase' GROUP BY … ORDER BY delta_spend DESC LIMIT 5` | ✅ | 5; top spender user_0068/us-east/$950.85 (consistent with Delta-only result) |

---

## Summary

```
Total tests : 11
Passed      : 11
Failed      : 0
Errors      : 0
```

**Connector features exercised:**

| Feature | Tested in |
|---|---|
| Full table scan (Parquet via HudiParquetRecordReader) | 1.1–1.4 |
| Timeline-aware reads (stale pre-compaction files skipped) | 1.1–1.4 |
| Snapshot cache (60s TTL + invalidation after write) | Active for all reads |
| COUNT(*) | 1.1, 2.2, 3.2 |
| GROUP BY (single column) | 1.3 |
| GROUP BY (multi-column) | 1.4, 2.5 |
| ORDER BY + LIMIT | 1.2, 2.3, 3.3 |
| WHERE filter | 2.4 |
| Aggregation on written data | 2.5 |
| Cross-source CTAS (Delta → Hudi) | 2.1 |
| CTAS write (`STORE AS (type => 'hudi')`) | 2.1 |
| Read-back after CTAS | 2.2–2.5 |
| Cross-connector write (Hudi → Delta) | 3.1 |
| Cross-connector JOIN (written Delta data + Delta events) | 3.3 |
| Auto-discovery via `HudiFormatMatcher` (`.hoodie/` detection) | All read tests |

---

## Known Behaviour

**Record key field must be present in CTAS output:** `HudiRecordWriter` keys every Avro record using `defaultRecordKeyField` (configured as `user_id` on `hudi_local`). If the CTAS SELECT list does not include that column (e.g., a pure aggregation by `event_type, region`), the writer throws `AvroRuntimeException: Not a valid schema field: user_id` and the CTAS fails with no data written. Always include the configured record key column in the SELECT list.

**Post-CTAS table visibility:** Same as the Delta connector — the written folder requires a metadata promotion step before Dremio can query it by SQL name. Promotion: `PUT /apiv2/source/hudi_local/folder_format/{path}` with `{"type":"Parquet","isFolder":true}`.

**INSERT INTO not supported:** Dremio 26.x DML routes through the Iceberg-only planner. CTAS is the only supported write path. This is a Dremio platform constraint.

**MOR log merging:** `HudiMORRecordReader` is compiled and available but not wired into the default scan path. COW tables (the default) are fully supported. MOR compaction/clustering runs automatically after every N commits (default: 10) via `HudiRecordWriter.maybeRunAutoCompaction()`.
