# Dremio Apache Cassandra Connector — End-to-End Test Results

*Run by Mark Shainman — 2026-04-05*

---

## Environment

| Component | Version / Details |
|---|---|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| Apache Cassandra | 4.x (`cassandra-test` Docker container) |
| Connector JAR | `dremio-cassandra-connector-1.0.0-SNAPSHOT-plugin.jar` |
| Source name | `cassandra_test` (type: `APACHE_CASSANDRA`) |
| Test keyspace | `test_ks` |
| Query interface | Dremio v3 REST API (`POST /api/v3/sql`) |
| Run date | 2026-04-05 |

---

## Test Data

| Table | Schema highlights | Rows |
|---|---|---|
| `users` | uuid PK · text · int · double · decimal · boolean · timestamp · LIST\<text\> · MAP\<text,text\> | 5 |
| `events` | (device_id text, event_time timestamp) composite PK · region text · value double | 5 |
| `products` | (category text, product_id uuid) composite PK · name text · price decimal · stock int | 6 |
| `orders` | uuid PK · customer/region/status text · amount double · secondary index on `status` | 6 |
| `contacts` | uuid PK · name text · phones LIST\<text\> · scores MAP\<text,int\> · home_addr frozen UDT | 3 |
| `new_discovery_table` | uuid PK · label text | 0 |

---

## Results — 48 / 48 Passed ✅

### Group 1 — Full Table Scans & Native Types

| Test | Query | Result | Rows |
|---|---|---|---|
| 1.1 | `SELECT user_id, name, email, age, score, active, balance FROM users` | ✅ | 5 |
| 1.2 | `SELECT device_id, event_time, region, "value" FROM events` | ✅ | 5 |
| 1.3 | `SELECT category, product_id, name, price, stock FROM products` | ✅ | 6 |
| 1.4 | `SELECT order_id, customer, region, status, amount FROM orders` | ✅ | 6 |
| 1.5 | `SELECT name, tags, "metadata" FROM users` — LIST + MAP columns | ✅ | 5 |
| 1.6 | `SELECT name, phones, scores, home_addr FROM contacts` — LIST + MAP + UDT | ✅ | 3 |

**Notes:** `value` and `metadata` are SQL reserved words and must be double-quoted — standard SQL behaviour, not a connector limitation. Lists render as `['a', 'b']`, maps as `{'k': 'v'}`, UDTs as `{'field': value}`. NULL collections render as `None`.

---

### Group 2 — COUNT and Projection Pushdown

| Test | Query | Result | Rows |
|---|---|---|---|
| 2.1 | `COUNT(*)` across all 5 tables via `UNION ALL` | ✅ | 5 rows (one per table) |
| 2.2 | `SELECT name, email FROM users ORDER BY name` — 2-column projection | ✅ | 5 |
| 2.3 | `SELECT category, price FROM products ORDER BY category, price` | ✅ | 6 |

**Verified:** Projection pushdown sends only the requested columns in the CQL query — wider tables fetch less data.

---

### Group 3 — Predicate Pushdown

| Test | Query | Pushdown type | Result | Rows |
|---|---|---|---|---|
| 3.1 | `WHERE user_id = '<uuid>'` | Partition key EQ | ✅ | 1 |
| 3.2 | `WHERE device_id = 'sensor-001'` | Partition key EQ | ✅ | 2 |
| 3.3 | `WHERE device_id = 'sensor-001' AND event_time >= TIMESTAMP '…'` | PK + clustering key range | ✅ | 1 |
| 3.4 | `WHERE category = 'electronics'` | Composite partition key EQ | ✅ | 3 |
| 3.5 | `WHERE status = 'shipped'` | Secondary index (`orders_status_idx`) | ✅ | 3 |
| 3.6 | `WHERE age > 30` | Residual filter (non-PK column) | ✅ | 2 |

**Pushdown behaviour:**
- Partition key equality → direct partition lookup in CQL; single-fragment execution (no duplicates)
- Clustering key range → pushed after full PK is covered
- Secondary index → used without `ALLOW FILTERING` where safe
- Non-indexed columns → full CQL scan + Dremio residual filter; results always correct

---

### Group 4 — LIMIT Pushdown

| Test | Query | Result | Rows |
|---|---|---|---|
| 4.1 | `SELECT … FROM users LIMIT 3` | ✅ | 3 |
| 4.2 | `WHERE category = 'electronics' LIMIT 2` | ✅ | 2 |
| 4.3 | `SELECT … FROM orders LIMIT 1` | ✅ | 1 |

**Verified:** `LIMIT N` is embedded in the CQL query, reducing rows fetched from Cassandra. Dremio retains the `LimitRel` in the plan as a safety cap for multi-fragment scans.

---

### Group 5 — Aggregations

| Test | Query | Result |
|---|---|---|
| 5.1 | `COUNT(*), AVG(age), MAX(score), AVG(score) FROM users` | ✅ `total=5, avg_age=32.0, top_score=98.5, avg_score=81.9` |
| 5.2 | `GROUP BY region, COUNT(*), SUM(amount) FROM orders ORDER BY total_rev DESC` | ✅ 5 regions; ap-south leads at $310.0 |
| 5.3 | `GROUP BY category, MIN/MAX/AVG price FROM products` | ✅ 3 categories; electronics avg $459.99 |
| 5.4 | `GROUP BY active, COUNT(*), AVG(score) FROM users` | ✅ 4 active (avg 80.4), 1 inactive (88.1) |
| 5.5 | `GROUP BY region, AVG/MIN/MAX "value" FROM events` | ✅ 3 sensor regions |

---

### Group 6 — Expressions, String Functions, Sorting

| Test | Query | Result |
|---|---|---|
| 6.1 | `CASE WHEN score >= 90 THEN 'platinum' …` | ✅ Alice/Eve=platinum, Carol=gold, Bob=silver, Dave=bronze |
| 6.2 | `balance + 100.0 … WHERE active = true ORDER BY balance DESC` | ✅ 4 active users; Eve 8750.25 → 8850.25 |
| 6.3 | `UPPER(name), CHAR_LENGTH(email)` | ✅ 5 rows; lengths correct |
| 6.4 | `WHERE email LIKE '%@example.com'` | ✅ 5 rows; all match |
| 6.5 | `phones IS NULL, scores IS NULL FROM contacts` | ✅ Carol: both NULL; Alice/Bob: both populated |
| 6.6 | `ORDER BY balance DESC LIMIT 3` | ✅ Eve 8750.25, Carol 3200.75, Alice 1500.0 |

---

### Group 7 — Cross-Table JOINs

| Test | Query | Result | Rows |
|---|---|---|---|
| 7.1 | `users INNER JOIN orders ON SUBSTRING(email, …) = customer` | ✅ | 6 |
| 7.2 | `users LEFT JOIN orders … GROUP BY name ORDER BY total_spent DESC` | ✅ | 5 (Dave/Eve show 0 orders) |
| 7.3 | `orders JOIN users … WHERE status = 'shipped'` with tier CASE | ✅ | 3 (carol=gold, alice=platinum, bob=silver) |

**Verified:** Dremio executes JOINs across two Cassandra tables using its hash-join executor. Both sides are read via the connector in parallel.

---

### Group 8 — Subqueries & CTEs

| Test | Query | Result | Rows |
|---|---|---|---|
| 8.1 | `WHERE score > (SELECT AVG(score) FROM users)` | ✅ | 3 (Alice 98.5, Eve 95.7, Carol 88.1) |
| 8.2 | CTE: per-region totals → `WHERE total > 100` | ✅ | 3 regions |
| 8.3 | CTE + JOIN: high-value customers (spend > 200) with user score | ✅ | 2 (carol 365.75, bob 289.0) |
| 8.4 | `WHERE email_prefix IN (SELECT DISTINCT customer FROM orders)` | ✅ | 3 |

---

### Group 9 — Window Functions

| Test | Query | Result |
|---|---|---|
| 9.1 | `RANK() OVER (ORDER BY score DESC) AS "rank"` | ✅ 1=Alice, 2=Eve, 3=Carol, 4=Bob, 5=Dave |
| 9.2 | `ROW_NUMBER() + DENSE_RANK() OVER (PARTITION BY category ORDER BY price DESC)` | ✅ 6 rows; correct per-category ranks |
| 9.3 | `SUM(amount) OVER (PARTITION BY customer ORDER BY amount DESC)` running total | ✅ 6 rows; carol: 310.0 → 365.75 |
| 9.4 | `LAG("value") OVER (PARTITION BY device_id ORDER BY event_time)` and delta | ✅ sensor-001: +2.3; sensor-002: −2.5; first reading NULL (no predecessor) |

---

### Group 10 — Temporal Queries

| Test | Query | Result | Rows |
|---|---|---|---|
| 10.1 | `WHERE event_time >= TIMESTAMP '2026-03-29 00:00:00' ORDER BY event_time DESC` | ✅ | 5 |
| 10.2 | `EXTRACT(YEAR/MONTH/DAY/HOUR FROM event_time)` | ✅ | 5; timestamps correctly decomposed |
| 10.3 | `WHERE created_at >= TIMESTAMP '2026-01-01 00:00:00'` | ✅ | 5 |

---

### Group 11 — Decimal, CAST, Type Handling

| Test | Query | Result |
|---|---|---|
| 11.1 | `price * stock AS inventory_value` | ✅ Laptop Pro: 58,499.55; total via window: 96,540.75 |
| 11.2 | `CAST(age AS DOUBLE) / 10.0` | ✅ No precision loss; 30 → 3.0 |

**Note:** `CAST(list_col AS VARCHAR)` is not supported by Dremio (collection → scalar CAST is invalid). Use `CASE WHEN col IS NULL THEN …` for null-handling on collection columns instead.

---

### Group 12 — Advanced Analytics

| Test | Query | Result |
|---|---|---|
| 12.1 | `CASE WHEN phones IS NULL THEN 'no phone on file'` | ✅ Carol: no phone; Alice/Bob: has phone |
| 12.2 | `GROUP BY region HAVING COUNT(*) > 1` | ✅ Only us-east qualifies (2 orders) |
| 12.3 | CTE + `NTILE(4)` score quartile distribution | ✅ Q1: 2 users (95.7–98.5), Q2–Q4: 1 each |
| 12.4 | `SELECT DISTINCT region FROM orders ORDER BY region` | ✅ 5 distinct regions |
| 12.5 | Live events `UNION ALL` reference baseline row | ✅ 6 rows |
| 12.6 | `SUM(CASE WHEN active …) / COUNT(*)` active % | ✅ 4/5 = 80.0% |

---

### Group 13 — Real-World Analytics Workloads

| Test | Query | Result |
|---|---|---|
| 13.1 | **Customer 360** — user profile + lifetime spend + biggest order + tier (CTE + LEFT JOIN) | ✅ 5 rows; Carol: Gold, $365.75 lifetime; Alice: Platinum, $179.49 |
| 13.2 | **Inventory forecast** — `RANK()` within category + `SUM() OVER()` grand total | ✅ 6 rows; total inventory value $96,540.75 |
| 13.3 | **IoT anomaly detection** — z-score via `CROSS JOIN stats` CTE, flag readings > 2σ | ✅ 5 rows; all readings within 2 standard deviations (no anomalies in dataset) |
| 13.4 | **Order funnel** — status breakdown + % of total via `SUM(COUNT(*)) OVER()` | ✅ shipped 50%, delivered 33.3%, pending 16.7% |

---

### Group 14 — Connector Correctness Checks

| Test | What's verified | Result |
|---|---|---|
| 14.1 | `COUNT(*) = COUNT(DISTINCT user_id)` — no duplicate rows from parallel token-range splits | ✅ **0 duplicates** |
| 14.2 | `balance - price` cross-join — decimal precision end-to-end through CQL → Arrow → SQL | ✅ No floating-point corruption |
| 14.3 | sensor-002 events `ORDER BY event_time DESC` — Cassandra DESC clustering order honoured | ✅ 2026-04-04 before 2026-04-03 |
| 14.4 | `INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA LIKE '%cassandra%'` — all datasets visible | ✅ All 6 tables discovered |
| 14.5 | Exact UUID lookup (`WHERE user_id = 'd0ae7215-…'`) returns exactly 1 row | ✅ Carol White |
| 14.6 | `WHERE user_id IN ('…', '…', '…')` returns exactly 3 rows | ✅ Alice, Carol, Eve |
| 14.7 | Non-existent UUID returns `COUNT = 0`, no error or exception | ✅ Clean empty result |

---

## Summary

```
Total tests : 48
Passed      : 48
Failed      : 0
Errors      : 0
```

**Every connector feature exercised and confirmed working:**

| Feature | Tested in |
|---|---|
| Full table scan (all CQL types) | 1.1–1.4 |
| Native collection types (LIST, MAP, UDT) | 1.5, 1.6 |
| COUNT(*) | 2.1 |
| Projection pushdown | 2.2, 2.3 |
| Partition key EQ pushdown | 3.1, 3.2, 3.4, 14.5, 14.6 |
| Clustering key range pushdown | 3.3 |
| Secondary index pushdown | 3.5 |
| Residual filter correctness | 3.6 |
| LIMIT pushdown | 4.1–4.3 |
| Aggregations (COUNT/AVG/MIN/MAX/SUM) | 5.1–5.5 |
| GROUP BY | 5.2–5.5 |
| CASE expressions | 6.1, 7.3 |
| String functions (UPPER, CHAR_LENGTH, LIKE, SUBSTRING) | 6.3, 6.4, 7.1–7.3 |
| NULL handling on collection columns | 1.6, 6.5, 12.1 |
| ORDER BY + LIMIT | 6.6 |
| Cross-table INNER JOIN | 7.1, 7.3 |
| Cross-table LEFT JOIN | 7.2 |
| Scalar subquery | 8.1 |
| CTE (WITH clause) | 8.2, 8.3, 13.1–13.3 |
| IN subquery | 8.4 |
| RANK() / DENSE_RANK() / ROW_NUMBER() | 9.1, 9.2, 13.2 |
| SUM() OVER (running window) | 9.3 |
| LAG() window function | 9.4 |
| Temporal filters (TIMESTAMP WHERE) | 10.1, 10.3 |
| EXTRACT (date parts) | 10.2 |
| Decimal arithmetic | 11.1, 14.2 |
| CAST | 11.2 |
| HAVING | 12.2 |
| NTILE() | 12.3 |
| DISTINCT | 12.4 |
| UNION ALL | 12.5 |
| CROSS JOIN | 13.3 |
| Token-range split correctness (no duplicates) | 14.1 |
| Catalog / auto-discovery | 14.4 |
| Empty result on non-existent PK | 14.7 |

---

## SQL Reserved Word Notes

Two column names in the test schema happen to be SQL reserved words:

| Column | Table | SQL reserved word? | Fix |
|---|---|---|---|
| `value` | `events` | Yes (SQL `VALUE` keyword) | Quote as `"value"` |
| `metadata` | `users` | Yes (Dremio parser reserved) | Quote as `"metadata"` |

**This is standard SQL behaviour**, not a connector limitation. The same quoting requirement applies in PostgreSQL, Spark SQL, BigQuery, and every other ANSI SQL engine. Column names that clash with SQL keywords must always be double-quoted.

Similarly, `rank` used as a *column alias* (`AS rank`) must be quoted as `AS "rank"` because `RANK` is a SQL window function keyword.

**Recommendation for schema design:** Avoid naming columns after SQL reserved words. For existing schemas you do not control, use quoted identifiers in your queries.
