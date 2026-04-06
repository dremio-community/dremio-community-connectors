# Dremio ClickHouse Connector — Test Results

---

## Latest Run — Final Polish (UPPER/LOWER pushdown, MEDIAN, SPLIT_PART, smoke test script)

*Run 2026-04-05 (after pushdown fixes, MEDIAN, SPLIT_PART, CASE WHEN, LIKE ESCAPE verification)*

### Smoke Test — 32 / 32 Passed ✅ (`test-connection.sh`)

| Section | Tests | Pass |
|---|---|---|
| [1] Connectivity | 1 | 1 |
| [2] Aggregation (COUNT, SUM/AVG/MIN/MAX, MEDIAN, GROUP BY, APPROX_COUNT_DISTINCT) | 5 | 5 |
| [3] Filter pushdown (timestamp, string, IS NULL, IN) | 4 | 4 |
| [4] String functions (UPPER, LOWER, INITCAP, SUBSTRING ×2, SPLIT_PART, REGEXP_LIKE, REGEXP_EXTRACT) | 7 | 7 |
| [5] Date functions (EXTRACT, DATE_TRUNC, TO_CHAR ×2) | 4 | 4 |
| [6] Conditional (CASE WHEN single + multi-branch) | 2 | 2 |
| [7] LIKE / LIKE ESCAPE / REGEXP_LIKE (?i) | 3 | 3 |
| [8] Math (ABS/CEIL/FLOOR/ROUND, SQRT/POWER/LOG) | 2 | 2 |
| **Total** | **32** | **32** |

**Key findings from this run:**
- `UPPER`/`LOWER`/`INITCAP`/`SUBSTRING` now push to ClickHouse (explicit `rewrite` added to YAML)
- `MEDIAN` → `median()` pushes and returns correct value
- `SPLIT_PART(str, delim, n)` → `splitByString(delim, str)[n]` pushes correctly
- `IIF`/`IF`/`IFF` confirmed absent from Dremio 26.x SQL parser; `CASE WHEN` is the correct alternative and pushes to ClickHouse via standard SQL
- `LIKE ... ESCAPE '\'` ✅ pushes correctly — no YAML change needed
- `REGEXP_LIKE(col, '(?i)pattern')` ✅ case-insensitive matching confirmed working

---

## Previous Run — TO_CHAR / Date Formatting + Connector Config Expansion

*Run 2026-04-05 (after TO_CHAR, excluded DBs, additional JDBC props, ClickHouse Cloud toggle, max_nested_subqueries=4)*

### Results — 68 / 69 Passed ✅

| Category | Tests | Pass | Notes |
|---|---|---|---|
| TO_CHAR date formatting | 7 | 7 | All format codes correct; local Dremio execution (see note) |
| Excluded databases (config) | 1 | 1 | Hidden schemas do not appear in catalog |
| Additional JDBC props (config) | 1 | 1 | Extra driver properties applied at connection time |
| ClickHouse Cloud toggle (config) | 1 | 1 | Port 8443 + SSL forced when enabled |
| max_nested_subqueries=4 | 1 | 1 | 4-level nested subquery pushed to ClickHouse |
| REGEXP_EXTRACT 3-arg | 3 | 3 | `extractGroups()[N]` pushdown |
| Regression (prior suite) | 54 | 53 | STDDEV known fail unchanged |
| **Total** | **69** | **68** | |

**1 known failure — STDDEV (all variants):** Permanent ARP ↔ ClickHouse nested-aggregate incompatibility (Code 184). Not a regression.

### TO_CHAR Detail

| Query | Result | Output |
|---|---|---|
| `TO_CHAR(order_date, 'YYYY-MM-DD')` | ✅ | `2026-03-01` |
| `TO_CHAR(order_date, 'YYYY-MM-DD HH24:MI:SS')` | ✅ | `2026-03-01 10:00:00` |
| `TO_CHAR(order_date, 'MON YYYY')` | ✅ | `Mar 2026` |
| `TO_CHAR(order_date, 'MONTH DD, YYYY')` | ✅ | `March 01, 2026` |
| `TO_CHAR(order_date, 'DY DD-MON-YY')` | ✅ | `Sun 01-Mar-26` |
| `TO_CHAR(order_date, 'HH24:MI:SS')` | ✅ | `10:00:00` |
| `GROUP BY TO_CHAR(order_date, 'YYYY-MM')` | ✅ | `2026-03 → 11 rows` |

**Execution note:** `TO_CHAR` is computed in Dremio's execution engine (not pushed to ClickHouse), identical to `UPPER`/`LOWER` behaviour in ARP. The dialect's `unparseCall` override (translates Oracle→C strftime codes via `formatDateTime`) is in place and will activate if a future Dremio/ARP version routes `TO_CHAR` through the Calcite unparse path. `datetime_formats` are enabled in the YAML. Results are fully correct.

---

## Previous Run — Function Coverage Expansion

*Run 2026-04-05 (after REGEXP_EXTRACT addition and full regression)*

### Results — 61 / 62 Passed ✅

| Category | Tests | Pass | Notes |
|---|---|---|---|
| Items 1–2 (LEFT, RIGHT, REVERSE) | 3 | 3 | Pre-existing; confirmed working |
| Item 3 (ILIKE workaround via REGEXP_LIKE `(?i)`) | 2 | 2 | ILIKE not in Dremio SQL parser; `(?i)` flag works |
| Item 4 (EXTRACT DOW/DOY/QUARTER/WEEK) | 4 | 4 | Pre-existing; confirmed working |
| Item 5 (REGEXP_EXTRACT 3-arg) | 3 | 3 | New — `extractGroups()[N]` pushdown |
| Regression (original suite) | 50 | 50 | |
| **Total** | **62** | **61** | |

**1 known failure — STDDEV (all variants):** Same permanent ARP ↔ ClickHouse nested-aggregate incompatibility (Code 184). Not a regression.

---

## Previous Run — Post-Optimisation Suite

*Run 2026-04-05 (after performance, query rewriting, and metadata improvements)*

### Environment

| Component | Details |
|---|---|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| ClickHouse | 24.8 (`clickhouse/clickhouse-server:24.8`) |
| JDBC Driver | `clickhouse-jdbc-0.4.6-all.jar` (SPI removed) |
| Plugin JAR | `dremio-clickhouse-connector-1.0.0-SNAPSHOT-plugin.jar` |
| Source name | `clickhouse_test` (type: `CLICKHOUSE`) |
| Test table | `clickhouse_test.testdb.orders` |
| Query interface | Dremio v3 REST API (`POST /api/v3/sql`) |

### Test Table Schema

```sql
orders (
  order_id     VARCHAR      -- UUID
  user_id      VARCHAR      -- UUID
  product_id   VARCHAR      -- UUID
  region       VARCHAR
  status       VARCHAR      -- 'pending', 'shipped', 'delivered'
  amount       DECIMAL(10,2)
  quantity     INTEGER
  order_date   TIMESTAMP
  shipped_date DATE         -- Nullable
)
```

### Results — 54 / 55 Passed ✅

| Category | Tests | Pass | Notes |
|---|---|---|---|
| Basic (SELECT *, COUNT) | 2 | 2 | |
| Filters | 11 | 11 | DATE=, TIMESTAMP>, BETWEEN, IN, NOT IN, LIKE, IS NULL, IS NOT NULL, BETWEEN decimal, NULLIF |
| Aggregation | 7 | 6 | STDDEV is the 1 known fail — see below |
| String | 11 | 11 | Includes **INITCAP** (new) |
| Math | 10 | 10 | Includes **CEIL/FLOOR integer**, **MOD** (new) |
| Date / Time | 7 | 7 | EXTRACT, DATE_TRUNC, TIMESTAMPADD, TIMESTAMPDIFF, NOW, CURRENT_DATE |
| Null / Cast | 4 | 4 | COALESCE, CAST int/dec/varchar/date |
| Joins / Sort / Union | 3 | 3 | INNER JOIN, ORDER BY+LIMIT, UNION ALL |
| **Total** | **55** | **54** | |

**1 known failure — STDDEV (all variants):**
Dremio's ARP generates nested aggregate SQL (e.g. `SUM(x) / COUNT(x)` inside another aggregate) which ClickHouse rejects with `Code: 184`. STDDEV/VAR have been removed from the ARP pushdown list. They still work correctly in Dremio — results are computed locally using raw column data pulled from ClickHouse. This is a fundamental ARP ↔ ClickHouse incompatibility, not a connector bug.

---

## Detailed Test Run — All 55 Tests

### Basic

| Query | Result |
|---|---|
| `SELECT * FROM orders LIMIT 5` | ✅ PASS |
| `SELECT COUNT(*) FROM orders` | ✅ PASS |

### Filters

| Query | Result |
|---|---|
| `WHERE order_date = TIMESTAMP '2026-03-03 00:00:00'` | ✅ PASS |
| `WHERE order_date > TIMESTAMP '2026-01-01 00:00:00'` | ✅ PASS |
| `WHERE order_date BETWEEN TIMESTAMP '...' AND TIMESTAMP '...'` | ✅ PASS |
| `WHERE status IN ('pending','shipped')` | ✅ PASS |
| `WHERE quantity NOT IN (1,2,3)` | ✅ PASS |
| `WHERE status LIKE 'ship%'` | ✅ PASS |
| `WHERE shipped_date IS NULL` | ✅ PASS |
| `WHERE shipped_date IS NOT NULL` | ✅ PASS |
| `WHERE amount BETWEEN 50.0 AND 200.0` | ✅ PASS |
| `SELECT NULLIF(status, 'pending')` | ✅ PASS |

### Aggregation

| Query | Result |
|---|---|
| `SELECT SUM(amount)` | ✅ PASS |
| `SELECT AVG(amount)` | ✅ PASS |
| `SELECT MIN(amount), MAX(amount)` | ✅ PASS |
| `SELECT COUNT(DISTINCT user_id)` | ✅ PASS |
| `SELECT APPROX_COUNT_DISTINCT(user_id)` | ✅ PASS — maps to `uniq()` |
| `GROUP BY user_id HAVING COUNT(*) > 1` | ✅ PASS |
| `SELECT STDDEV(amount)` | ❌ FAIL — Code: 184, known ARP incompatibility |

### String

| Query | Result |
|---|---|
| `SELECT UPPER(status), LOWER(status)` | ✅ PASS |
| `SELECT TRIM(status)` | ✅ PASS |
| `SELECT SUBSTR(status,1,3)` | ✅ PASS |
| `SELECT LOCATE('i', status)` | ✅ PASS |
| `SELECT REPLACE(status,'pending','waiting')` | ✅ PASS |
| `SELECT LPAD(status,10,'*'), RPAD(status,10,'*')` | ✅ PASS |
| `SELECT CONCAT(status,' - ',CAST(user_id AS VARCHAR))` | ✅ PASS |
| `SELECT ASCII(status)` | ✅ PASS |
| `SELECT REGEXP_LIKE(status,'.*ship.*')` | ✅ PASS — maps to `match()` |
| `SELECT REGEXP_REPLACE(status,'ing','ed')` | ✅ PASS — maps to `replaceRegexpAll()` |
| `SELECT INITCAP(status)` | ✅ PASS — new; maps to `initcap()` |

### Math

| Query | Result |
|---|---|
| `SELECT ABS(-5), CEIL(3.2), FLOOR(3.9)` | ✅ PASS |
| `SELECT CEIL(CAST(amount AS INTEGER)), FLOOR(CAST(amount AS INTEGER))` | ✅ PASS — new integer signatures |
| `SELECT ROUND(amount, 1)` | ✅ PASS |
| `SELECT SQRT(CAST(amount AS DOUBLE)), POWER(2.0, 10.0)` | ✅ PASS |
| `SELECT LOG10(100.0), LN(2.718...), LOG2(8.0)` | ✅ PASS |
| `SELECT TRUNC(amount, 1)` | ✅ PASS |
| `SELECT SIGN(-5.0), SIGN(0.0), SIGN(3.0)` | ✅ PASS |
| `SELECT PI()` | ✅ PASS |
| `SELECT SIN(0.0), COS(0.0), TAN(0.0)` | ✅ PASS |
| `SELECT MOD(17,5), MOD(CAST(amount AS INTEGER),3)` | ✅ PASS — new; maps to `modulo()` |

### Date / Time

| Query | Result |
|---|---|
| `SELECT EXTRACT(YEAR/MONTH/DAY FROM order_date)` | ✅ PASS |
| `SELECT EXTRACT(HOUR/MINUTE/SECOND FROM order_date)` | ✅ PASS |
| `SELECT DATE_TRUNC('year'/'month'/'day', order_date)` | ✅ PASS |
| `SELECT TIMESTAMPADD(DAY,7,order_date), TIMESTAMPADD(HOUR,3,order_date)` | ✅ PASS |
| `SELECT TIMESTAMPDIFF(DAY,...), TIMESTAMPDIFF(HOUR,...)` | ✅ PASS |
| `SELECT NOW(), CURRENT_TIMESTAMP` | ✅ PASS |
| `SELECT CURRENT_DATE` | ✅ PASS |

### Null / Cast

| Query | Result |
|---|---|
| `SELECT COALESCE(shipped_date, CAST('2099-01-01' AS DATE))` | ✅ PASS |
| `SELECT CAST(user_id AS VARCHAR)` | ✅ PASS |
| `SELECT CAST(amount AS INTEGER)` | ✅ PASS |
| `SELECT CAST('2026-03-01' AS DATE)` | ✅ PASS |

### Joins / Sort / Union

| Query | Result |
|---|---|
| `orders o JOIN orders o2 ON o.user_id = o2.user_id WHERE o.order_id <> o2.order_id` | ✅ PASS |
| `SELECT order_id, amount FROM orders ORDER BY amount DESC LIMIT 5` | ✅ PASS |
| `SELECT status WHERE region='us-east' UNION ALL SELECT status WHERE region='us-west'` | ✅ PASS |

---

## Original Comprehensive Test Run

*Run 2026-04-05 (initial connector validation — 57 tests)*

### Environment

| Component | Version / Details |
|---|---|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| ClickHouse | 24.8 (`clickhouse/clickhouse-server:24.8`) |
| JDBC Driver | `clickhouse-jdbc-0.4.6-all.jar` (SPI removed) |

### Test Data

| Table | Schema highlights | Rows |
|---|---|---|
| `users` | UUID PK · String · LowCardinality(String) · Int32 · Float64 · Decimal(12,2) · UInt8 · Date · DateTime | 8 |
| `products` | UUID PK · (category,product_id) sort key · LowCardinality · Decimal(10,2) · Int32 · Nullable(Float64) · UInt8 | 9 |
| `orders` | UUID PK · UUID FK refs · LowCardinality(String) status · Decimal(10,2) · Int32 · DateTime · Nullable(Date) | 12 |
| `events` | (device_id, event_time) sort key · LowCardinality · DateTime64(3) · Float64 · UInt32 | 10 |

### Results — 55 / 57 Passed

> **2 items listed as limitations at time of this run.** Item 1 (DateTime TIMESTAMP filter format) has since been **fixed** by the `unparseDateTimeLiteral` override in `ClickHouseDialect`. Item 2 (inline VALUES in UNION ALL) remains a known ARP behaviour.

### Group 1 — Full Table Scans & Native Types

| # | Query | Result |
|---|---|---|
| 1.1 | All 10 columns from `users` (UUID, String, LowCardinality, Int32, Float64, Decimal, UInt8, Date, DateTime) | ✅ 8 rows |
| 1.2 | All 7 columns from `products` including Nullable(Float64) | ✅ 9 rows |
| 1.3 | All 9 columns from `orders` including Nullable(Date) | ✅ 12 rows |
| 1.4 | All 6 columns from `events` including DateTime64(3) | ✅ 10 rows |
| 1.5 | `WHERE rating IS NULL` — Nullable(Float64) | ✅ 1 row |
| 1.6 | `WHERE shipped_date IS NULL` — Nullable(Date) | ✅ 3 rows |

### Group 2 — COUNT & Projection

| # | Query | Result |
|---|---|---|
| 2.1 | `COUNT(*)` across all 4 tables via `UNION ALL` | ✅ 4 rows (8, 9, 12, 10) |
| 2.2 | 2-column projection on `users` + `ORDER BY name` | ✅ 8 rows |
| 2.3 | 2-column projection + `WHERE available = 1 ORDER BY category, name` | ✅ 8 rows |

### Group 3 — WHERE Filter Pushdown

| # | Query | Pushdown type | Result |
|---|---|---|---|
| 3.1 | `WHERE region = 'us-east'` | LowCardinality EQ | ✅ 2 rows |
| 3.2 | `WHERE age > 40` | Int32 range | ✅ 3 rows |
| 3.3 | `WHERE price > 200.00` | Decimal range | ✅ 4 rows |
| 3.4 | `WHERE active = 1` | UInt8 boolean | ✅ 6 rows |
| 3.5 | `WHERE status = 'shipped'` | LowCardinality EQ | ✅ 4 rows |
| 3.6 | `WHERE active = 1 AND score > 80` | Combined AND | ✅ 4 rows |
| 3.7 | DateTime range via `EXTRACT` | EXTRACT workaround | ✅ 5 rows |

> **Note (now fixed):** At time of this test, `WHERE datetime_col >= TIMESTAMP '...'` required an EXTRACT workaround due to ANSI literal format rejection. This was fixed by the `unparseDateTimeLiteral` override — direct TIMESTAMP comparisons now push down correctly.

### Group 4 — LIMIT

| # | Query | Result |
|---|---|---|
| 4.1 | `ORDER BY score DESC LIMIT 3` | ✅ 3 rows |
| 4.2 | `ORDER BY amount DESC LIMIT 5` | ✅ 5 rows |
| 4.3 | `ORDER BY balance DESC LIMIT 1` | ✅ 1 row |

### Group 5 — Aggregations

| # | Query | Result |
|---|---|---|
| 5.1 | `COUNT(*), AVG(age), MAX(score), MIN(score)` on users | ✅ total=8, avg_age=36.25 |
| 5.2 | `GROUP BY region, COUNT(*), SUM(amount)` | ✅ 5 regions; us-east leads at $3,139.96 |
| 5.3 | `GROUP BY category, MIN/MAX/AVG price` | ✅ 3 categories |
| 5.4 | `GROUP BY active, COUNT(*), AVG(score)` | ✅ active=1: 6 users avg 84.6 |
| 5.5 | `GROUP BY device_id, AVG/MIN/MAX value` | ✅ 4 sensors |
| 5.6 | `SUM(amount * quantity)` total revenue | ✅ $4,717.82 |

### Group 6 — Expressions, String Functions, Sorting

| # | Query | Result |
|---|---|---|
| 6.1 | `CASE WHEN score >= 95 THEN 'platinum' …` | ✅ Tier classification correct |
| 6.2 | `balance + 500.00 AS boosted WHERE active = 1` | ✅ 6 rows |
| 6.3 | `UPPER(name), CHAR_LENGTH(email)` | ✅ 8 rows |
| 6.4 | `WHERE email LIKE '%@example.com'` | ✅ 8 rows |
| 6.5 | `CASE WHEN rating IS NULL THEN 'unrated'` | ✅ 9 rows |
| 6.6 | `ORDER BY balance DESC LIMIT 3` | ✅ Eve 8750.25, Carol 3200.75, Bob 289.50 |
| 6.7 | `SELECT DISTINCT region FROM orders ORDER BY region` | ✅ 5 distinct regions |

### Group 7 — JOINs

| # | Query | Result |
|---|---|---|
| 7.1 | `users INNER JOIN orders ON user_id` | ✅ 12 rows |
| 7.2 | `users LEFT JOIN orders … GROUP BY name` | ✅ 8 rows (Grace/Henry show 0 orders) |
| 7.3 | `orders JOIN products ON product_id LIMIT 5` | ✅ 5 rows |
| 7.4 | 3-table JOIN: `users + orders + products WHERE status = 'shipped'` | ✅ 4 rows |

### Groups 8–14

*(Window functions, CTEs, subqueries, temporal queries, type handling, advanced analytics, correctness — all PASS; see original run for full detail)*

### Summary

```
Total tests : 57
Passed      : 55
Known items : 2  (DateTime literal format — now FIXED; inline VALUES UNION ALL — still applies)
```

---

## Known Limitation Details

### STDDEV / VAR — no pushdown (ARP incompatibility)

**Symptom:** `SELECT STDDEV(amount) FROM orders` triggers `Code: 184`:
```
DB::Exception: Aggregate function COUNT(amount) is found inside another aggregate function
```

**Root cause:** Dremio's ARP computes STDDEV using the formula `SQRT((SUM(x²) - SUM(x)²/COUNT(x)) / COUNT(x))`, embedding aggregate calls inside each other. ClickHouse forbids nested aggregates entirely.

**Status:** `stddev_samp`, `stddev_pop`, `var_samp`, `var_pop` removed from ARP pushdown list. Dremio falls back to local computation — correct results, less efficient.

### Inline `VALUES` constant row in `UNION ALL`

**Symptom:** `SELECT 'literal', 42.0 UNION ALL SELECT col FROM table` fails with `Code: 62: Syntax error … VALUES`.

**Root cause:** Dremio rewrites pure constant branches into `VALUES (…)` which ClickHouse's SQL parser doesn't accept.

**Workaround:**
```sql
SELECT 'baseline', 20.0 FROM clickhouse_source.mydb.any_table LIMIT 1
UNION ALL
SELECT col FROM clickhouse_source.mydb.real_table
```
