# Dremio Redis Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read support for Redis hash data**.

Discovers "tables" by scanning Redis keys and grouping them by prefix
(e.g., keys `user:1`, `user:2` → table `user`). Infers Arrow schema by
sampling hash fields. Executes queries via `SCAN` + `HGETALL` with
client-side predicate evaluation by Dremio's query engine.

Uses the **Jedis** Redis client bundled directly in the plugin JAR — no
separate client deployment required.

---

## Architecture

```
SQL:  SELECT name, country, COUNT(*)
      FROM redis_source.user
      WHERE country = 'US'
      GROUP BY country

Dremio Planner
  └── RedisScanRule  → RedisScanDrel (logical)
  └── RedisScanPrule → RedisScanPrel (physical)
        └── RedisGroupScan → RedisSubScan
              └── RedisRecordReader
                    ├── SCAN user:* (iterate all matching keys)
                    └── HGETALL user:{id} (read each hash)

Dremio Filter Operator (client-side WHERE evaluation)
Dremio Aggregate Operator (GROUP BY, COUNT, SUM, AVG, MIN, MAX)

Jedis → Redis server (AUTH + SELECT db)
```

### Key Classes

| Class | Role |
|---|---|
| `RedisConf` | Source config shown in Dremio's "Add Source" UI. Defines host, port, password, database, key delimiter, sample size, and timeout fields. |
| `RedisPlugin` | StoragePlugin implementation. Manages Jedis connection pool, table discovery, schema inference with TTL cache. |
| `RedisConnection` | Jedis connection pool wrapper. Exposes `listTables()`, `scanKeys()`, `hgetAll()`, `sampleHashes()`. |
| `RedisTypeConverter` | Infers Arrow field types from sampled string values: BigInt (integer strings), Float8 (decimal strings), Bool ("true"/"false"), Utf8 (everything else). |
| `RedisRecordReader` | Reads Redis hash data via SCAN + HGETALL into Arrow vectors. |

### How Tables are Discovered

Redis has no native concept of tables. This connector discovers tables by
scanning all keys and grouping by the prefix before the key delimiter (`:` by default):

```
Redis keys:        Tables discovered:
  user:1    ──┐
  user:2    ──┤──→  user  (5 rows)
  user:3    ──┘
  order:1   ──┐
  order:2   ──┤──→  order (6 rows)
  order:3   ──┘
  product:1 ──→     product (6 rows)
```

The key suffix (the part after the delimiter) becomes the special `_id` column
in each table. For `user:1`, `_id = "1"`.

### Schema Inference

Redis stores all values as strings. The connector samples up to `sampleSize`
keys per table and inspects the string values to infer Arrow types:

| Observed values | Arrow type |
|---|---|
| All parse as `Long` | `BigInt` |
| All parse as `Double` (not Long) | `Float8` |
| Only `true`/`false`/`1`/`0` | `Bool` |
| Otherwise | `Utf8` |

### Single-JAR Deployment

The Jedis library is bundled directly inside the plugin fat JAR via
`maven-shade-plugin` with package relocation to avoid classpath conflicts.

---

## What's Implemented

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ Working | Full hash scan via SCAN + HGETALL |
| **Schema inference** | ✅ Working | Type detection from sampled string values |
| **WHERE filter** | ✅ Working | Client-side evaluation by Dremio |
| **ORDER BY** | ✅ Working | Evaluated by Dremio after scan |
| **GROUP BY + aggregations** | ✅ Working | COUNT, SUM, AVG, MIN, MAX by Dremio |
| **LIMIT pushdown** | ✅ Working | Limits key count in SCAN loop |
| **JOIN** | ✅ Working | Cross-table JOIN by Dremio |
| **CASE WHEN** | ✅ Working | Computed by Dremio |
| **String functions** | ✅ Working | UPPER, LOWER, CONCAT, SUBSTRING, etc. |
| **Boolean type** | ✅ Working | Inferred from true/false/1/0 strings |
| **Integer type** | ✅ Working | BigInt for integer-valued hash fields |
| **Double type** | ✅ Working | Float8 for decimal-valued hash fields |
| **AUTH password** | ✅ Working | Redis AUTH command |
| **Database select** | ✅ Working | Redis SELECT db-index |
| **Custom delimiter** | ✅ Working | Default `:`, configurable |
| **Redis Strings** | ❌ Not supported | Only Hash keys are exposed as tables |
| **Lists, Sets, Sorted Sets** | ❌ Not supported | Only Hash type is supported |
| **INSERT / UPDATE / DELETE** | ❌ Not supported | Read-only connector |
| **Server-side filtering** | ❌ Not supported | No Redis Query language; Dremio filters post-scan |

---

## Quick Start

### 1. Install the connector

```bash
# Docker (default container: try-dremio)
./install.sh --docker try-dremio --prebuilt

# Bare-metal
./install.sh --local /opt/dremio --prebuilt

# Kubernetes
./install.sh --k8s dremio-master-0 --prebuilt
```

### 2. Add a Redis source in Dremio

Open `http://localhost:9047` → **Sources → + → Redis** and fill in:

| Field | Value |
|---|---|
| Host | `localhost` or your Redis hostname |
| Port | `6379` |
| Password | your Redis AUTH password (leave blank if none) |
| Database Index | `0` |
| Key Delimiter | `:` |

### 3. Query

```sql
-- Full table scan
SELECT * FROM redis.user LIMIT 10;

-- Filter (evaluated by Dremio after SCAN)
SELECT name, country, score
FROM redis.user
WHERE country = 'US' AND score > 80;

-- Aggregation
SELECT country, COUNT(*) AS users, AVG(score) AS avg_score
FROM redis.user
GROUP BY country ORDER BY avg_score DESC;

-- ORDER BY
SELECT name, score FROM redis.user ORDER BY score DESC LIMIT 5;

-- JOIN across Redis tables
SELECT u.name, o.total, o.status
FROM redis.user u
JOIN redis.order o ON u._id = o.user_id
WHERE o.status = 'delivered';

-- CASE expression
SELECT name,
       CASE WHEN score >= 90 THEN 'high'
            WHEN score >= 70 THEN 'mid'
            ELSE 'low' END AS tier
FROM redis.user ORDER BY score DESC;

-- Cross-source JOIN: Redis + Iceberg
SELECT u.name, u.email, i.segment
FROM redis.user u
JOIN iceberg.analytics.segments i ON u._id = i.user_id;
```

---

## Key Design Notes

### Key Pattern Discovery

Tables are discovered with a full `SCAN` of the keyspace on every metadata
refresh. For large Redis instances with millions of keys, this can be slow.
Use the `Schema Cache TTL` setting to control refresh frequency.

### `_id` Column

Every table has a synthetic `_id` column containing the key suffix (the part
after the first delimiter). For `user:123`, `_id = "123"`. The `_id` type is
always `Utf8` regardless of content.

### Client-Side Filtering

Redis has no server-side query language for hash data. All `WHERE` predicates
are evaluated by Dremio's filter operator after the full table scan. For large
tables, this means all keys are scanned and all hashes are fetched before
filtering. Index-like optimization is not available.

### Type Inference Limitations

Schema is inferred by sampling a fixed number of keys. If a field has mixed
content (e.g., `"98.5"` in some keys and `"N/A"` in others), it will be
typed as `Utf8`. Increase `sampleSize` for better accuracy.

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| Host | — | Redis server hostname or IP |
| Port | 6379 | Redis server port |
| Password | _(blank)_ | Redis AUTH password |
| Database Index | 0 | Redis logical database (0–15) |
| Key Delimiter | `:` | Separator between table name and key ID in Redis keys |
| Schema Sample Size | 50 | Keys sampled per table for type inference |
| SCAN Batch Size | 100 | COUNT hint per SCAN call (performance tuning) |
| Connection Timeout (ms) | 5000 | TCP connect timeout |
| Socket Timeout (ms) | 30000 | Socket read timeout |

---

## Building

```bash
mvn clean package -DskipTests
# Deployable JAR:
target/dremio-redis-connector-1.0.0-SNAPSHOT-plugin.jar
```

---

## Smoke Tests

37 tests (13 standard + 24 Redis-specific) in the companion test harness:

```bash
cd dremio-connector-tests
python3 -m pytest connectors/test_redis.py -v
```

**Result: 37/37 tests passing.**

---

## References

- [Redis HGETALL Command](https://redis.io/commands/hgetall/)
- [Redis SCAN Command](https://redis.io/commands/scan/)
- [Jedis Java Client](https://github.com/redis/jedis)
