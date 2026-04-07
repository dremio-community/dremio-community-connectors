# Dremio Splunk Connector — User Guide

Query Splunk indexes as tables directly from Dremio SQL. SQL queries are automatically translated into Splunk Processing Language (SPL) and executed against the Splunk REST API — no Splunk SDK, forwarder, or Spark required.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a Splunk Source in Dremio](#adding-a-splunk-source-in-dremio)
   - [Connection](#connection)
   - [Authentication](#authentication)
   - [Time Range](#time-range)
   - [Index Filtering](#index-filtering)
   - [Performance](#performance)
   - [Advanced](#advanced)
5. [Metadata Columns](#metadata-columns)
6. [Writing Queries](#writing-queries)
7. [Filter Pushdown](#filter-pushdown)
8. [Projection Pushdown](#projection-pushdown)
9. [Schema Inference](#schema-inference)
10. [Splunk Cloud Setup](#splunk-cloud-setup)
11. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector translates Dremio SQL into SPL and executes it via the Splunk management REST API (port 8089). Results are streamed back as JSON events, written into Arrow vectors, and returned to Dremio.

**Query execution flow:**

1. Dremio's planning layer applies `SplunkFilterRule` and `SplunkProjectionRule` to push predicates and column lists into the `SplunkScanSpec`.
2. `SplunkScanSpec.toSpl()` renders a complete SPL string: `search index=<name> [earliest=<t>] [latest=<t>] [field=value…] | fields col1 col2 | head <N>`.
3. When **Parallel Time Buckets** > 1, the time window is divided into N equal sub-ranges at planning time. Each sub-range is a separate `DatasetSplit` dispatched in parallel to different Dremio executor fragments — each executor runs its own Splunk search independently.
4. At execution time, `SplunkRecordReader` submits the SPL and reads results:
   - **Blocking mode** (≤ 1,000 events): one `exec_mode=blocking` POST — results return immediately with no polling.
   - **Async mode** (> 1,000 events): create job → poll until `DONE` → page through results.
4. Results are written into typed Arrow vectors and returned to Dremio.

Each Splunk index appears as a separate table in the Dremio catalog. The schema is inferred by sampling recent events.

---

## Prerequisites

- Splunk Enterprise 8.x or later, or Splunk Cloud
- Splunk management API reachable from Dremio on port 8089 (on-prem) or 443 (Cloud)
- A Splunk user or token with `search` capability on the indexes you want to expose
- Dremio 26.x running in Docker, bare-metal, or Kubernetes

---

## Installation

```bash
cd dremio-splunk-connector

# Interactive installer
./install.sh

# Non-interactive:
./install.sh --docker try-dremio --prebuilt   # use included pre-built JAR
./install.sh --docker try-dremio --build       # compile from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

After installation, restart Dremio. The **Splunk** source type will appear in **Sources → +**.

See `INSTALL.md` for full installation details including `rebuild.sh` and K8s options.

---

## Adding a Splunk Source in Dremio

Navigate to **Sources → + → Splunk**. Or use `add-splunk-source.sh`:

```bash
# On-prem Splunk, username/password
./add-splunk-source.sh \
  --name splunk \
  --host splunk.example.com \
  --splunk-user admin \
  --splunk-pass changeme

# Splunk Cloud with bearer token
./add-splunk-source.sh \
  --name splunk_cloud \
  --host mycompany.splunkcloud.com \
  --cloud \
  --token eyJraWQ...

# Local Docker dev (no SSL)
./add-splunk-source.sh \
  --name splunk_dev \
  --host localhost \
  --no-ssl \
  --splunk-user admin \
  --splunk-pass changeme
```

---

### Connection

#### Hostname
**Required.** Hostname or IP of your Splunk instance.

```
splunk.example.com                    # on-prem
mycompany.splunkcloud.com             # Splunk Cloud
172.17.0.3                            # Docker container IP
```

#### Port
The Splunk management API port.

- Default: `8089` (on-prem)
- Splunk Cloud: `443` (set automatically when **Splunk Cloud** is checked)

#### Use SSL
When checked, the connector communicates with Splunk over HTTPS. Enabled by default.

- Disable for local Docker development where Splunk uses a self-signed cert and you prefer plain HTTP (`--no-ssl`)
- For on-prem with a self-signed cert: keep SSL enabled and check **Disable SSL Certificate Verification**

#### Splunk Cloud
Check when connecting to **Splunk Cloud** (managed service). Automatically sets port to 443 and forces HTTPS.

#### Disable SSL Certificate Verification
When checked, skips TLS certificate chain validation and hostname verification.

> **Warning:** Only use in development environments with self-signed certificates. The connector uses a full-bypass `X509ExtendedTrustManager` — both certificate validation and hostname checking are disabled. Never enable in production.

---

### Authentication

Choose one of two authentication methods:

#### Username / Password
Standard Splunk authentication. The connector calls `POST /services/auth/login` to obtain a session key, which is used for all subsequent requests.

- Requires a Splunk user with `search` capability
- Session keys are refreshed automatically on expiry

#### Auth Token
A Splunk Bearer token (JWT). Recommended for Splunk Cloud and production on-prem deployments.

- Generate a token in Splunk Web: **Settings → Tokens → + New Token**
- The token is sent as `Authorization: Bearer <token>` on every request
- No session key exchange is needed

If both are filled in, the token takes precedence.

---

### Time Range

#### Default Earliest Time
The default start of the time window for searches when no `WHERE _time` filter is provided.

- Default: `-24h`
- Accepts Splunk time modifiers: `-1h`, `-7d`, `-30d`, `2024-01-01T00:00:00`
- Use `-0s` or leave blank to start from the beginning of index retention

#### Default Max Events
Maximum number of events a scan returns when no `LIMIT` clause is pushed down.

- Default: `50000`
- Set lower (e.g. `1000`) for interactive exploration of large indexes
- Set to `0` for no limit (use with caution on high-volume indexes)

---

### Index Filtering

#### Index Exclude Pattern (regex)
Java regex. Matching indexes are hidden from the Dremio catalog.

- Example: `^(summary|history)` — hide summary and history indexes
- Leave blank to show all indexes

#### Index Include Pattern (regex)
Optional Java regex. When set, only matching indexes appear in the catalog.

- Example: `^prod-` — show only production indexes
- Leave blank to include all non-excluded indexes

---

### Performance

#### Schema Sample Events
Number of recent events to sample per index for schema inference.

- Default: `100`
- The connector searches `index=<name> | head N` and collects all field names and values
- Increase for indexes with high field variability across events

#### Metadata Cache TTL (seconds)
How long to cache inferred schemas and event counts locally in Dremio.

- Default: `300` seconds (5 minutes)
- Set to `0` to always re-infer (useful during development)
- Cached schemas are invalidated immediately when you run `ALTER TABLE ... REFRESH METADATA`

#### Results Page Size
Number of events fetched per HTTP request when paging through large result sets (async mode).

- Default: `1000`
- Larger values reduce round-trips but use more memory
- Has no effect in blocking mode (≤ 1,000 events total)

---

### Advanced

#### Search Mode
Controls whether Splunk searches both raw events and summary indexes or only raw events.

- `smart` (default): Splunk decides automatically based on available summaries
- `fast`: faster scans; may miss events not yet in the summary
- `verbose`: always scans raw events; most accurate, slowest

#### Read Timeout (seconds)
Maximum time to wait for a Splunk search job to complete.

- Default: `300` seconds
- Increase for long-running searches over large time windows
- Has no effect in blocking mode (blocking mode uses the HTTP client timeout)

#### Parallel Time Buckets
Number of parallel time-range splits per index scan.

- Default: `1` (single sequential scan — the original behaviour)
- When set to N > 1, the default time window (`Default Earliest Time` → now) is divided into N equal sub-ranges. Each sub-range becomes a separate Dremio `DatasetSplit` that can be dispatched to a different executor fragment, reducing wall-clock query time on large or high-volume indexes.
- Typical values: `4` to `8` for production clusters with 4+ executor nodes
- The per-bucket event cap is `Default Max Events / N` so the total cap across all buckets is preserved
- Only affects the default time window; explicit `WHERE _time` filters are re-partitioned by the planner

---

## Metadata Columns

Every Splunk index exposes these columns regardless of its event schema:

| Column | Type | Description |
|--------|------|-------------|
| `_time` | TIMESTAMP | Event timestamp (millisecond precision) |
| `_raw` | VARCHAR | Full raw event text |
| `_host` | VARCHAR | Host that generated the event |
| `_source` | VARCHAR | Source file or input name |
| `_sourcetype` | VARCHAR | Splunk sourcetype |
| `_index` | VARCHAR | Splunk index name |

Additional field columns are appended after these based on schema inference.

---

## Writing Queries

### Basic select

```sql
-- Most recent events (up to defaultMaxEvents)
SELECT * FROM splunk.main;

-- Select specific columns (pushes | fields to Splunk)
SELECT _time, _host, _sourcetype, _raw
FROM splunk.main
LIMIT 100;
```

### Filter by time

```sql
-- Last hour of events
SELECT * FROM splunk.main
WHERE _time >= NOW() - INTERVAL '1' HOUR;

-- Specific date range
SELECT * FROM splunk.main
WHERE _time BETWEEN TIMESTAMP '2024-01-15 00:00:00'
               AND  TIMESTAMP '2024-01-15 23:59:59';
```

### Filter by field value

```sql
-- Field equality (pushed down as SPL keyword search)
SELECT * FROM splunk.main
WHERE _sourcetype = 'access_combined'
  AND _host = 'web01';

-- Combined time + field filter
SELECT _time, _host, status, uri
FROM splunk.main
WHERE _time >= NOW() - INTERVAL '1' HOUR
  AND status = '500';
```

### Aggregations

```sql
-- Event count by sourcetype
SELECT _sourcetype, COUNT(*) AS event_count
FROM splunk.main
GROUP BY _sourcetype
ORDER BY event_count DESC;

-- Error rate by host over last 24 hours
SELECT
    _host,
    COUNT(*) AS total_events,
    SUM(CASE WHEN status = '500' THEN 1 ELSE 0 END) AS errors
FROM splunk.main
WHERE _time >= NOW() - INTERVAL '24' HOUR
GROUP BY _host;
```

### LIMIT pushdown

```sql
-- LIMIT is pushed down — Splunk's | head clause limits events at the source
SELECT * FROM splunk.main
WHERE _time >= NOW() - INTERVAL '1' HOUR
LIMIT 500;
-- SPL: search index=main earliest=-1h | head 500
```

### Joining Splunk with other sources

```sql
-- Join Splunk events with a Dremio-managed dimension table
SELECT
    e._time,
    e._host,
    e.user_id,
    u.email,
    u.department
FROM splunk.main e
JOIN my_catalog.users u ON e.user_id = u.id
WHERE e._time >= NOW() - INTERVAL '1' HOUR
LIMIT 1000;
```

### EXPLAIN PLAN

```sql
-- Verify that filter and projection pushdown are active
EXPLAIN PLAN FOR
SELECT _time, _host, status
FROM splunk.main
WHERE _time >= NOW() - INTERVAL '1' HOUR
  AND status = '500';
-- Look for "SplunkScan" in the plan output
```

---

## Filter Pushdown

The connector pushes two types of predicates directly into the SPL, reducing data transferred from Splunk:

### Time range pushdown

```sql
WHERE _time >= NOW() - INTERVAL '1' HOUR
-- SPL: ... earliest=-1h latest=now
```

```sql
WHERE _time BETWEEN TIMESTAMP '2024-01-15 00:00:00'
               AND  TIMESTAMP '2024-01-15 23:59:59'
-- SPL: ... earliest=1705276800 latest=1705363199
```

Splunk's `earliest`/`latest` modifiers are resolved at **planning time** from the epoch millisecond values pushed down by Dremio. This is the most impactful optimization — time-bounded searches use Splunk's index-level bloom filters and skip entire buckets outside the window.

### Field equality pushdown

```sql
WHERE _sourcetype = 'access_combined' AND status = '500'
-- SPL: ... sourcetype="access_combined" status="500"
```

Field equality filters on string columns are appended to the SPL search string as keyword-value pairs. Splunk evaluates these using its inverted index.

> **Note:** Only `=` comparisons on string fields push down. Range filters (`>`, `<`), `LIKE`, `IN`, and filters on numeric or timestamp non-`_time` columns are applied as residual filters in Dremio after the scan.

### Multiple filter layers

The `SplunkFilterRule` accumulates predicates across multiple Calcite planning passes. If Dremio applies a time filter from one plan node and a field filter from another, both are merged into a single SPL search — no duplicate API calls.

---

## Projection Pushdown

When a query selects specific columns rather than `SELECT *`, the connector appends a `| fields` clause to the SPL:

```sql
SELECT _time, _host, status FROM splunk.main
-- SPL: search index=main | fields _time _host status | head 50000
```

This reduces the JSON payload returned by Splunk — events contain only the requested fields rather than all fields. The benefit is most significant on wide events with many fields (e.g. 50+ fields) where only a few are needed.

---

## Schema Inference

Splunk is schemaless — fields vary event by event. The connector infers a schema by:

1. Running `search index=<name> | head <sampleEventsForSchema>` against the index
2. Collecting all field names observed across sampled events
3. Inferring column types by examining sampled values:
   - All-numeric values → BIGINT or DOUBLE
   - ISO-8601 timestamps → TIMESTAMP
   - Everything else → VARCHAR
4. Always including the 6 standard metadata columns (`_time`, `_raw`, `_host`, `_source`, `_sourcetype`, `_index`)

**Schema is cached** for `metadataCacheTtlSeconds` (default 5 minutes). To refresh after new fields appear in your data:

```sql
ALTER TABLE splunk.main REFRESH METADATA;
```

Or right-click the table in the Dremio UI → **Refresh Metadata**.

Fields present in some events but absent in others always produce `null` for events that don't have them — the connector never fails on a missing field.

---

## Splunk Cloud Setup

Splunk Cloud uses HTTPS on port 443 with JWT bearer token authentication.

```bash
./add-splunk-source.sh \
  --name splunk_cloud \
  --host mycompany.splunkcloud.com \
  --cloud \
  --token eyJraWQ...
```

Or via the UI:
- **Hostname**: `mycompany.splunkcloud.com`
- **Splunk Cloud**: ✅ checked (sets port to 443, forces HTTPS)
- **Auth Token**: paste your JWT
- **Use SSL**: ✅ checked
- **Disable SSL Verification**: leave unchecked (Splunk Cloud uses a trusted certificate)

To generate a token in Splunk Cloud Web:
1. **Settings → Tokens → + New Token**
2. Set an appropriate expiry (90 days recommended for production)
3. Assign the token to a user with `search` capability

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| `SPLUNK` not in source dropdown | JAR not in `jars/3rdparty/` or Dremio not restarted | Check path, restart Dremio |
| Source shows "bad state" | Authentication failed or Splunk unreachable | Check hostname, port, credentials, SSL setting |
| `SSLHandshakeException: No subject alternative names` | SSL hostname verification failing on bare IP | Enable **Disable SSL Certificate Verification** in source config |
| No indexes visible in catalog | All indexes filtered out, or auth token lacks search access | Check include/exclude patterns; verify user has `search` capability |
| Schema missing expected fields | New fields not present in the sampled events | Increase **Schema Sample Events**; run `ALTER TABLE ... REFRESH METADATA` |
| Empty results from a query | Time window too narrow, or `defaultMaxEvents` too low | Widen the `WHERE _time` filter; increase **Default Max Events** |
| Slow queries on large indexes | Full index scan with no time filter | Add a `WHERE _time >= ...` filter — this is the single most impactful optimization |
| Aggregations return wrong counts | Dremio is aggregating on top of Splunk results | Expected behavior — aggregations are not pushed to Splunk, they run in Dremio |
| `SYSTEM ERROR` on query | Schema/type mismatch or unexpected Splunk response | Run `EXPLAIN PLAN` to check the scan spec; open a GitHub issue with the error ID |
