# Dremio Splunk Connector

Query Splunk indexes as SQL tables directly from Dremio. SQL queries are translated to SPL (Splunk Processing Language) with time-range and field-equality pushdown for efficient execution.

**Status: ✅ 20/20 smoke tests passing**

## What it does

- Exposes every visible Splunk index as a Dremio table
- Infers column schema by sampling recent events (Splunk is schemaless)
- Translates `WHERE _time >= ...` to Splunk `earliest`/`latest` parameters
- Translates `WHERE field = 'value'` to SPL field-equality filters
- Pushes `LIMIT N` to `| head N` in SPL
- Supports Splunk on-prem (port 8089) and Splunk Cloud (port 443, JWT bearer tokens)
- Authentication: username/password (session key) or bearer token

## Quick Install

```bash
# Step 1: Install the plugin into your Dremio Docker container
./install.sh --docker try-dremio --prebuilt

# Step 2: Register a Splunk source in Dremio
./add-splunk-source.sh --name splunk --host splunk.example.com \
  --splunk-user admin --splunk-pass changeme

# Step 3: Run smoke tests
./test-connection.sh --source splunk --index main
```

Or open the Dremio UI → **Sources** → **+** → **Splunk**.

## SQL Examples

```sql
-- Browse recent events
SELECT _time, _host, _sourcetype, _raw
FROM splunk.main
WHERE _time >= NOW() - INTERVAL '1' HOUR
LIMIT 100;

-- Count events by sourcetype in the last 24 hours
SELECT _sourcetype, COUNT(*) AS event_count
FROM splunk.main
WHERE _time >= NOW() - INTERVAL '1' DAY
GROUP BY _sourcetype
ORDER BY event_count DESC;

-- Filter by a specific field value
SELECT _time, host, status, clientip
FROM splunk.web_logs
WHERE _time >= NOW() - INTERVAL '6' HOUR
  AND status = '404'
LIMIT 500;

-- Time range with exact timestamps
SELECT _time, _sourcetype, _raw
FROM splunk.security
WHERE _time BETWEEN TIMESTAMP '2024-01-15 00:00:00'
                AND TIMESTAMP '2024-01-15 23:59:59'
LIMIT 1000;

-- CTAS: materialize Splunk data into an Iceberg table
CREATE TABLE iceberg_minio."dremio-test".splunk_errors AS
SELECT _time, _host, _sourcetype, message
FROM splunk.main
WHERE _time >= NOW() - INTERVAL '7' DAY
  AND level = 'ERROR';
```

## Schema

Every index always has these metadata columns:

| Column | Type | Description |
|---|---|---|
| `_time` | `TIMESTAMP` | Event timestamp (parsed from ISO-8601) |
| `_raw` | `VARCHAR` | Raw event text |
| `_index` | `VARCHAR` | Splunk index name |
| `_sourcetype` | `VARCHAR` | Splunk sourcetype |
| `_source` | `VARCHAR` | Source (file path, syslog host, etc.) |
| `_host` | `VARCHAR` | Host that generated the event |

Additional columns are inferred by sampling recent events. Since Splunk is schemaless, columns vary per index and may not appear in every event (missing fields return `NULL`).

## Filter Pushdown

| SQL predicate | Pushed to Splunk as |
|---|---|
| `WHERE _time >= TIMESTAMP '...'` | `earliest_time` job parameter |
| `WHERE _time < TIMESTAMP '...'` | `latest_time` job parameter |
| `WHERE _time BETWEEN a AND b` | Both `earliest_time` and `latest_time` |
| `WHERE field = 'value'` | `field="value"` in SPL search string |
| `LIMIT N` | `\| head N` in SPL |

All pushed-down predicates are also kept as Dremio residual filters for correctness.

## Configuration

| Setting | Default | Description |
|---|---|---|
| Hostname | `localhost` | Splunk server hostname |
| Port | `8089` | Management API port (Cloud: 443) |
| Use SSL | `true` | Enable HTTPS |
| Splunk Cloud | `false` | Forces port 443 + SSL |
| Username / Password | — | Session key authentication |
| Auth Token | — | Bearer token (takes priority over password) |
| Default Earliest | `-24h` | Time window for unfiltered scans |
| Default Max Events | `50000` | Cap per query when no LIMIT is set |
| Sample Events | `200` | Events sampled per index for schema inference |
| Metadata Cache TTL | `300s` | How long to cache schemas and index list |
| Index Exclude Pattern | `^(_.*\|history\|fishbucket)$` | Regex for indexes to hide |
| Search Mode | `normal` | `normal`, `fast`, or `verbose` |

## Splunk Cloud

```bash
./add-splunk-source.sh \
  --name splunk_cloud \
  --host mycompany.splunkcloud.com \
  --cloud \
  --token eyJraWQ...
```

For Splunk Cloud, use a bearer token (JWT) created under **Settings → Tokens** in your Splunk Cloud instance. Do not use username/password with Splunk Cloud.

## Keeping Up with Dremio Upgrades

`rebuild.sh` detects the running Dremio version, updates `pom.xml`, rebuilds the JAR against the live JARs, and redeploys — all in one command:

```bash
cd splunk && ./rebuild.sh               # Docker (default: try-dremio)
cd splunk && ./rebuild.sh --k8s pod-0   # Kubernetes
cd splunk && ./rebuild.sh --dry-run     # Preview only (no changes)
```

If nothing changed it exits immediately — safe to run on every Dremio upgrade.

## Building from Source

```bash
# Prerequisites: Java 11+, Maven 3.6+
mvn package -q -DskipTests
# Output: target/dremio-splunk-connector-1.0.0-SNAPSHOT-plugin.jar

# Or build inside Docker (no local Maven needed):
./install.sh --docker try-dremio --build
```

## How It Works

1. **Schema inference**: At metadata refresh time, Dremio calls `getDatasetMetadata()`. The connector runs `search index=<name> | head 200` against Splunk, samples the returned events, and infers Arrow field types. The schema is cached for 5 minutes.

2. **Query execution**: When you run a SQL query, Dremio calls `SplunkFilterRule` to extract any `WHERE _time` and `WHERE field=value` predicates from the query tree and embed them into a `SplunkScanSpec`. The spec is serialized into the physical plan.

3. **Search job**: `SplunkRecordReader.setup()` calls `POST /services/search/jobs` with the translated SPL string, then polls until the job reaches `DONE` state.

4. **Result pagination**: `SplunkRecordReader.next()` pages through `GET /services/search/jobs/{sid}/results` in batches of 5000 events, writing each event's fields into Arrow vectors.

5. **Cleanup**: `SplunkRecordReader.close()` always calls `DELETE /services/search/jobs/{sid}` to avoid leaving orphaned jobs on the Splunk server.

## Limitations

- Splunk is schemaless: schema is inferred from a sample and may not reflect all fields. Refresh metadata in Dremio to pick up new fields.
- Single-partition scan per index (no time-bucketed parallelism in V1). For large indexes, use `WHERE _time` filters to narrow the scan window.
- SPL is not SQL: complex predicates (OR across fields, LIKE, numeric ranges on non-`_time` fields) are not pushed down and are applied as Dremio residual filters.
- `_time` pushdown uses Splunk job parameters (`earliest_time`/`latest_time`), not SPL `earliest()`/`latest()` functions — this is the correct approach for job-based search.
- Write operations (`INSERT`, `UPDATE`, `DELETE`) are not supported — Splunk is a read-only source for Dremio.

## More Documentation

- [INSTALL.md](INSTALL.md) — step-by-step install for Docker, bare-metal, and Kubernetes
- [TEST_RESULTS.md](TEST_RESULTS.md) — full test results and known bugs found during testing

## License

Apache License 2.0. See [LICENSE](LICENSE).
