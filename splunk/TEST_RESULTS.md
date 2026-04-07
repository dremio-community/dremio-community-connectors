# Dremio Splunk Connector — Test Results

---

## Latest Run — Post-Optimization Validation (v1.1)

*Run 2026-04-07 against Dremio OSS 26.0.5, Splunk Enterprise 10.2.2 (Docker)*

Includes all four optimizations:
1. **Column projection pushdown** (`SplunkProjectionRule`) — `| fields …` in SPL
2. **Blocking mode for small queries** — `exec_mode=blocking` when `maxEvents ≤ 1000`
3. **Cached index event counts** — `listIndexesWithCounts()` avoids per-index HTTP call
4. **Filter rule accumulation** — `SplunkFilterRule` accumulates across multi-pass planning

### Smoke Test — 20 / 20 Passed ✅ (`test-connection.sh`)

| # | Test | Result |
|---|------|--------|
| 1 | SELECT * LIMIT 5 | ✅ |
| 2 | Metadata column: _time | ✅ |
| 3 | Metadata column: _raw | ✅ |
| 4 | Metadata column: _host | ✅ |
| 5 | Metadata column: _sourcetype | ✅ |
| 6 | Metadata column: _source | ✅ |
| 7 | Metadata column: _index | ✅ |
| 8 | COUNT(*) returns a number | ✅ |
| 9 | WHERE _time range (last 1 hour) | ✅ |
| 10 | WHERE field = value (string eq) | ✅ |
| 11 | GROUP BY _sourcetype | ✅ |
| 12 | GROUP BY _host | ✅ |
| 13 | ORDER BY _time DESC | ✅ |
| 14 | LIMIT 10 returns <= 10 rows | ✅ |
| 15 | CTE: WITH latest AS (...) SELECT ... | ✅ |
| 16 | EXPLAIN PLAN contains SplunkScan | ✅ |
| 17 | _time BETWEEN two timestamps | ✅ |
| 18 | NULL check: WHERE _raw IS NOT NULL | ✅ |
| 19 | MIN/MAX _time | ✅ |
| 20 | CAST(_time AS VARCHAR) | ✅ |

**Environment:** Dremio OSS 26.0.5 in Docker (Apple Silicon, amd64 emulation); Splunk Enterprise 9.4.1 in Docker; `main` index with internal Splunk events.

---

### Unit Tests — 18 / 18 Passed ✅ (`mvn test`)

| Test Class | Tests | Result |
|------------|-------|--------|
| `SplunkScanSpecTest` | 9 | ✅ |
| `SplunkSchemaInferrerTest` | 9 | ✅ |

`SplunkScanSpecTest` covers: `toSpl()` generation, `effectiveEarliest/Latest()` with epoch-ms and string forms, serialization round-trip via `toExtendedProperty/fromExtendedProperty()`, `hasFilterPushdown()`, and `withMaxEvents()`.

`SplunkSchemaInferrerTest` covers: `rankValue()` type promotion order (boolean < bigint < double < varchar), `parseTime()` with ISO-8601 + offset, Unix epoch float strings, null and invalid inputs, and `getMetadataFields()` field count and names.

---

## Bugs Found and Fixed During Testing

| Bug | Root Cause | Fix |
|-----|-----------|-----|
| `SSLHandshakeException: No subject alternative names present` | Java 11 `HttpClient` wraps plain `X509TrustManager` and still runs hostname check even with a trust-all context | Switched to `X509ExtendedTrustManager`; override all 4 `checkXxx` variants including the 3-arg `SSLEngine` form |
| `UnsupportedOperationException: Unable to determine vector class for type Timestamp(MILLISECOND, UTC)` | Dremio's `CompleteType.getValueVectorClass()` has no mapping for timezone-qualified timestamps | Changed schema to `Timestamp(MILLISECOND, null)`; changed RecordReader to `TimeStampMilliVector` |
| `SchemaPath.STAR cannot find symbol` | `SchemaPath.STAR` constant does not exist in Dremio 26.x | Replaced with root-segment `"*"` string check |

## Optimizations Applied

| # | Optimization | Change | Impact |
|---|---|---|---|
| 1 | Column projection pushdown | Added `SplunkProjectionRule`; appends `\| fields col1 col2` to SPL | Reduces Splunk JSON payload and network transfer for narrow SELECT |
| 2 | Blocking mode for small queries | `SplunkRecordReader` uses `exec_mode=blocking` when `maxEvents ≤ 1000` | Saves 3+ round-trips (create → poll → fetch) for bounded queries |
| 3 | Cached index event counts | `listIndexesWithCounts()` returns names + counts in one HTTP call; `getDatasetMetadata()` uses cache | Eliminates one per-index API call during metadata refresh |
| 4 | Filter rule accumulation | `SplunkFilterRule` accumulates across multiple planner passes; anti-loop via `changed` flag | Allows stacked filters to merge pushdowns correctly |

---

## Known Limitations

- **Schemaless index**: schema is inferred from a sample of recent events. Fields absent from the sample won't appear as columns. Refresh metadata in Dremio to pick up new fields.
- **Single-partition scan**: V1 does not time-bucket large scans in parallel. Use `WHERE _time` filters to bound large indexes.
- **Complex predicates not pushed down**: OR across fields, LIKE, numeric range on non-`_time` fields are applied as Dremio residual filters (correct, just not efficient).
- **Write operations not supported**: Splunk is treated as read-only. `INSERT`, `UPDATE`, `DELETE` will fail.
