# Dremio Apache Cassandra Connector

*Built by Mark Shainman*

A native Dremio storage plugin that adds full **read support for Apache Cassandra** tables,
including predicate pushdown, parallel token-range splits, and automatic reconnection.

Dremio 26.x ships with no built-in Cassandra connector. This plugin bridges that gap by
implementing Dremio's `ConnectionConf` + `StoragePlugin` interfaces and using the
**DataStax Java Driver 4.x** to query Cassandra via the CQL native binary protocol
— no Spark, no JDBC, no ORM.

---

## Features

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ | Full table scans via CQL native protocol |
| **Auto-discovery** | ✅ | All keyspaces and tables appear in catalog browser |
| **Schema inference** | ✅ | CQL types mapped to Arrow/Dremio types automatically |
| **Schema change notifications** | ✅ | Live column fingerprint on every query; auto re-plans on schema drift |
| **Projection pushdown** | ✅ | Only requested columns sent in CQL query |
| **Partition key pushdown** | ✅ | `WHERE pk = val` pushed to CQL as direct partition lookup |
| **Clustering key pushdown** | ✅ | Range predicates on clustering columns pushed after partition key |
| **Secondary index pushdown** | ✅ | SAI and regular secondary indexes used without `ALLOW FILTERING` where possible |
| **LIMIT pushdown** | ✅ | `LIMIT N` embedded in CQL query; reduces rows fetched from Cassandra |
| **Parallel splits** | ✅ | Token-ring splits (configurable parallelism, default 8) |
| **Direct node routing** | ✅ | `setRoutingToken()` routes each split to its owning replica, bypassing coordinator fan-out |
| **Native collection types** | ✅ | LIST/SET → Arrow `ListVector`; MAP → `MapVector`; UDT → `StructVector` |
| **Row count estimation** | ✅ | Queries `system.size_estimates` for planner cost statistics |
| **Adaptive fetch size** | ✅ | Page size auto-capped to 2 MB budget for wide-column tables |
| **Async page prefetch** | ✅ | Next CQL page fetched in background while current page writes to Arrow vectors |
| **Protocol compression** | ✅ | Optional LZ4/Snappy wire compression; recommended for WAN/cross-AZ |
| **Authentication** | ✅ | Username/password via DataStax `PasswordAuthProvider` |
| **SSL/TLS + mTLS** | ✅ | Custom truststore, client keystore, hostname verification toggle |
| **DC auto-detection** | ✅ | Detects local datacenter from `system.local` + `system.peers` if left blank |
| **Multi-DC load balancing** | ✅ | Configurable fallback DCs; `LOCAL_*` consistency auto-relaxed when set |
| **Contact point validation** | ✅ | DNS resolution checked at source start; warns per bad host, fails fast if all unresolvable |
| **Auto-reconnect** | ✅ | Recovers from session loss without Dremio restart |
| **Cross-source JOIN** | ✅ | Joins Cassandra tables with Hudi, Delta, S3, etc. |
| **VECTOR\<float, N\> type** | ✅ | Cassandra 5.x embedding vectors → Arrow `FixedSizeList<float32>`; queryable as fixed-length float arrays |
| **CTAS / INSERT INTO** | ❌ | Read-only; write support not planned |

---

## Quick Install

> **Docker users:** Before adding the source, connect the Cassandra container to Dremio's
> named network and use the **container hostname** (not an IP) in the source config.
> IPs change on container restart; hostnames are stable. See [INSTALL.md — Docker / Container Networking](INSTALL.md#docker--container-networking) for full details and recovery steps.

```bash
# One-time network setup (Docker deployments only)
docker network create dremio-net   # if not already created
docker network connect dremio-net try-dremio
docker network connect dremio-net cassandra-test

# Interactive — prompts for mode and JAR choice
./install.sh

# Docker, use the included pre-built JAR (fastest, no Maven needed)
./install.sh --docker try-dremio --prebuilt

# Docker, build from source inside the container
./install.sh --docker try-dremio --build

# Bare-metal Dremio installation
./install.sh --local /opt/dremio --prebuilt

# Kubernetes pod
./install.sh --k8s dremio-0 --prebuilt
```

The installer handles everything: copying the JAR, setting permissions, and restarting Dremio.
A pre-built JAR is included in `jars/` so Maven is not required for a standard install.

See [INSTALL.md](INSTALL.md) for full configuration reference.

---

## Upgrading Dremio

When you upgrade Dremio to a new version, the connector JAR must be recompiled against
the new Dremio JARs. Run the one-click rebuild tool — it auto-detects the running version,
updates `pom.xml`, recompiles, and redeploys:

```bash
# Detect version + rebuild + redeploy — Docker
./rebuild.sh --docker try-dremio

# Bare-metal
./rebuild.sh --local /opt/dremio

# See what would change without rebuilding
./rebuild.sh --dry-run
```

**Browser UI (no command line required):**

| Platform | Launcher |
|---|---|
| macOS | Double-click `Rebuild Connector.command` in Finder |
| Windows | Double-click `Rebuild Connector.bat` in File Explorer |
| Linux / Linux Mint | Run `./Rebuild\ Connector.sh` — or use `Rebuild Connector.desktop` for a desktop icon |

```bash
python3 rebuild-ui.py   # starts http://localhost:8765 and opens your browser
```

The UI streams real-time output, color-codes each step, and shows a clear ✓ / ✗ result.
Requires Python 3 (no `pip install` needed — stdlib only).

---

## SQL Usage

```sql
-- Full table scan
SELECT * FROM cassandra_source.my_keyspace.users LIMIT 100;

-- Partition key pushdown — CQL: SELECT ... WHERE user_id = '...'
SELECT * FROM cassandra_source.my_keyspace.users
WHERE user_id = '50babb30-7260-4d98-898b-2a8b43ba806e';

-- Partition key + clustering key pushdown
SELECT * FROM cassandra_source.my_keyspace.events
WHERE device_id = 'sensor-001'
  AND event_time >= TIMESTAMP '2026-01-01 00:00:00';

-- Aggregation (executed in Dremio after CQL fetch)
SELECT region, COUNT(*) as cnt, AVG(score) as avg_score
FROM cassandra_source.analytics.user_events
GROUP BY region
ORDER BY cnt DESC;

-- Cross-source JOIN with a Hudi table
SELECT c.user_id, c.email, h.tier
FROM cassandra_source.commerce.users c
JOIN hudi_source.dim.customers h ON c.email = h.email;

-- Cross-source JOIN with Delta Lake
SELECT e.device_id, e.value, d.description
FROM cassandra_source.iot.events e
JOIN delta_source.devices.catalog d ON e.device_id = d.id;
```

> **Pushdown behaviour:** Equality predicates on all partition key columns are pushed to CQL
> as a direct partition lookup. Range predicates (`>`, `>=`, `<`, `<=`) on clustering columns
> are pushed after the partition key. All other predicates are evaluated by Dremio's execution
> engine (residual filter) — results are always correct.

---

## Architecture

```
SQL Query
  └── ScanCrel  (Dremio catalog scan)
        └── CassandraScanRule       [LOGICAL phase]
              └── CassandraScanDrel
                    ├── CassandraFilterRule  [LOGICAL phase — extracts pushdown predicates]
                    ├── CassandraLimitRule   [LOGICAL phase — pushes LIMIT N into scan spec]
                    └── CassandraScanPrule  [PHYSICAL phase]
                          └── CassandraScanPrel
                                └── getPhysicalOperator()
                                      └── CassandraGroupScan
                                            └── getSpecificScan()
                                                  └── CassandraSubScan
                                                        └── CassandraScanCreator
                                                              └── CassandraRecordReader
                                                                    └── CQL → Arrow batches
```

### Key Classes

| Class | Role |
|---|---|
| `CassandraStoragePluginConfig` | Source config shown in "Add Source" UI. All 24 configuration fields (`@Tag(1)`–`@Tag(24)`). |
| `CassandraTypeConverter` (VECTOR) | Maps `VectorType` → `ArrowType.FixedSizeList(N)` with `Float4Vector` child. |
| `CassandraStoragePlugin` | Plugin lifecycle, metadata API, health-check with auto-reconnect. |
| `CassandraConnection` | Wraps `CqlSession`. One per plugin instance; thread-safe. |
| `CassandraTypeConverter` | Maps CQL types to Arrow types for schema construction. |
| `CassandraScanSpec` | Carries keyspace, table, token range, predicates, and parallelism through planning and serialization. |
| `CassandraPredicate` | Serializable representation of a single pushed-down filter condition. |
| `CassandraFilterRule` | Planner rule: extracts partition/clustering key predicates from `FilterRel` and bakes them into `CassandraScanSpec`. |
| `CassandraLimitRule` | Planner rule: pushes `LIMIT N` from `LimitRel` into `CassandraScanSpec` so CQL queries include `LIMIT N`. |
| `CassandraGroupScan` | Planning-layer scan; distributes token-range splits across executor fragments. |
| `CassandraSubScan` | JSON-serializable executor work unit for one or more token-range splits. |
| `CassandraRecordReader` | Runs the CQL query, validates pushed predicates against live metadata, writes Arrow batches. |
| `CassandraScanRule` | Planner rule: `ScanCrel` → `CassandraScanDrel` (LOGICAL phase). |
| `CassandraScanPrule` | Planner rule: `CassandraScanDrel` → `CassandraScanPrel` (PHYSICAL phase). |
| `CassandraRulesFactory` | Registers LOGICAL (`ScanRule` + `FilterRule` + `LimitRule`) and PHYSICAL rule sets. |
| `CassandraScanCreator` | Executor-side factory; maps `CassandraSubScan` → `ScanOperator` + `CassandraRecordReader`. |

---

## CQL Type Mapping

| Cassandra Type | Arrow / Dremio Type | Notes |
|---|---|---|
| TEXT, VARCHAR, ASCII | UTF8 | |
| INT | INT32 | |
| BIGINT, COUNTER | INT64 | |
| SMALLINT | INT16 | |
| TINYINT | INT8 | |
| FLOAT | FLOAT32 | |
| DOUBLE | FLOAT64 | |
| BOOLEAN | BIT | |
| DECIMAL | DECIMAL(38, 10) | Fixed scale; adjust in `CassandraTypeConverter` if needed |
| VARINT | UTF8 | Serialized as decimal string |
| TIMESTAMP | TIMESTAMP_MILLI (UTC) | |
| DATE | DATE_DAY | |
| TIME | TIME_NANO | |
| UUID, TIMEUUID | UTF8 | Serialized as string |
| INET | UTF8 | Dotted-decimal string |
| BLOB | VARBINARY | |
| DURATION | DURATION_MILLI | |
| LIST\<T\>, SET\<T\> | List\<T\> | Native Arrow `ListVector`; elements recursively typed |
| MAP\<K,V\> | Map\<K,V\> | Native Arrow `MapVector`; keys non-nullable, values nullable |
| UDT | Struct | Native Arrow `StructVector`; one child field per UDT field |
| TUPLE, FROZEN | UTF8 | Serialized as `.toString()` |
| VECTOR\<float, N\> | FixedSizeList\<float32\>(N) | Cassandra 5.x; Arrow `FixedSizeListVector`; each row is a fixed-length float array of dimension N |

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| `host` | `localhost` | Cassandra contact point hostname(s) or IP(s), comma-separated |
| `port` | `9042` | CQL native protocol port |
| `datacenter` | _(auto-detect)_ | Local datacenter name; leave blank to detect from `system.local` |
| `username` | _(empty)_ | Username for PasswordAuthProvider |
| `password` | _(empty)_ | Password (stored encrypted by Dremio) |
| `readTimeoutMs` | `30000` | CQL read timeout in milliseconds |
| `fetchSize` | `1000` | Rows per CQL page (pagination) |
| `excludedKeyspaces` | _(empty)_ | Comma-separated keyspace names to hide |
| `consistencyLevel` | `LOCAL_ONE` | CQL consistency level (`LOCAL_ONE`, `LOCAL_QUORUM`, `QUORUM`, …) |
| `fallbackDatacenters` | _(empty)_ | Comma-separated fallback DC names; relaxes `LOCAL_*` consistency for cross-DC failover |
| `sslEnabled` | `false` | Enable TLS for the driver connection |
| `sslTruststorePath` | _(empty)_ | Path to JKS/PKCS12 truststore for custom CA; blank = JVM default |
| `sslTruststorePassword` | _(empty)_ | Truststore password (stored encrypted) |
| `sslTruststoreType` | `JKS` | Truststore format: `JKS` or `PKCS12` |
| `sslKeystorePath` | _(empty)_ | Path to JKS/PKCS12 keystore for mTLS client certificate; blank = one-way TLS |
| `sslKeystorePassword` | _(empty)_ | Keystore password (stored encrypted) |
| `sslKeystoreType` | `JKS` | Keystore format: `JKS` or `PKCS12` |
| `sslHostnameVerification` | `true` | Verify server hostname against cert CN/SAN; disable only for self-signed dev certs |
| `speculativeExecutionEnabled` | `false` | Enable speculative retries |
| `speculativeExecutionDelayMs` | `500` | Delay before speculative retry fires |
| `splitParallelism` | `8` | Number of token-range splits per table |
| `metadataCacheTtlSeconds` | `60` | How long to cache Cassandra schema metadata |
| `compressionAlgorithm` | `NONE` | CQL wire-protocol compression: `NONE`, `LZ4`, or `SNAPPY`. Reduces bandwidth at cost of CPU; recommended for WAN/cross-AZ |
| `asyncPagePrefetch` | `true` | Prefetch the next CQL page while writing the current page into Arrow vectors. Overlaps network I/O with CPU for large scans. |

---

## Design Notes

**Why DataStax Java Driver 4.x and not JDBC?**
The DataStax driver communicates via the CQL native binary protocol (port 9042), giving
direct access to Cassandra's type system and streaming pagination. JDBC drivers for Cassandra
have limited type fidelity. Dremio's ARP framework is JDBC-only and not applicable here.

**Why residual filters for pushdown safety?**
The filter rule runs at planning time without database access — it cannot know which columns
are partition keys at that stage. All simple equality predicates are pushed as candidates.
The `RecordReader` validates them at execution time against live DataStax metadata. Non-PK
predicates are handled by Dremio's residual filter, so results are always correct.

**Why `Integer.MAX_VALUE` in `getMaxParallelizationWidth()`?**
Returning `MAX_VALUE` lets Dremio use all available splits naturally. The actual degree of
parallelism is bounded by the number of splits created in `listPartitionChunks()`, which
equals the `splitParallelism` config value. No artificial cap is needed.

**Why single-fragment mode for predicate pushdown?**
When predicates cover all partition key columns, the query is a direct partition lookup.
Running it on N fragments would produce N copies of the same rows. The connector forces
`maxParallelizationWidth = 1` when predicates are present to prevent duplicates.

**How schema change notifications work**
`CassandraRecordReader.setup()` computes a fingerprint of the live Cassandra column schema
on every query execution and compares it against the last-seen fingerprint stored in
`CassandraStoragePlugin.schemaHashCache`. On a mismatch it invalidates the metadata cache
and throws `UserException.schemaChangeError()` — Dremio's signal to re-plan the query
with the updated schema. Transparent to the user; adds one `ConcurrentHashMap.get()` per query.

**Why adaptive fetch size instead of a fixed page size?**
The configured `fetchSize` (default 1000 rows) is appropriate for narrow tables. For
wide-column tables the same row count can produce multi-MB pages, pressuring both the
Cassandra coordinator and the Dremio executor. `CassandraRecordReader` caps the fetch
size to a 2 MB per-page budget (`max(50, min(fetchSize, 2MB / estimatedRowBytes))`) using
the same row-size estimate already computed for planner statistics. Only kicks in for
tables wider than ~2 KB/row; has no effect on typical tables.

**Why `setRoutingToken()` and not vnode-aligned splits?**
Vnode alignment matters for storage engines that bypass the CQL layer. This connector
uses the CQL binary protocol, so the coordinator always handles routing correctly
regardless of vnode boundaries. `setRoutingToken()` is a cheaper alternative: a one-line
hint to the DataStax driver's token-aware load-balancing policy to send each split
directly to the replica owning the start of the token range, removing one coordinator
hop without any planning-time overhead.

---

## Requirements

- **Dremio OSS** 26.x (tested on 26.0.5)
- **Apache Cassandra** 3.x, 4.x, or 5.x (VECTOR type requires 5.x)
- **Java** 11+ (provided by the Dremio container)
- **Maven** 3.8+ (only required if building from source)

---

## References

- [DataStax Java Driver 4.x Docs](https://docs.datastax.com/en/developer/java-driver/4.17/)
- [Cassandra CQL Reference](https://cassandra.apache.org/doc/latest/cassandra/cql/types.html)
- [Dremio StoragePlugin Interface](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/StoragePlugin.java)
- [Dremio ConnectionConf Base Class](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/catalog/conf/ConnectionConf.java)
