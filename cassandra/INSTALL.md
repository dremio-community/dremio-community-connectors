# Dremio Apache Cassandra Connector — Install Guide

Adds **native read support** for Apache Cassandra tables to Dremio 26.x.
Uses the DataStax Java Driver 4.x (no Spark, no JDBC) via the CQL native binary protocol.

> **Read-only connector.** Supports `SELECT` queries only. `INSERT INTO`, `CTAS`,
> and `UPDATE` are not supported. All Cassandra tables are automatically discovered
> by the catalog browser — no manual promotion needed.

---

## What's Included

| Feature | Status |
|---|---|
| Full `SELECT` with projection pushdown | ✅ |
| Partition key predicate pushdown (`WHERE pk = val`) | ✅ |
| Clustering key range pushdown (`WHERE pk = val AND ck > val`) | ✅ |
| Secondary index pushdown (SAI + regular indexes) | ✅ |
| `LIMIT N` pushdown (embedded in CQL query) | ✅ |
| Token-range parallel splits (configurable parallelism) | ✅ |
| Direct node routing via `setRoutingToken()` | ✅ |
| Native collection types (LIST/SET → `ListVector`, MAP → `MapVector`, UDT → `StructVector`) | ✅ |
| `VECTOR<float, N>` type (Cassandra 5.x) → Arrow `FixedSizeList<float32>` | ✅ |
| Row count estimation via `system.size_estimates` | ✅ |
| Adaptive fetch size (auto-capped to 2 MB per page for wide tables) | ✅ |
| Async CQL page prefetch (overlaps network I/O with Arrow writes) | ✅ |
| Protocol compression (`NONE` / `LZ4` / `SNAPPY`) | ✅ |
| Schema change notifications (live fingerprint; auto re-plan on drift) | ✅ |
| Auto-discovery of all keyspaces and tables | ✅ |
| Cross-source JOINs (Cassandra ↔ Hudi, Delta, Iceberg, S3…) | ✅ |
| Auto-reconnect on session loss | ✅ |
| Username / password authentication | ✅ |
| SSL / TLS + mTLS (custom truststore, client keystore) | ✅ |
| Datacenter auto-detection from `system.local` / `system.peers` | ✅ |
| Multi-DC failover with configurable fallback datacenters | ✅ |
| Contact point DNS validation at source start | ✅ |
| Configurable consistency level | ✅ |
| Speculative execution | ✅ |
| Metadata caching (TTL) | ✅ |

---

## Prerequisites

| Requirement | Version |
|---|---|
| Dremio OSS | 26.0.5 (match `dremio.version` in `pom.xml`) |
| Apache Cassandra | 3.x, 4.x, or 5.x |
| Java | 11+ (inside the Dremio container — host does not need Java) |
| Maven | 3.8+ (only if building from source) |
| Docker | Any recent version (for Docker install path) |

---

## Quick Start

The installer supports three deployment modes and two JAR options. The simplest path
uses the pre-built JAR included in `jars/` — no Maven required.

### Option A — Pre-built JAR (recommended, fastest)

```bash
chmod +x install.sh

# Interactive — prompts for mode and options
./install.sh

# Or fully non-interactive:
./install.sh --docker try-dremio --prebuilt   # Docker container
./install.sh --local /opt/dremio --prebuilt   # Bare-metal / VM
./install.sh --k8s dremio-0 --prebuilt        # Kubernetes pod
```

The installer copies the JAR, sets permissions, and restarts Dremio automatically.

### Option B — Build from source

Compiles the connector inside the Dremio container (ensures the exact same JVM and
Dremio JARs are used). Maven is installed automatically if not already present.

```bash
./install.sh --docker try-dremio --build
```

After a successful build the compiled JAR is saved to `jars/` so future installs
can use `--prebuilt` without rebuilding.

### After installation

1. Wait ~30 seconds for Dremio to restart, then open **http://localhost:9047**
2. Go to **Sources → + Add Source → Apache Cassandra**
3. Fill in the required fields (see [Configuration Reference](#configuration-reference))
4. Click **Save**

Dremio immediately discovers all non-system keyspaces and their tables.

```sql
SELECT * FROM cassandra_source.my_keyspace.my_table LIMIT 10;
```

---

## Manual Deploy (without the installer)

If you prefer to deploy the JAR yourself:

```bash
# Copy the pre-built JAR into Dremio's plugin directory
cp jars/dremio-cassandra-connector-1.0.0-SNAPSHOT-plugin.jar \
   $DREMIO_HOME/jars/3rdparty/

# Restart Dremio
docker restart try-dremio
# or: $DREMIO_HOME/bin/dremio restart
# or: sudo systemctl restart dremio
```

### Building from source manually

If you need to build outside of the installer (e.g. on a CI server with Maven):

```bash
# 1. Install Dremio JARs into your local Maven repo
DREMIO_HOME=/opt/dremio
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
HADOOP_VER=3.3.6-dremio-202507241551560856-75923ad5
JARS=$DREMIO_HOME/jars
TP=$JARS/3rdparty

install_jar() {
  mvn install:install-file \
    -Dfile="$1" -DgroupId="$2" -DartifactId="$3" -Dversion="$4" \
    -Dpackaging=jar -q
}

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar          com.dremio        dremio-sabot-kernel         $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar                com.dremio        dremio-common               $DREMIO_VER
install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar         com.dremio.plugin dremio-plugin-common        $DREMIO_VER
install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar    com.dremio        dremio-sabot-kernel-proto   $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar    com.dremio        dremio-sabot-vector-tools   $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar    com.dremio        dremio-services-namespace   $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar             com.dremio        dremio-connector            $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar         com.dremio        dremio-sabot-logical        $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar           com.dremio        dremio-common-core          $DREMIO_VER
install_jar $JARS/dremio-services-credentials-${DREMIO_VER}.jar  com.dremio.services dremio-services-credentials $DREMIO_VER
install_jar $JARS/dremio-services-datastore-${DREMIO_VER}.jar    com.dremio        dremio-services-datastore   $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                    org.apache.arrow  arrow-vector                $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar               org.apache.arrow  arrow-memory-core           $ARROW_VER
install_jar $TP/arrow-format-${ARROW_VER}.jar                    org.apache.arrow  arrow-format                $ARROW_VER
install_jar $TP/hadoop-common-${HADOOP_VER}.jar                  org.apache.hadoop hadoop-common              $HADOOP_VER
install_jar $TP/guava-33.4.0-jre.jar                             com.google.guava  guava                       33.4.0-jre
install_jar $TP/javax.inject-1.jar                               javax.inject      javax.inject                1

CALCITE_VER=$(ls $TP/calcite-core-*.jar | head -1 | sed "s/.*calcite-core-//;s/\.jar//")
install_jar $TP/calcite-core-${CALCITE_VER}.jar   org.apache.calcite calcite-core   $CALCITE_VER
install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar org.apache.calcite calcite-linq4j $CALCITE_VER

# 2. Build
mvn package -DskipTests

# 3. Deploy
cp target/dremio-cassandra-connector-1.0.0-SNAPSHOT-plugin.jar \
   $DREMIO_HOME/jars/3rdparty/
```

---

## Configuration Reference

All fields are set when adding or editing the source in the Dremio UI under
**Sources → Add Source → Apache Cassandra**.

| Field | Default | Description |
|---|---|---|
| **Host** | `localhost` | Cassandra contact point hostname(s) or IP(s), comma-separated |
| **Port** | `9042` | CQL native transport port |
| **Local Datacenter** | _(auto-detect)_ | Leave blank to detect from `system.local`/`system.peers`; or set explicitly (e.g. `datacenter1`) |
| **Username** | _(empty)_ | Auth username; leave empty if authentication is disabled |
| **Password** | _(empty)_ | Auth password (stored encrypted by Dremio) |
| **Read Timeout (ms)** | `30000` | CQL request timeout; increase for large full-table scans |
| **Fetch Size** | `1000` | Rows returned per CQL page; controls memory usage vs. round-trips |
| **Excluded Keyspaces** | _(empty)_ | Comma-separated keyspace names to hide (system keyspaces always excluded) |
| **Consistency Level** | `LOCAL_ONE` | CQL read consistency: `LOCAL_ONE`, `LOCAL_QUORUM`, `QUORUM`, `ONE`, etc. |
| **Fallback Datacenters** | _(empty)_ | Comma-separated remote DCs for cross-DC failover; automatically relaxes `LOCAL_*` consistency levels |
| **Enable SSL/TLS** | `false` | Enable TLS 1.2+ for wire encryption |
| **TLS Truststore Path** | _(empty)_ | Path to JKS/PKCS12 truststore; blank = JVM default CA bundle |
| **TLS Truststore Password** | _(empty)_ | Truststore password (stored encrypted) |
| **TLS Truststore Type** | `JKS` | `JKS` or `PKCS12` |
| **mTLS Keystore Path** | _(empty)_ | Path to JKS/PKCS12 keystore for client certificate (mTLS); blank = one-way TLS |
| **mTLS Keystore Password** | _(empty)_ | Keystore password (stored encrypted) |
| **mTLS Keystore Type** | `JKS` | `JKS` or `PKCS12` |
| **TLS Hostname Verification** | `true` | Verify server hostname against cert CN/SAN; disable for self-signed dev certs |
| **Speculative Execution** | `false` | Issue a retry to another node if the first is slow |
| **Speculative Delay (ms)** | `500` | Milliseconds to wait before issuing a speculative retry |
| **Split Parallelism** | `8` | Number of token-range splits per table (degree of read parallelism) |
| **Metadata Cache TTL (s)** | `60` | How long to cache Cassandra schema metadata before refreshing |
| **Protocol Compression** | `NONE` | CQL wire-protocol compression: `NONE`, `LZ4`, or `SNAPPY`. Reduces bandwidth at the cost of CPU; recommended for WAN/cross-AZ |
| **Async Page Prefetch** | `true` | Prefetch the next CQL page while writing the current page into Arrow vectors. Overlaps network I/O with CPU for large scans; disable only for debugging. |

### Choosing Split Parallelism

Token-range splits divide the Cassandra ring into N equal segments and scan each in
parallel using separate Dremio executor fragments.

| Cluster size | Recommended `splitParallelism` |
|---|---|
| Single node (dev/test) | `1` – `4` |
| 3-node cluster | `4` – `8` |
| 6+ node cluster | `8` – `16` |
| Large production cluster | `16` – `32` |

Setting it higher than your cluster can handle will not cause errors — Dremio
naturally throttles to the available executor count.

---

## Querying

### Basic reads

```sql
-- All rows
SELECT * FROM cassandra_source.my_keyspace.users LIMIT 100;

-- Projection pushdown — only user_id, name, email sent in CQL query
SELECT user_id, name, email FROM cassandra_source.my_keyspace.users;
```

### Partition key predicate pushdown

When a `WHERE` clause provides equality conditions on **all** partition key columns,
the connector pushes it to Cassandra as a direct partition lookup — no token-range
scan, no full table read.

```sql
-- Single partition key (UUID)
SELECT * FROM cassandra_source.my_keyspace.users
WHERE user_id = '50babb30-7260-4d98-898b-2a8b43ba806e';

-- Composite partition key — all parts required for pushdown to fire
SELECT * FROM cassandra_source.analytics.events
WHERE tenant_id = 'acme' AND event_type = 'login';
```

### Partition key + clustering key pushdown

Once the partition key is fully specified, range predicates on clustering key columns
are also pushed to CQL for server-side filtering.

```sql
-- device_id is partition key, event_time is clustering key
SELECT * FROM cassandra_source.iot.events
WHERE device_id = 'sensor-001'
  AND event_time >= TIMESTAMP '2026-04-01 00:00:00'
  AND event_time <  TIMESTAMP '2026-04-02 00:00:00';
```

> **When pushdown doesn't fire:** If the `WHERE` clause does not cover all partition
> key columns, the connector falls back to a token-range scan and Dremio applies the
> filter after fetching. Results are always correct; only performance differs.
> Check partition keys with: `DESCRIBE TABLE keyspace.table;` (in cqlsh)

### Aggregations

```sql
-- Aggregation is computed by Dremio's execution engine after the CQL scan
SELECT region, COUNT(*) AS cnt, AVG(score) AS avg_score
FROM cassandra_source.analytics.events
GROUP BY region
ORDER BY cnt DESC;
```

### Cross-source JOINs

Cassandra tables can be joined with any other source registered in Dremio.

```sql
-- Cassandra ↔ Hudi
SELECT c.user_id, c.email, h.tier
FROM cassandra_source.commerce.users c
JOIN hudi_lake.customers h ON c.email = h.email;

-- Cassandra ↔ Delta Lake
SELECT e.device_id, e.value, d.description
FROM cassandra_source.iot.events e
JOIN delta_source.devices.catalog d ON e.device_id = d.id;

-- Cassandra ↔ S3 / Parquet
SELECT c.name, p.order_count
FROM cassandra_source.app.users c
JOIN s3_source.reports.user_summary p ON c.user_id = p.user_id;
```

---

## Upgrading Dremio

When you upgrade Dremio to a new version, the connector JAR must be recompiled against
the new Dremio JARs. Use `rebuild.sh` — it handles version detection, `pom.xml` update,
JAR installation, compilation, and deployment automatically.

### Option A — Browser UI (recommended, no command line)

```bash
python3 rebuild-ui.py   # opens http://localhost:8765 in your browser automatically
```

Or use the platform launcher:

| Platform | Launcher |
|---|---|
| macOS | Double-click `Rebuild Connector.command` in Finder |
| Windows | Double-click `Rebuild Connector.bat` in File Explorer |
| Linux / Linux Mint | `./Rebuild\ Connector.sh` — or use `Rebuild Connector.desktop` for a desktop icon |

Select your target (Docker / Local / Kubernetes), click **▶ Rebuild & Deploy**, and watch
the real-time output stream. Requires Python 3 (stdlib only, no `pip install`).

### Option B — Command line

```bash
# Docker (default container name: try-dremio)
./rebuild.sh --docker try-dremio

# Bare-metal
./rebuild.sh --local /opt/dremio

# Kubernetes pod
./rebuild.sh --k8s dremio-0

# Preview only — see what would change without touching anything
./rebuild.sh --dry-run

# Force a rebuild even when the detected version matches pom.xml
./rebuild.sh --docker try-dremio --force
```

The script:
1. Detects Dremio, Arrow, Hadoop, and Calcite JAR versions from the running instance
2. Compares them against `pom.xml` — exits cleanly if nothing changed
3. Updates `pom.xml` (backing up `pom.xml.bak`) and installs JARs into Maven local repo
4. Compiles the connector and deploys the new JAR
5. Restarts Dremio automatically (Docker) or prompts for a manual restart (local/K8s)
6. On build failure: restores `pom.xml.bak` and prints diagnostic guidance

### Manual upgrade (advanced)

If you prefer to manage version updates yourself:

1. Find the Dremio version string from JAR filenames in `$DREMIO_HOME/jars/`
   (e.g. `dremio-common-26.1.0-<timestamp>.jar`)
2. Update `dremio.version` (and `arrow.version`, `hadoop.version`) in `pom.xml`
3. Re-install Dremio JARs into Maven local repo (see Manual Build section above)
4. Rebuild with `mvn package -DskipTests`
5. Copy the new JAR to `$DREMIO_HOME/jars/3rdparty/` and restart Dremio
6. Update `jars/` in this package so future `--prebuilt` installs use the new JAR

---

## Docker / Container Networking

> **This section is critical for Docker-based deployments.** Skipping it is the most
> common cause of a Cassandra source showing as "unavailable" after a container restart.

### Why container IPs are unreliable

Docker's default bridge network assigns IP addresses dynamically. Every time containers
are restarted (or the host reboots), they may receive different IPs. If you configure the
Cassandra source with an IP address (`172.17.0.x`), that address may belong to a completely
different container after the next restart — causing "source unavailable" or connection
failures with no obvious error.

### Solution: user-defined bridge network with hostname resolution

Create a named Docker network once. Containers on a user-defined network resolve each other
by container name as a stable DNS hostname.

```bash
# Create the network (one-time)
docker network create dremio-net

# Connect both containers (run once per container; persists across restarts)
docker network connect dremio-net try-dremio
docker network connect dremio-net cassandra-test

# Verify: Dremio container can reach Cassandra by name
docker exec try-dremio bash -c "curl -s --max-time 3 telnet://cassandra-test:9042 && echo OK || echo FAIL"
```

When adding the source in Dremio (or via `add-cassandra-source.sh`), set **Host** to the
**container name** (`cassandra-test`), never to an IP address.

### Recovering a source configured with a stale IP

If the source is already in an "unavailable" state due to a stale IP:

**Option 1 — Edit via UI:**
1. Go to **Sources → cassandra_test → ⚙ → Edit**
2. Change **Host** from the old IP to the container hostname (e.g. `cassandra-test`)
3. Click **Save** — Dremio reconnects immediately

**Option 2 — Delete and recreate via script:**
```bash
# The --force flag handles the delete+recreate for you
./add-cassandra-source.sh \
  --name cassandra_test \
  --cassandra cassandra-test \
  --user mark \
  --password yourpassword \
  --force
```

**Option 3 — Delete via API and recreate manually:**
```bash
# 1. Get the source's catalog ID and version tag
curl -s -H "Authorization: _dremio<TOKEN>" \
  http://localhost:9047/api/v3/catalog/by-path/cassandra_test \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d['id'], d['tag'])"

# 2. Delete with the version tag (URL-encode = as %3D)
curl -s -X DELETE \
  -H "Authorization: _dremio<TOKEN>" \
  "http://localhost:9047/apiv2/source/cassandra_test?version=<TAG>"

# 3. Recreate with hostname
./add-cassandra-source.sh --name cassandra_test --cassandra cassandra-test ...
```

### Checking which network a container is on

```bash
# See all networks a container belongs to
docker inspect cassandra-test --format '{{json .NetworkSettings.Networks}}' | python3 -m json.tool | grep -E '"(Name|IPAddress)"'

# Quick check — is cassandra-test on dremio-net?
docker network inspect dremio-net --format '{{range .Containers}}{{.Name}} {{end}}'
```

---

## Troubleshooting

**Source doesn't appear in Add Source**
- Confirm the JAR is in `$DREMIO_HOME/jars/3rdparty/`
- Check Dremio startup logs: `grep -i cassandra /var/log/dremio/server.log`
- Verify `sabot-module.conf` is inside the JAR:
  `jar tf dremio-cassandra-connector-*-plugin.jar | grep sabot`
- Dremio must be fully restarted (not just refreshed) to pick up new JARs

**"Failed to connect to Cassandra" on source save**
- Verify the host is reachable from the Dremio container:
  `docker exec try-dremio bash -c "curl -s telnet://cassandra-host:9042"`
- Verify the datacenter name matches exactly:
  `SELECT data_center FROM system.local;` (in cqlsh)
- If auth is enabled, verify credentials are correct in the source config

**Tables appear in catalog but SELECT returns no rows**
- Right-click the source → **Refresh Metadata**
- Verify the table has data: `SELECT COUNT(*) FROM keyspace.table;` (in cqlsh)

**SELECT hangs or times out**
- Increase **Read Timeout (ms)** in the source settings (default: 30000)
- Reduce **Split Parallelism** if the Cassandra cluster is under load
- Check for tombstones or compaction issues on the Cassandra side

**Predicate pushdown not firing**
- Pushdown only fires when `WHERE` covers **all** partition key columns with `=`
- Clustering key range pushdown requires the partition key to be fully specified first
- Find partition keys: `DESCRIBE TABLE keyspace.table;` (in cqlsh)
- Non-key predicates are always handled by Dremio — results are still correct

**Source shows "bad state" or "unavailable" after Cassandra restart**
- The connector auto-reconnects on the next health-check cycle (~30–60 seconds)
- If it doesn't recover, check that the Cassandra container is on the `dremio-net` network
  (see [Docker / Container Networking](#docker--container-networking) above)
- If the source has a stale IP, use `--force` to delete and recreate it:
  ```bash
  ./add-cassandra-source.sh --name cassandra_test --cassandra cassandra-test \
    --user mark --password yourpassword --force
  ```
- Or edit the source in the UI: **Sources → cassandra_test → ⚙ → Edit**, update the Host field

**ClassNotFoundException on first query after deploy**
- Dremio requires a full restart to load new JARs
- `docker restart try-dremio` (or `systemctl restart dremio`)

**Concurrent update error when editing source via API**
- Dremio's health-check thread updates source state frequently; fetch the latest `tag`
  immediately before the PUT to avoid conflicts
