# Dremio Amazon DynamoDB Connector — Install Guide

Adds **native read support** for Amazon DynamoDB tables to Dremio 26.x.
Uses the AWS SDK for Java v2 (no Spark, no JDBC) with shaded classes to avoid
conflicts with any AWS SDKs already present in Dremio.

> **Read-only connector.** Supports `SELECT` queries only. `INSERT INTO`, `CTAS`,
> and `UPDATE` are not supported. All DynamoDB tables are automatically discovered
> by the catalog browser — no manual promotion needed.

---

## What's Included

| Feature | Status |
|---|---|
| Full `SELECT` with projection pushdown (ProjectionExpression) | ✅ |
| Partition key EQ pushdown → DynamoDB Query API | ✅ |
| Sort key range pushdown → KeyConditionExpression | ✅ |
| Non-key filter pushdown → FilterExpression | ✅ |
| `LIMIT N` pushdown | ✅ |
| Parallel Scan segments (configurable `splitParallelism`) | ✅ |
| String Set (SS) → Arrow `ListVector<VarChar>` | ✅ |
| Number Set (NS) → Arrow `ListVector<Float8>` | ✅ |
| Integer N detection (all-integer samples → BigInt) | ✅ |
| Schema inference by sampling (configurable `sampleSize`) | ✅ |
| Schema cache with configurable TTL | ✅ |
| Row count estimation via `DescribeTable` | ✅ |
| Auto-discovery of all DynamoDB tables | ✅ |
| Cross-source JOINs (DynamoDB ↔ Hudi, Delta, Iceberg, S3…) | ✅ |
| DynamoDB Local support (for dev/test) | ✅ |
| IAM / instance profile credential chain | ✅ |

---

## Prerequisites

| Requirement | Version |
|---|---|
| Dremio OSS | 26.0.5 (match `dremio.version` in `pom.xml`) |
| Amazon DynamoDB | Any region / DynamoDB Local |
| Java | 11+ (inside the Dremio container — host does not need Java) |
| Maven | 3.8+ (only if building from source) |
| Docker | Any recent version (for Docker install path) |

---

## Quick Start

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

### Option B — Build from source

```bash
./install.sh --docker try-dremio --build
```

After a successful build the compiled JAR is saved to `jars/` so future installs
can use `--prebuilt` without rebuilding.

### After installation

1. Wait ~30 seconds for Dremio to restart, then open **http://localhost:9047**
2. Go to **Sources → + Add Source → Amazon DynamoDB**
3. Fill in the required fields (see [Configuration Reference](#configuration-reference))
4. Click **Save**

Dremio immediately discovers all DynamoDB tables.

```sql
SELECT * FROM dynamodb_source.users LIMIT 10;
```

---

## Manual Deploy (without the installer)

```bash
# Copy the pre-built JAR into Dremio's plugin directory
cp jars/dremio-dynamodb-connector-1.0.0-SNAPSHOT-plugin.jar \
   $DREMIO_HOME/jars/3rdparty/

# Restart Dremio
docker restart try-dremio
# or: $DREMIO_HOME/bin/dremio restart
```

### Building from source manually

```bash
# 1. Install Dremio JARs into your local Maven repo
DREMIO_HOME=/opt/dremio
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
CALCITE_VER=1.22.0-202501311721330426-8b30dab9
JARS=$DREMIO_HOME/jars
TP=$JARS/3rdparty

install_jar() {
  mvn install:install-file \
    -Dfile="$1" -DgroupId="$2" -DartifactId="$3" -Dversion="$4" \
    -Dpackaging=jar -q
}

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar          com.dremio          dremio-sabot-kernel         $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar                com.dremio          dremio-common               $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar             com.dremio          dremio-connector            $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar         com.dremio          dremio-sabot-logical        $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar    com.dremio          dremio-sabot-vector-tools   $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar    com.dremio          dremio-services-namespace   $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar           com.dremio          dremio-common-core          $DREMIO_VER
install_jar $JARS/dremio-services-datastore-${DREMIO_VER}.jar    com.dremio          dremio-services-datastore   $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                    org.apache.arrow    arrow-vector                $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar               org.apache.arrow    arrow-memory-core           $ARROW_VER
install_jar $TP/calcite-core-${CALCITE_VER}.jar                  org.apache.calcite  calcite-core                $CALCITE_VER
install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar                org.apache.calcite  calcite-linq4j              $CALCITE_VER
install_jar $TP/guava-33.4.0-jre.jar                             com.google.guava    guava                       33.4.0-jre
install_jar $TP/javax.inject-1.jar                               javax.inject        javax.inject                1

# 2. Build
mvn package -DskipTests

# 3. Deploy
cp target/dremio-dynamodb-connector-1.0.0-SNAPSHOT-plugin.jar \
   $DREMIO_HOME/jars/3rdparty/
```

---

## Configuration Reference

All fields are set when adding or editing the source in the Dremio UI under
**Sources → Add Source → Amazon DynamoDB**.

| Field | Default | Description |
|---|---|---|
| **AWS Region** | `us-east-1` | AWS region where your DynamoDB tables are located |
| **Endpoint Override** | _(blank)_ | Override the DynamoDB endpoint URL. Use `http://localhost:8000` for DynamoDB Local. Leave blank to connect to AWS DynamoDB. |
| **Access Key ID** | _(blank)_ | AWS Access Key ID. Leave blank to use the default credential chain (IAM role, instance profile, environment variables, `~/.aws/credentials`) |
| **Secret Access Key** | _(blank)_ | AWS Secret Access Key. Leave blank to use the default credential chain. |
| **Schema Sample Size** | `100` | Number of items to scan per table for schema inference. Larger values improve accuracy but increase metadata refresh time. |
| **Split Parallelism** | `4` | Number of parallel scan segments per table. Higher values increase throughput on large tables (DynamoDB max: 1,000,000). |
| **Read Timeout (seconds)** | `30` | AWS SDK call timeout in seconds. Increase for slow networks or very large pages. |
| **Page Size** | `1000` | Maximum number of items returned per DynamoDB Scan/Query page. Controls memory pressure vs. round-trip count. |
| **Schema Cache TTL (seconds)** | `60` | How long to cache the inferred Arrow schema per table before re-sampling. Set to `0` to disable caching (always re-sample). |

---

## Upgrading Dremio

When you upgrade Dremio to a new version, the connector JAR must be recompiled against
the new Dremio JARs. Use `rebuild.sh` — it handles version detection, `pom.xml` update,
JAR installation, compilation, and deployment automatically.

```bash
# Docker (default container name: try-dremio)
./rebuild.sh --docker try-dremio

# Bare-metal
./rebuild.sh --local /opt/dremio

# Kubernetes pod
./rebuild.sh --k8s dremio-0

# Preview only — see what would change without touching anything
./rebuild.sh --dry-run
```

---

## Docker / Container Networking

For DynamoDB Local deployments, the DynamoDB Local container must be on the same
Docker network as Dremio and reachable by hostname.

```bash
# Create the network (one-time)
docker network create dremio-net

# Connect both containers
docker network connect dremio-net try-dremio
docker network connect dremio-net dynamodb-local

# Verify connectivity
docker exec try-dremio curl -s --max-time 3 http://dynamodb-local:8000 && echo "OK"
```

Set **Endpoint Override** to `http://dynamodb-local:8000` in the source config.
Never use an IP address — Docker may reassign IPs on container restart.

### DynamoDB Local data persistence

DynamoDB Local typically runs with `-inMemory` which means **all data is lost when the
container restarts**. You must re-seed your tables after any container restart.

If you also need to re-add the source to Dremio after a restart:

```bash
./add-dynamodb-source.sh \
  --name dynamodb_local \
  --region us-east-1 \
  --endpoint http://dynamodb-local:8000 \
  --access-key fakeKey \
  --secret-key fakeSecret \
  --force
```

### Forcing table discovery after source recreate

After deleting and recreating a source, Dremio's name discovery runs on a schedule
(default: every hour). To force immediate discovery:

1. Edit the source in the UI (Sources → dynamodb_local → ⚙ → Edit)
2. Change `namesRefreshMillis` via the API to `1000`, wait 20–30 seconds, restore it:

```bash
# Using add-dynamodb-source.sh --force will recreate and trigger rediscovery
./add-dynamodb-source.sh --name dynamodb_local --region us-east-1 \
  --endpoint http://dynamodb-local:8000 --access-key fakeKey --secret-key fakeSecret --force
```

---

## Troubleshooting

**Source doesn't appear in Add Source**
- Confirm the JAR is in `$DREMIO_HOME/jars/3rdparty/`
- Check Dremio startup logs: `docker logs try-dremio 2>&1 | grep -i dynamodb`
- Verify `sabot-module.conf` is inside the JAR: `jar tf dremio-dynamodb-connector-*-plugin.jar | grep sabot`
- Dremio must be fully restarted (not just refreshed) to pick up new JARs

**Tables don't appear in catalog (numberOfDatasets: 0)**
- Tables in DynamoDB must exist before Dremio's names refresh runs
- With the default 1-hour refresh interval, you may need to wait or force refresh
- Querying a specific table path (PREFETCH_QUERIED mode) promotes it immediately

**"Object 'tableName' not found within 'source'"**
- The table name isn't in Dremio's namespace yet — see above

**SELECT returns 0 rows**
- Verify the table has data in DynamoDB
- Check the source state (should be "good") and endpoint override

**Partition key filter not using Query mode**
- The partition key name must be stored in metadata (requires at least one metadata refresh after source creation)
- Verify with: `SELECT * FROM source.table WHERE pk_column = 'value'`; check Dremio query profile for "DynamoDBQuery" vs "DynamoDBScan"

**ClassNotFoundException on first query after deploy**
- Dremio requires a full restart to load new JARs: `docker restart try-dremio`
