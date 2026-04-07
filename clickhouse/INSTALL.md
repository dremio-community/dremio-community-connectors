# Dremio ClickHouse Connector — Installation Guide

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Dremio OSS or Enterprise | 26.x (connector is version-pinned; rebuild for other versions) |
| Java 11+ | Required on the build machine if building from source |
| Maven 3.8+ | Required for build-from-source only; auto-installed in Docker if missing |
| `curl` | Required by `add-clickhouse-source.sh` and `test-connection.sh` |
| `python3` | Required by `add-clickhouse-source.sh` (for JSON payload building) |
| ClickHouse server | 22.x or later, HTTP interface enabled (port 8123 or 8443 for SSL) |

---

## Step 1 — Install the Connector JARs

> **Important:** The ClickHouse connector requires **two JARs** in Dremio's `jars/3rdparty/` directory:
> 1. `dremio-clickhouse-connector-1.0.0-SNAPSHOT-plugin.jar` — the connector plugin
> 2. `clickhouse-jdbc-0.4.6-nospi.jar` — the ClickHouse JDBC driver
>
> `install.sh` deploys both automatically. Do not deploy only one.

### Docker

```bash
# Default container name (try-dremio)
./install.sh --docker try-dremio

# Or just:
./install.sh
# (interactive — picks up running Dremio containers automatically)
```

To build from source inside the container instead of using the prebuilt JARs:

```bash
./install.sh --docker try-dremio --build
```

### Bare-metal / VM

```bash
./install.sh --local /opt/dremio
```

### Kubernetes

The installer auto-discovers all Running pods matching the same `app=` label as the
target pod (coordinator + all executors) and deploys to each.

```bash
# Deploy prebuilt JARs
./install.sh --k8s dremio-master-0

# With namespace
./install.sh --k8s dremio-master-0 --namespace dremio-ns
./install.sh --k8s dremio-master-0 -n dremio-ns

# Build from source inside the coordinator pod (requires Maven in pod)
./install.sh --k8s dremio-master-0 --build

# Override pod selector (default: auto-detected from app= label)
./install.sh --k8s dremio-master-0 -n dremio-ns --pod-selector app=dremio
```

> **⚠️ K8s Note:** The JARs are copied into the pod filesystem. If a pod is replaced by
> its StatefulSet (node failure, rolling update), the JARs will be lost. For persistence,
> mount `jars/3rdparty/` from a PVC, or add the JARs to a custom Dremio image (see `k8s/`).

### Manual

```bash
cp jars/dremio-clickhouse-connector-1.0.0-SNAPSHOT-plugin.jar \
   /opt/dremio/jars/3rdparty/
cp jars/clickhouse-jdbc-0.4.6-nospi.jar \
   /opt/dremio/jars/3rdparty/
# Then restart Dremio
```

---

## Step 2 — Restart Dremio

Dremio must restart to load the new JARs.

```bash
# Docker
docker restart try-dremio

# Bare-metal
systemctl restart dremio
# or: $DREMIO_HOME/bin/dremio restart

# Kubernetes (rolling restart — zero downtime if replicas > 1)
kubectl rollout restart statefulset/<name> -n <namespace>
```

Wait for Dremio to be ready (`http://localhost:9047` returns HTTP 200).

---

## Step 3 — Register a ClickHouse Source

```bash
# Simplest — local ClickHouse with defaults
./add-clickhouse-source.sh --name clickhouse --host localhost

# Named database + credentials
./add-clickhouse-source.sh \
  --name clickhouse_prod \
  --host clickhouse.example.com \
  --database analytics \
  --ch-user dremio_reader \
  --ch-pass mypassword

# ClickHouse Cloud (auto-sets port 8443 + SSL)
./add-clickhouse-source.sh \
  --name ch_cloud \
  --host abc123.us-east-1.aws.clickhouse.cloud \
  --cloud \
  --ch-pass "$CH_CLOUD_PASSWORD"

# Self-hosted HTTPS with SSL truststore
./add-clickhouse-source.sh \
  --name clickhouse_ssl \
  --host clickhouse.example.com \
  --port 8443 --ssl \
  --ssl-truststore /certs/truststore.jks \
  --ssl-ts-pass tspass

# Exclude system databases from the catalog
./add-clickhouse-source.sh \
  --name clickhouse \
  --host localhost \
  --exclude-dbs "system,information_schema,INFORMATION_SCHEMA"

# Preview the JSON payload without submitting
./add-clickhouse-source.sh --name clickhouse --host localhost --dry-run
```

Run `./add-clickhouse-source.sh --help` for the full list of options.

---

## Step 4 — Verify

```bash
# Run the smoke test suite
./test-connection.sh \
  --url http://localhost:9047 \
  --user dremio \
  --password dremio123 \
  --source clickhouse \
  --database default

# Quick manual check
curl -s -X POST http://localhost:9047/api/v3/sql \
  -H "Authorization: _dremio<TOKEN>" \
  -H "Content-Type: application/json" \
  -d '{"sql":"SELECT * FROM clickhouse.default.system.one"}'
```

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded, both JARs must be rebuilt against the new Dremio JARs.
`rebuild.sh` automates the entire process — version detection, `pom.xml` update, compile, deploy, restart:

```bash
# Docker (default container: try-dremio)
./rebuild.sh

# Named container
./rebuild.sh --docker my-dremio

# Bare-metal
./rebuild.sh --local /opt/dremio

# Kubernetes — auto-deploys to coordinator + all executor pods
./rebuild.sh --k8s dremio-master-0
./rebuild.sh --k8s dremio-master-0 --namespace dremio-ns

# Preview what would change without touching anything
./rebuild.sh --dry-run

# Force rebuild even if version strings already match (after source code edits)
./rebuild.sh --force
```

**What it does:**
1. Reads JAR filenames from the running instance to detect `dremio.version` and `arrow.version`
2. Compares against `pom.xml` — exits cleanly if nothing changed
3. Updates `pom.xml`, backs up the previous version to `pom.xml.bak`
4. Installs Dremio JARs from the running instance into the Maven local repo
5. Builds the connector from source against the new JARs
6. Deploys **both** new JARs (plugin + JDBC driver) to every pod / container
7. Restarts Dremio (Docker) or prints restart instructions (bare-metal / K8s)

---

## Troubleshooting

**Source type "ClickHouse" not in dropdown**
- Confirm both JARs are in `jars/3rdparty/`: `docker exec try-dremio ls /opt/dremio/jars/3rdparty/ | grep -E 'clickhouse'`
- Check Dremio startup logs: `docker logs try-dremio | grep -i clickhouse`
- Confirm `sabot-module.conf` is inside the plugin JAR: `jar tf dremio-clickhouse-connector-*-plugin.jar | grep sabot`

**"Connection refused" from Dremio to ClickHouse**
- ClickHouse's **HTTP port** must be reachable — the connector does NOT use the native TCP port (9000).
- Default HTTP port is `8123`. Use `8443` for HTTPS.
- For Docker: use `host.docker.internal` or the container IP instead of `localhost`.
- For K8s: use the ClickHouse service DNS name (`clickhouse.default.svc.cluster.local:8123`).

**"Authentication failed" / "Access denied"**
- Verify credentials directly: `curl "http://clickhouse:8123/?query=SELECT+1&user=dremio_reader&password=secret"`
- Ensure the user has `SELECT` privileges on the target database.

**Tables not appearing in catalog**
- Check **Excluded Databases** — `system` databases are excluded by default in many setups.
- Right-click the source in Dremio UI → **Refresh Metadata**.

**Queries timing out**
- Increase **Socket Timeout** in Advanced Options (default 300s). Long ClickHouse aggregations may need 600s+.
- Also check ClickHouse-side `max_execution_time` setting.

**Only the plugin JAR was deployed (forgot the driver JAR)**
- Deploy the missing JAR: `docker exec -u root try-dremio cp /path/to/clickhouse-jdbc-0.4.6-nospi.jar /opt/dremio/jars/3rdparty/`
- Restart Dremio. Both JARs must be present.

**K8s: JARs missing after pod restart**
- The pod filesystem is ephemeral. See the K8s note in Step 1 above.
- Use a custom Dremio image (see `k8s/Dockerfile`) or mount `jars/3rdparty/` from a PVC.
