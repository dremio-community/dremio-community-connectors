# Dremio Apache Kafka Connector — Installation Guide

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Dremio OSS or Enterprise | 26.x (connector is version-pinned; rebuild for other versions) |
| Java 11+ | Required on the build machine if building from source |
| Maven 3.8+ | Required for build-from-source only; auto-installed in Docker if missing |
| `curl` | Required by `add-kafka-source.sh` and `test-connection.sh` |
| `python3` | Required by `add-kafka-source.sh` (for JSON payload building) |
| Kafka cluster | Reachable from Dremio's network |

---

## Step 1 — Install the Connector JAR

The connector ships as a single fat JAR (`dremio-kafka-connector-1.0.0-SNAPSHOT-plugin.jar`).
It must be placed in Dremio's `jars/3rdparty/` directory and Dremio must be restarted.

### Docker

```bash
# Default container name (try-dremio)
./install.sh --docker try-dremio

# Or just:
./install.sh
```

To build from source inside the container instead of using the prebuilt JAR:

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
# Deploy prebuilt JAR
./install.sh --k8s dremio-master-0

# With namespace
./install.sh --k8s dremio-master-0 --namespace dremio-ns
./install.sh --k8s dremio-master-0 -n dremio-ns

# Build from source inside the coordinator pod (requires Maven in pod)
./install.sh --k8s dremio-master-0 --build

# Override pod selector (default: auto-detected from app= label)
./install.sh --k8s dremio-master-0 -n dremio-ns --pod-selector app=dremio
```

> **⚠️ K8s Note:** The JAR is copied into the pod filesystem. If a pod is replaced by
> its StatefulSet (node failure, rolling update), the JAR will be lost. For persistence,
> mount `jars/3rdparty/` from a PVC, or add the JAR to a custom Dremio image.

After installing, `install.sh` prints the exact `kubectl rollout restart` command
for your StatefulSet.

---

## Step 2 — Restart Dremio

Dremio must restart to load the new JAR.

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

## Step 3 — Register a Kafka Source

```bash
# Minimal — local Kafka, JSON mode
./add-kafka-source.sh --name kafka --brokers localhost:9092

# Multi-broker + SASL_SSL
./add-kafka-source.sh \
  --name kafka_prod \
  --brokers broker1:9092,broker2:9092 \
  --security SASL_SSL \
  --sasl-mech SCRAM-SHA-512 \
  --sasl-user myuser \
  --sasl-pass mypassword \
  --ssl-truststore /certs/truststore.jks \
  --ssl-ts-pass truststorepass

# AVRO mode with Schema Registry
./add-kafka-source.sh \
  --name kafka_avro \
  --brokers localhost:9092 \
  --schema-mode AVRO \
  --schema-registry http://localhost:8081

# RAW mode (binary/unknown payloads)
./add-kafka-source.sh \
  --name kafka_raw \
  --brokers localhost:9092 \
  --schema-mode RAW

# Confluent Cloud
./add-kafka-source.sh \
  --name confluent \
  --brokers pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
  --security SASL_SSL \
  --sasl-mech PLAIN \
  --sasl-user "$CONFLUENT_API_KEY" \
  --sasl-pass "$CONFLUENT_API_SECRET" \
  --schema-mode AVRO \
  --schema-registry https://psrc-xxxxx.us-east-2.aws.confluent.cloud \
  --schema-registry-user "$SR_API_KEY" \
  --schema-registry-pass "$SR_API_SECRET"

# Preview the JSON payload without submitting
./add-kafka-source.sh --name kafka --brokers localhost:9092 --dry-run
```

Run `./add-kafka-source.sh --help` for the full list of options.

---

## Step 4 — Verify

```bash
# Run the smoke test suite (prompts for Dremio credentials and topic name)
./test-connection.sh

# Non-interactive
./test-connection.sh \
  --url http://localhost:9047 \
  --user dremio \
  --password dremio123 \
  --source kafka \
  --topic orders \
  --skip-ctas

# AVRO tests (requires Schema Registry)
./test-avro.sh \
  --source kafka_avro \
  --topic orders_avro \
  --schema-registry http://localhost:8081
```

---

## Local Test Environment

`docker-compose.yml` spins up a complete stack with Kafka, Schema Registry, MinIO, and Dremio:

```bash
docker compose up -d
# Wait ~60 seconds for all services to be healthy

./install.sh --docker dremio
docker restart dremio
# Wait ~60 seconds for Dremio to restart

./add-kafka-source.sh --name kafka_test --brokers localhost:9092
./test-connection.sh --source kafka_test --topic orders --skip-ctas

# Or with S3/MinIO CTAS:
./test-connection.sh \
  --source kafka_test \
  --topic orders \
  --s3-source iceberg_minio \
  --s3-bucket dremio-test

# Tear down
docker compose down        # keeps volumes
docker compose down -v     # full reset
```

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded, the connector JAR must be rebuilt against the new Dremio JARs.
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
./rebuild.sh --k8s dremio-master-0 --namespace dremio-ns   # with namespace

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
6. Deploys the new JAR to every pod / container
7. Restarts Dremio (Docker) or prints restart instructions (bare-metal / K8s)

If the build fails, `rebuild.sh` restores `pom.xml` automatically and prints a diagnostic
guide for common Dremio API-change errors.

---

## Troubleshooting

**Source doesn't appear in Add Source**
- Confirm the JAR is in `jars/3rdparty/`: `docker exec try-dremio ls /opt/dremio/jars/3rdparty/ | grep kafka`
- Check Dremio startup logs: `docker logs try-dremio | grep -i kafka`
- Confirm `sabot-module.conf` is inside the JAR: `jar tf dremio-kafka-connector-*-plugin.jar | grep sabot`

**"Connection refused" from Dremio to Kafka**
- The Kafka broker address must be reachable from inside the Dremio container/pod, not just from your laptop.
- For Docker: use `host.docker.internal` (Mac/Windows) or the container's gateway IP instead of `localhost`.
- For K8s: use the Kafka service DNS name (`kafka.default.svc.cluster.local:9092`).

**Schema inference returns no columns (JSON mode)**
- Confirm messages are valid JSON: `kafka-console-consumer.sh --bootstrap-server ... --topic ... --max-messages 3`
- Increase `--sample` count: `./add-kafka-source.sh ... --sample 500`
- If messages are AVRO-encoded but source is in JSON mode, switch to `--schema-mode AVRO`

**AVRO decoding fails with "Schema not found"**
- Confirm the Schema Registry is reachable from Dremio: `curl http://schema-registry:8081/subjects`
- Confirm the topic subject exists: `curl http://schema-registry:8081/subjects | grep <topic>-value`
- Check Schema Registry credentials if using auth.

**Filter pushdown not working (full scan despite WHERE clause)**
- Run `EXPLAIN PLAN FOR SELECT ...` and look for `KafkaScan` in the plan.
- Filters on metadata columns (`_partition`, `_offset`, `_timestamp`) push down.
- Filters on JSON/AVRO data columns do not push down (they are applied post-scan).

**ClassNotFoundException on first query after deploy**
- Dremio needs a full restart (not just a metadata refresh) to load new JARs.
- `docker restart try-dremio`

**K8s: JAR missing after pod restart**
- The pod filesystem is ephemeral. See K8s Note in Step 1 above.
- Add the JAR to a custom Dremio Docker image, or mount `jars/3rdparty/` from a PVC.

**Build fails with "artifact not found"**
- Run `rebuild.sh` which installs the Dremio JARs into Maven local repo automatically.
- Or run the JAR install step manually as shown in `install.sh --build` output.
