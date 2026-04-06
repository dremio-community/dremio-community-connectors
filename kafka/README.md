# Dremio Apache Kafka Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read support for Apache Kafka topics** as SQL tables.

Each Kafka topic appears as a table under the source. Queries are **bounded snapshot reads** —
partition offsets are frozen at plan time, giving every query deterministic, repeatable results.

---

## What Works

| Feature | Status | Notes |
|---------|--------|-------|
| **SELECT on any topic** | ✅ Working | Metadata columns + JSON field inference |
| **JSON schema inference** | ✅ Working | Samples N messages per partition; infers column types |
| **AVRO decoding** | ✅ Working | Full Avro → Arrow type mapping; Schema Registry required |
| **RAW mode** | ✅ Working | Returns metadata + `_value_raw` bytes; no schema needed |
| **Filter pushdown** | ✅ Working | `_partition`, `_offset`, `_timestamp` filters pushed to scan |
| **CTAS to Iceberg** | ✅ Working | Snapshot a topic to Iceberg via `CREATE TABLE ... AS SELECT` |
| **SASL auth** | ✅ Working | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 |
| **TLS / SSL** | ✅ Working | Truststore (broker cert) + keystore (mTLS client cert) |
| **Schema Registry auth** | ✅ Working | Basic auth + Confluent Cloud API key/secret |
| **Multi-broker clusters** | ✅ Working | Comma-separated bootstrap servers |
| **Topic auto-discovery** | ✅ Working | Configurable include/exclude regex patterns |
| **Kubernetes deployment** | ✅ Working | Auto-discovers coordinator + executor pods, deploys to all |
| **INSERT INTO** | ❌ Not supported | Kafka as a sink is not implemented |

---

## Metadata Columns

Every Kafka table exposes these system columns regardless of schema mode:

| Column | Type | Description |
|--------|------|-------------|
| `_topic` | VARCHAR | Topic name |
| `_partition` | INT | Partition index |
| `_offset` | BIGINT | Message offset within partition |
| `_timestamp` | TIMESTAMP | Message timestamp (producer or broker) |
| `_key` | VARCHAR | Message key (null if not set) |
| `_value_raw` | VARCHAR | Raw message value as string |
| `_headers` | VARCHAR | Kafka headers as JSON object |

JSON and AVRO modes additionally expose all inferred / decoded schema fields as columns.

---

## Quick Start

### 1. Install the connector

```bash
# Docker
./install.sh --docker try-dremio

# Bare-metal
./install.sh --local /opt/dremio

# Kubernetes
./install.sh --k8s dremio-master-0
./install.sh --k8s dremio-master-0 --namespace dremio-ns
```

### 2. Add a Kafka source

```bash
# Local Kafka, JSON mode (simplest)
./add-kafka-source.sh --name kafka --brokers localhost:9092

# SASL_SSL + SCRAM-SHA-512 (common production setup)
./add-kafka-source.sh \
  --name kafka_prod \
  --brokers broker1:9092,broker2:9092 \
  --security SASL_SSL \
  --sasl-mech SCRAM-SHA-512 \
  --sasl-user myuser \
  --sasl-pass mypassword \
  --ssl-truststore /path/to/truststore.jks \
  --ssl-ts-pass truststorepass

# AVRO mode + Confluent Schema Registry
./add-kafka-source.sh \
  --name kafka_avro \
  --brokers localhost:9092 \
  --schema-mode AVRO \
  --schema-registry http://localhost:8081

# Confluent Cloud
./add-kafka-source.sh \
  --name confluent \
  --brokers pkc-xxxxx.us-east-1.aws.confluent.cloud:9092 \
  --security SASL_SSL \
  --sasl-mech PLAIN \
  --sasl-user $CONFLUENT_API_KEY \
  --sasl-pass $CONFLUENT_API_SECRET \
  --schema-mode AVRO \
  --schema-registry https://psrc-xxxxx.us-east-2.aws.confluent.cloud \
  --schema-registry-user $SR_API_KEY \
  --schema-registry-pass $SR_API_SECRET
```

### 3. Query

```sql
-- List all topics
SHOW TABLES IN kafka;

-- Read a topic (JSON mode)
SELECT _partition, _offset, _timestamp, user_id, event_type
FROM kafka.clickstream
WHERE _partition = 0
ORDER BY _offset
LIMIT 100;

-- Aggregate
SELECT
  _partition,
  COUNT(*) AS msg_count,
  MIN(_offset) AS first_offset,
  MAX(_offset) AS last_offset
FROM kafka.orders
GROUP BY _partition;

-- Snapshot to Iceberg
CREATE TABLE iceberg.snapshots.orders_20260406
AS SELECT * FROM kafka.orders;
```

---

## Schema Modes

| Mode | Flag | When to use |
|------|------|-------------|
| **JSON** (default) | `--schema-mode JSON` | Topics with JSON messages; Dremio infers columns from samples |
| **AVRO** | `--schema-mode AVRO` | Topics with Avro-encoded messages; requires Schema Registry |
| **RAW** | `--schema-mode RAW` | Unknown/binary format; only metadata columns + `_value_raw` |

---

## Security

### SASL Authentication

```bash
# SASL_PLAINTEXT (no TLS) — dev/internal only
./add-kafka-source.sh --security SASL_PLAINTEXT --sasl-mech PLAIN ...

# SASL_SSL (TLS + auth) — production
./add-kafka-source.sh --security SASL_SSL --sasl-mech SCRAM-SHA-512 ...
```

### TLS / mTLS

```bash
# One-way TLS (verify broker cert only)
./add-kafka-source.sh \
  --ssl-truststore /path/to/truststore.jks \
  --ssl-ts-pass password ...

# mTLS (mutual — also send client cert to broker)
./add-kafka-source.sh \
  --ssl-truststore /path/to/truststore.jks --ssl-ts-pass tspass \
  --keystore /path/to/keystore.jks --keystore-pass kspass ...
```

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded, run `rebuild.sh` to detect the new version, update `pom.xml`,
rebuild the JAR, and redeploy — all in one command:

```bash
./rebuild.sh                                    # Docker (default: try-dremio)
./rebuild.sh --docker my-dremio                 # Named Docker container
./rebuild.sh --local /opt/dremio                # Bare-metal
./rebuild.sh --k8s dremio-master-0              # Kubernetes
./rebuild.sh --k8s dremio-master-0 -n dremio-ns # Kubernetes with namespace
./rebuild.sh --dry-run                          # Preview only, no changes
```

See [INSTALL.md](INSTALL.md#keeping-up-with-dremio-upgrades) for full details.

---

## Local Development Environment

`docker-compose.yml` starts a complete local test stack:

```bash
docker compose up -d
# Wait ~60s for Dremio to be ready, then:
./install.sh --docker dremio
docker restart dremio
./add-kafka-source.sh --name kafka_test --brokers localhost:9092
./test-connection.sh --topic orders
./test-avro.sh --topic orders_avro
```

| Service | URL | Credentials |
|---------|-----|-------------|
| Dremio | http://localhost:9047 | dremio / dremio123 |
| Schema Registry | http://localhost:8081 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka | localhost:9092 | — |

Pre-seeded topics: `orders` (500 JSON messages, 4 partitions), `events` (200 keyed messages + tombstones, 2 partitions), `orders_avro`.

---

## Tooling Reference

| Script | Purpose |
|--------|---------|
| `install.sh` | Install connector JAR into Dremio (Docker / bare-metal / K8s) |
| `rebuild.sh` | Detect Dremio version change, rebuild JAR, redeploy |
| `add-kafka-source.sh` | Register a Kafka source in Dremio via REST API |
| `test-connection.sh` | 27-test JSON smoke suite |
| `test-avro.sh` | 20-test AVRO + Schema Registry smoke suite |
| `refresh-schema.sh` | Trigger metadata refresh on an existing Kafka source |
| `docker-compose.yml` | Full local test environment (Kafka, Schema Registry, MinIO, Dremio) |

---

## Architecture

### Bounded Snapshot Reads

Kafka topics are infinite streams, but SQL queries must be deterministic. At planning time,
Dremio freezes the latest partition offsets via `AdminClient.listOffsets(OffsetSpec.latest())`.
Each query reads exactly `[frozenStart, frozenEnd)` — messages arriving after planning starts
are never included. The frozen ranges are serialized into each `KafkaScanSpec`.

### Filter Pushdown

`_partition`, `_offset`, and `_timestamp` filters are pushed into the scan spec before
any data is fetched. A `WHERE _partition = 0 AND _offset BETWEEN 100 AND 500` clause
causes only partition 0 to be assigned to readers, and the reader seeks directly to offset 100
rather than scanning from the beginning.

### Schema Inference (JSON mode)

On first access, the connector samples up to N messages per partition (configurable),
parses them as JSON, and unions all keys into a schema. The schema is cached for
`metadataCacheTtlSeconds` seconds. String, long, double, boolean, and null types are
detected. Nested objects and arrays are surfaced as VARCHAR.

### AVRO Decoding

In AVRO mode, each message's schema ID is extracted from the Confluent wire format
(magic byte + 4-byte schema ID), fetched from the Schema Registry (cached), and used
to deserialize the message via `GenericDatumReader`. Avro primitive types map to Arrow
types; nullable unions (`[null, T]`) produce nullable Arrow vectors; complex types
(records, arrays, maps) surface as VARCHAR.

---

## References

### Dremio Plugin API
- [StoragePlugin.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/StoragePlugin.java)
- [AbstractRecordReader.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/AbstractRecordReader.java)
- [SupportsListingDatasets.java](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/SupportsListingDatasets.java)

### Apache Kafka
- [Kafka Java Client](https://kafka.apache.org/documentation/#api)
- [AdminClient.listOffsets](https://kafka.apache.org/documentation/javadoc/org/apache/kafka/clients/admin/AdminClient.html)
- [KafkaConsumer](https://kafka.apache.org/documentation/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html)

### Confluent Schema Registry
- [Schema Registry API Reference](https://docs.confluent.io/platform/current/schema-registry/develop/api.html)
- [Wire Format](https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format)

### Apache Avro
- [Avro Specification](https://avro.apache.org/docs/current/spec.html)
- [GenericDatumReader](https://avro.apache.org/docs/current/api/java/org/apache/avro/generic/GenericDatumReader.html)
