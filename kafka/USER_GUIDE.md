# Dremio Apache Kafka Connector — User Guide

Query Apache Kafka topics as tables directly from Dremio SQL. Each query takes a bounded, deterministic snapshot of the topic — messages are never missed or duplicated regardless of when they arrive.

---

## Table of Contents

1. [How It Works](#how-it-works)
2. [Prerequisites](#prerequisites)
3. [Installation](#installation)
4. [Adding a Kafka Source in Dremio](#adding-a-kafka-source-in-dremio)
   - [Connection](#connection)
   - [Schema](#schema)
   - [Scan Window](#scan-window)
   - [Topic Filtering](#topic-filtering)
   - [Security](#security)
   - [SSL / TLS](#ssl--tls)
   - [Advanced](#advanced)
5. [Metadata Columns](#metadata-columns)
6. [Schema Modes](#schema-modes)
7. [Writing Queries](#writing-queries)
8. [Filter Pushdown](#filter-pushdown)
9. [Confluent Cloud Setup](#confluent-cloud-setup)
10. [Refreshing a Topic Schema](#refreshing-a-topic-schema)
11. [Troubleshooting](#troubleshooting)

---

## How It Works

The connector uses the Kafka Java client in **manual partition assignment** mode — it never joins a consumer group, so it never interferes with your application consumers or affects committed offsets.

At query planning time, Dremio calls `ListOffsets(LATEST)` on every partition and freezes those offsets as the query's end boundary. Each partition becomes an independent scan split. This means:

- Every `SELECT` on a topic is a **bounded snapshot** — results are deterministic and repeatable.
- Messages arriving after planning starts are not included in that query.
- Scans on different partitions run in parallel across Dremio's executors.

---

## Prerequisites

- Apache Kafka 0.10.x or later (compatible with Kafka clients 3.6.x)
- Dremio 26.x running in Docker, bare-metal, or Kubernetes
- Network connectivity from Dremio executor nodes to all Kafka brokers

---

## Installation

```bash
cd dremio-kafka-connector

# Interactive installer (prompts for options)
./install.sh

# Or non-interactive:
./install.sh --docker try-dremio --prebuilt   # use included pre-built JAR
./install.sh --docker try-dremio --build       # compile from source inside container
./install.sh --local /opt/dremio --prebuilt    # bare-metal installation
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes pod
```

After installation, restart Dremio. The "Apache Kafka" source type will appear in **Sources → +**.

---

## Adding a Kafka Source in Dremio

Navigate to **Sources → + → Apache Kafka**. Fill in the sections below.

---

### Connection

#### Bootstrap Servers
**Required.** Comma-separated list of Kafka broker addresses in `host:port` format.

```
broker1.example.com:9092,broker2.example.com:9092,broker3.example.com:9092
```

Kafka only needs one reachable broker to discover the full cluster — but listing 2–3 guards against a single broker being temporarily unavailable at connection time. You do **not** need to list every broker.

For Docker/local development: `localhost:9092`
For Confluent Cloud: `pkc-abc123.us-east-1.aws.confluent.cloud:9092`

---

### Schema

#### Schema Mode
Controls how the connector exposes Kafka message values as columns.

| Mode | Description | Use when |
|------|-------------|----------|
| **JSON** *(default)* | Samples recent messages and infers top-level JSON field types. Inferred fields appear as named columns alongside metadata columns. Non-JSON messages leave payload columns null. | Messages are JSON objects with a consistent schema |
| **RAW** | No schema inference. Exposes only the 9 metadata columns plus `_value_raw`. Fast and always works. | Binary messages, mixed formats, or when you just need the raw bytes |
| **AVRO** | Fetches the Avro schema from a Confluent-compatible Schema Registry. Typed columns derived from the schema. Decodes Confluent wire-format messages (magic byte + schema ID prefix). | Messages serialized with `KafkaAvroSerializer` and a Schema Registry |

#### Schema Sample Records
*(JSON mode only)* Number of recent messages to sample **per partition** for schema inference.

- Default: `100`
- The connector reads up to this many messages from the end of each partition, then merges the inferred types across all samples by majority vote.
- Increase if your topic has high field variability across messages. Decrease for faster source creation on very large topics.

#### Schema Registry URL
*(AVRO mode only)* The base URL of your Confluent-compatible Schema Registry.

```
http://schema-registry:8081                              # internal Docker/k8s
https://psrc-abc123.us-east-1.aws.confluent.cloud        # Confluent Cloud
```

Leave blank for RAW or JSON modes.

#### Schema Registry Username
*(AVRO mode only)* Username for HTTP Basic authentication against the Schema Registry.

- For **Confluent Cloud**: enter the Schema Registry **API Key**
- For an **unauthenticated** (open) registry: leave blank

#### Schema Registry Password
*(AVRO mode only)* Password for Schema Registry Basic auth.

- For **Confluent Cloud**: enter the Schema Registry **API Secret**
- Stored encrypted in Dremio's secret store

#### Disable Schema Registry SSL Hostname Verification
*(AVRO + HTTPS only)* When checked, disables TLS certificate and hostname verification for Schema Registry connections.

> **Warning:** Only enable this for internal development environments with self-signed certificates. Never enable in production.

---

### Scan Window

#### Default Max Records Per Partition
Limits how many records a plain `SELECT * FROM source.topic` scan returns per partition when no explicit offset filter is provided.

- Default: `10000`
- The connector seeks to `max(earliest, latest - N)` and reads forward to `latest`.
- **Set to `0`** to read from the earliest available offset (full history). Be cautious on high-volume topics — this may return billions of rows.

This limit applies only to unbounded scans. Queries with explicit `WHERE _offset >= X` or `WHERE _timestamp >= X` filters bypass this limit and use the pushed-down range.

---

### Topic Filtering

#### Topic Exclude Pattern (regex)
Java regex pattern. Topics matching this pattern are hidden from Dremio's catalog.

- Default: `^__` — hides all internal Kafka topics (those starting with `__`, e.g. `__consumer_offsets`, `__transaction_state`)
- Example: `^(__|\\.confluent)` — also hides Confluent Schema Registry internal topics
- Leave blank to show all topics

#### Topic Include Pattern (regex)
Optional Java regex. When set, **only** matching topics appear in the Dremio catalog.

- Example: `^(orders|customers|products)` — show only these three topics
- Example: `^prod-` — show only topics with the `prod-` prefix
- Leave blank to include all non-excluded topics

Both patterns are evaluated: a topic is visible only if it matches the include pattern (if set) AND does not match the exclude pattern.

---

### Security

#### Security Protocol
The Kafka security protocol to use for all connections (broker and consumer).

| Value | Description |
|-------|-------------|
| `PLAINTEXT` *(default)* | No authentication, no encryption. Use for local development only. |
| `SSL` | TLS encryption only. No authentication. Broker identity verified via truststore. |
| `SASL_PLAINTEXT` | SASL authentication, no TLS encryption. Credentials sent in plain text — use only on trusted internal networks. |
| `SASL_SSL` | SASL authentication **+** TLS encryption. **Recommended for production.** |

#### SASL Mechanism
*(Required when Security Protocol is `SASL_PLAINTEXT` or `SASL_SSL`)*

| Value | Description |
|-------|-------------|
| `PLAIN` | Simple username/password. Widely supported. Credentials are base64-encoded (not encrypted) — use with `SASL_SSL` in production. |
| `SCRAM-SHA-256` | Password-based auth with SCRAM challenge-response. More secure than PLAIN. |
| `SCRAM-SHA-512` | Same as SCRAM-SHA-256 but with a stronger hash. |

#### SASL Username / SASL Password
Credentials for PLAIN or SCRAM authentication.

- For **Confluent Cloud**: use the Kafka cluster **API Key** (username) and **API Secret** (password)
- For **AWS MSK** with SASL/SCRAM: use the credentials stored in AWS Secrets Manager
- Password is stored encrypted in Dremio's secret store

---

### SSL / TLS

#### SSL Truststore Path
Path to a JKS or PKCS12 truststore file that contains the CA certificate(s) used to verify the Kafka broker's TLS certificate.

```
/etc/dremio/certs/kafka-truststore.jks
```

- Leave blank to use the JVM's default trust store (works for brokers with publicly trusted certificates, e.g. Confluent Cloud)
- The path must be accessible from **all Dremio executor nodes**, not just the coordinator

#### SSL Truststore Password
Password for the truststore file. Leave blank if the truststore has no password.

#### SSL Truststore Type
File format of the truststore. `JKS` (default) or `PKCS12`.

#### SSL Keystore Path
*(For mutual TLS only)* Path to a JKS or PKCS12 keystore containing the **client's** private key and certificate.

Only needed when the Kafka broker is configured to require client authentication (mutual TLS / mTLS). Leave blank for standard one-way TLS.

#### SSL Keystore Password / SSL Keystore Type
Password and format for the keystore file.

#### Disable SSL Hostname Verification
When checked, sets `ssl.endpoint.identification.algorithm=""` which disables TLS hostname verification.

> **Warning:** This allows connecting to a broker whose certificate CN/SAN does not match its hostname. Only use for self-signed certificates in internal development environments.

---

### Advanced

#### Max Poll Records
Maximum number of records returned by a single Kafka `poll()` call.

- Default: `500`
- Higher values increase throughput but use more memory per executor thread.
- Lower values reduce memory pressure on wide rows or large message values.

#### Request Timeout (ms)
Timeout in milliseconds for Kafka metadata requests and consumer poll calls.

- Default: `30000` (30 seconds)
- Increase for brokers with high latency (e.g. cross-region Confluent Cloud clusters)

#### Metadata Cache TTL (seconds)
How long to cache topic metadata (partition counts, latest offsets, inferred schemas) locally in Dremio.

- Default: `60` seconds
- Set to `0` to always fetch fresh metadata (useful for rapidly evolving topics in development)
- Cached metadata is invalidated immediately when you run `ALTER TABLE ... REFRESH METADATA`

---

## Metadata Columns

Every Kafka topic exposes these 9 columns regardless of schema mode:

| Column | Type | Description |
|--------|------|-------------|
| `_topic` | VARCHAR | Kafka topic name |
| `_partition` | INT | Partition number (0-based) |
| `_offset` | BIGINT | Message offset within the partition |
| `_timestamp` | BIGINT | Message timestamp in epoch milliseconds |
| `_timestamp_type` | VARCHAR | `CREATE_TIME` (set by producer) or `LOG_APPEND_TIME` (set by broker) |
| `_key` | VARCHAR | Message key decoded as UTF-8 string (null if no key) |
| `_headers` | VARCHAR | Message headers as a flat JSON object: `{"header-name":"value"}` |
| `_value_raw` | VARCHAR | Raw message bytes decoded as UTF-8 (always populated) |
| `_schema_id` | INT | Confluent Schema Registry schema ID (populated when message starts with Confluent magic byte 0x00; null otherwise) |

In JSON mode, additional columns from the inferred schema are appended after these.
In AVRO mode, typed columns from the Avro schema are appended after these.

---

## Schema Modes

### JSON Mode

The connector samples up to `sampleRecordsForSchema` recent messages per partition and infers column types by majority vote. For example, if 90 of 100 sampled messages have `{"amount": 99.99}` and 10 have `{"amount": null}`, `amount` becomes a nullable DOUBLE column.

Type mapping:

| JSON type | Arrow / Dremio type |
|-----------|---------------------|
| `true`/`false` | BOOLEAN |
| Integer number | BIGINT |
| Decimal number | DOUBLE |
| String | VARCHAR |
| Object, Array | VARCHAR (JSON string) |

Non-JSON messages in a JSON-mode topic leave all payload columns null but still populate the metadata columns including `_value_raw`.

### RAW Mode

Only the 9 metadata columns are exposed. No sampling is performed. Use this when:
- Messages are binary (Protobuf, Avro without a registry URL, custom format)
- You want the raw bytes and will decode them yourself in SQL using `CONVERT_FROM`
- Topic creation is very slow due to large message volume and you don't need schema inference

### AVRO Mode

The connector fetches the **latest** Avro schema for `{topic}-value` from the Schema Registry at source-creation time and maps each field to an Arrow type:

| Avro type | Arrow / Dremio type |
|-----------|---------------------|
| `null` | (field skipped) |
| `boolean` | BOOLEAN |
| `int` | INT |
| `long` | BIGINT |
| `float` | FLOAT |
| `double` | DOUBLE |
| `string`, `enum` | VARCHAR |
| `bytes`, `fixed` | VARCHAR (hex-encoded) |
| `record`, `array`, `map` | VARCHAR (JSON string) |
| `["null", T]` union | Nullable T |
| `[T1, T2, …]` union | VARCHAR (JSON string) |

At execution time, each message's schema ID (from the Confluent wire-format prefix) is used to fetch and cache the writer schema. Schema evolution is handled gracefully: fields present in the writer but not the reader are ignored; fields in the reader but not the writer produce null values.

---

## Writing Queries

### Basic select

```sql
-- Most recent 10,000 records per partition (default scan window)
SELECT * FROM kafka_source."my-topic";

-- Select specific columns
SELECT _offset, _timestamp, order_id, customer, amount
FROM kafka_source."orders"
ORDER BY _timestamp DESC;
```

### Querying headers

```sql
-- Extract a specific header value
SELECT
    _offset,
    CONVERT_FROM(_headers, 'JSON')['x-request-id'] AS request_id,
    CONVERT_FROM(_headers, 'JSON')['content-type'] AS content_type
FROM kafka_source."events";
```

### Querying nested JSON in _value_raw

```sql
-- When using RAW mode, parse JSON manually
SELECT
    _offset,
    _timestamp,
    CONVERT_FROM(_value_raw, 'JSON')['order_id'] AS order_id,
    CAST(CONVERT_FROM(_value_raw, 'JSON')['amount'] AS DOUBLE) AS amount
FROM kafka_source."orders";
```

### Aggregations

```sql
-- Message count per partition
SELECT _partition, COUNT(*) AS msg_count
FROM kafka_source."orders"
GROUP BY _partition
ORDER BY _partition;

-- Revenue by status (AVRO or JSON mode)
SELECT status, COUNT(*) AS orders, SUM(amount) AS total_revenue
FROM kafka_source."orders"
GROUP BY status;
```

### Time-based analysis

```sql
-- Messages from the last hour
SELECT *
FROM kafka_source."events"
WHERE _timestamp >= UNIX_TIMESTAMP(NOW() - INTERVAL '1' HOUR) * 1000;

-- Messages in a specific time window
WHERE _timestamp >= 1700000000000   -- epoch ms
  AND _timestamp <  1700003600000;
```

---

## Filter Pushdown

The connector pushes three types of predicates directly into the Kafka scan, enabling seek-based access instead of scan-and-filter:

### Partition filter

```sql
-- Read only from partition 0 — all other partitions are skipped entirely at planning time
WHERE _partition = 0
```

Partition filtering is resolved at **planning time** — skipped partitions produce zero scan splits and consume no executor resources.

### Offset range

```sql
-- Read messages from offset 1000 onward in each partition
WHERE _offset >= 1000

-- Read only messages at offsets 500 through 999
WHERE _offset >= 500 AND _offset < 1000
```

The connector seeks the Kafka consumer directly to the start offset, skipping all earlier messages with zero polling overhead.

### Timestamp range

```sql
-- Messages produced on or after a specific time
WHERE _timestamp >= 1700000000000

-- Messages in a 1-hour window (using TIMESTAMP literal)
WHERE _timestamp >= TIMESTAMP '2023-11-15 00:00:00'
  AND _timestamp <  TIMESTAMP '2023-11-15 01:00:00'
```

Timestamp filters are resolved via `KafkaConsumer.offsetsForTimes()` at **execution time**. The consumer finds the first offset with timestamp ≥ the requested value and seeks directly to it — no records before the window are polled.

> **Note:** All pushed-down filters are also kept as residual filters that Dremio applies after reading. Results are always correct regardless of pushdown effectiveness.

---

## Confluent Cloud Setup

### Kafka Broker

| Field | Value |
|-------|-------|
| Bootstrap Servers | `pkc-<id>.<region>.<cloud>.confluent.cloud:9092` |
| Security Protocol | `SASL_SSL` |
| SASL Mechanism | `PLAIN` |
| SASL Username | Kafka API Key |
| SASL Password | Kafka API Secret |

### Schema Registry (AVRO mode)

| Field | Value |
|-------|-------|
| Schema Registry URL | `https://psrc-<id>.<region>.<cloud>.confluent.cloud` |
| Schema Registry Username | Schema Registry API Key |
| Schema Registry Password | Schema Registry API Secret |

No truststore configuration is needed — Confluent Cloud uses publicly trusted certificates that the JVM accepts by default.

---

## Refreshing a Topic Schema

When a JSON topic's message schema evolves (new fields added, types changed), Dremio may cache the old schema. Force a refresh with:

```bash
./refresh-schema.sh \
  --url http://localhost:9047 \
  --user admin \
  --source kafka_source \
  --topic orders

# Refresh all topics in a source at once
./refresh-schema.sh --source kafka_source --all
```

Alternatively, in Dremio SQL:
```sql
ALTER TABLE kafka_source."orders" REFRESH METADATA;
```

For AVRO topics: the Schema Registry client uses a TTL-based cache (default 60 seconds controlled by **Metadata Cache TTL**). After the TTL expires, the next query automatically fetches the latest schema. You can also set **Metadata Cache TTL** to `0` to always use the latest schema.

---

## Troubleshooting

### Source shows "bad state" after creation

**Check:** Can Dremio reach the brokers? The connector calls `AdminClient.listTopics()` during `start()`. If this times out, the source enters bad state.

```bash
# From the Dremio container, test connectivity:
docker exec try-dremio bash -c "curl -v telnet://broker1:9092" 2>&1 | head -5
```

**Common causes:**
- Bootstrap server address is wrong or uses a hostname not resolvable from Dremio containers
- Firewall blocks port 9092 (or the configured port)
- SASL credentials are incorrect — check Dremio server logs for `SaslAuthenticationException`

### Topics not appearing in catalog

- Check **Topic Exclude Pattern** — the default `^__` hides all `__`-prefixed topics
- Check **Topic Include Pattern** — if set, only matching topics appear
- The catalog may need a refresh: right-click the source in Dremio UI → **Refresh Metadata**

### "No records returned" from a query

- The topic may be empty (`earliest == latest`). Check with `kafka-consumer-groups.sh --describe`
- **Default Max Records Per Partition** limits how far back the scan reaches. If older messages were already past the window, set this to `0` or use an explicit `WHERE _offset >= 0` filter
- Filter pushdown may have produced an empty window. Check `_timestamp` filter values — use epoch milliseconds (13 digits), not seconds (10 digits)

### AVRO mode: "Schema not found" errors

- Verify the Schema Registry URL is reachable from Dremio executors (not just the coordinator)
- The connector looks for `{topic}-value` subject. If your schema uses a different naming strategy, the subject may not exist
- Check authentication: a 401 response from the registry appears as a schema fetch failure in the logs

### Headers show as null

- Kafka message producers must explicitly set headers. Most frameworks don't set headers by default.
- Headers are decoded as UTF-8 strings. Binary header values may appear garbled — use `_value_raw` and decode manually.

### Schema shows stale fields after topic evolution

Run `./refresh-schema.sh` or `ALTER TABLE ... REFRESH METADATA`. The metadata cache TTL (default 60s) controls how long old schemas are retained.
