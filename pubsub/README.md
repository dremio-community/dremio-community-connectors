# Dremio Google Cloud Pub/Sub Connector

A Dremio storage plugin that exposes Google Cloud Pub/Sub subscriptions as queryable tables.

## How it works

Each Pub/Sub **subscription** becomes a table in Dremio. When you run a query, the connector:
1. Pulls up to `defaultMaxMessages` messages from the subscription
2. Returns them as rows (with metadata + JSON payload fields)
3. **NACKs** (returns) all messages immediately — they stay in the subscription for other consumers

This "pull-without-ack" approach means every query sees the current backlog without consuming it. Multiple queries on the same subscription return the same messages (order may vary).

> **Important:** Create a **dedicated Dremio subscription** per topic. Do not share the subscription used by your CDC pipeline or applications. Concurrent pulls across subscriptions from the same topic each get their own copy of messages.

## Metadata columns

| Column | Type | Description |
|--------|------|-------------|
| `_subscription` | VARCHAR | Subscription name |
| `_message_id` | VARCHAR | Pub/Sub-assigned message ID |
| `_publish_time` | BIGINT | Publish timestamp (epoch milliseconds) |
| `_ordering_key` | VARCHAR | Message ordering key (empty if none) |
| `_attributes` | VARCHAR | Message attributes as JSON object |
| `_value_raw` | VARCHAR | Raw message data (UTF-8) |
| *(inferred)* | varies | Top-level JSON fields (JSON mode only) |

## Authentication

Priority order:
1. **Emulator** — set `Emulator Host` (e.g. `localhost:8085`) for local development
2. **Service Account** — set `Service Account Credentials File` to a JSON key file path
3. **ADC** — Application Default Credentials (`gcloud auth application-default login`, Workload Identity, etc.)

## Setup

### 1. Build

```bash
cd dremio-pubsub-connector
./rebuild.sh --force
```

### 2. Install

```bash
./install.sh try-dremio
# or manually:
docker cp jars/dremio-pubsub-connector-1.0.0-SNAPSHOT-plugin.jar \
    try-dremio:/opt/dremio/jars/3rdparty/
docker restart try-dremio
```

### 3. Add Source in Dremio UI

- Source Type: **Google Pub/Sub**
- GCP Project ID: `my-gcp-project`
- Credentials File: `/path/to/sa-key.json` (or blank for ADC)
- Schema Mode: `JSON`

### 4. Create a query subscription

```bash
# Create a subscription dedicated to Dremio queries
gcloud pubsub subscriptions create orders-dremio \
    --topic=orders \
    --project=my-gcp-project

# Verify messages are flowing
gcloud pubsub subscriptions pull orders-dremio --limit=5 --project=my-gcp-project
```

## Example queries

```sql
-- View latest messages with inferred fields
SELECT _message_id, _publish_time, order_id, customer, amount
FROM pubsub_source.`orders-dremio`
LIMIT 100;

-- Filter by publish time (client-side after pull)
SELECT *
FROM pubsub_source.`orders-dremio`
WHERE _publish_time > 1700000000000;

-- Parse attributes
SELECT _message_id,
       CONVERT_FROM(_attributes, 'JSON')['source'] AS event_source
FROM pubsub_source.`events-dremio`;

-- Aggregate over buffered messages
SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
FROM pubsub_source.`orders-dremio`
GROUP BY status;
```

## Local development with the emulator

```bash
# Start emulator + seed data
docker-compose up -d

# Run unit tests
mvn test

# Run emulator integration tests
PUBSUB_EMULATOR_HOST=localhost:8085 mvn test -Dgroups=emulator
```

The emulator is pre-configured with:
- Project: `test-project`
- Topics: `orders`, `events`
- Subscriptions: `orders-dremio`, `events-dremio`
- 5 order messages + 3 event messages seeded

## Configuration reference

| Field | Default | Description |
|-------|---------|-------------|
| GCP Project ID | *(required)* | GCP project containing the subscriptions |
| Service Account Credentials File | *(blank = ADC)* | Path to SA JSON key file |
| Emulator Host | *(blank = production)* | `host:port` of local emulator |
| Schema Mode | `JSON` | `JSON` (infer fields) or `RAW` (metadata only) |
| Schema Sample Messages | `20` | Messages to sample for JSON schema inference |
| Default Max Messages Per Scan | `1000` | Max messages returned per query |
| Pull Timeout (seconds) | `10` | Total budget for pulling messages per query |
| Subscription Include Pattern | *(all)* | Regex to show only matching subscriptions |
| Subscription Exclude Pattern | `_dremio_` | Regex to hide matching subscriptions |
| Metadata Cache TTL (seconds) | `60` | How long to cache schema per subscription |
