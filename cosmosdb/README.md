# Dremio Azure Cosmos DB Connector

A native Dremio 26.x storage plugin that exposes Azure Cosmos DB (NoSQL API) containers as queryable SQL tables via the Cosmos DB REST API. No SDK or JDBC driver required — uses Java 11's built-in `HttpClient` with HMAC-SHA256 master key auth.

## Tables

Each container in the configured database is exposed as a Dremio table. Schema is inferred at metadata refresh time by sampling documents.

| Container | Exposed As | Schema |
|-----------|-----------|--------|
| Any Cosmos container | `cosmosdb.<container_name>` | Inferred from sampled documents |

Nested objects are flattened one level deep using underscore-joined keys:

```json
{ "contact": { "email": "a@b.com", "phone": "555-0100" } }
```
→ columns `contact_email`, `contact_phone`

Arrays and deeper-nested objects are serialized as JSON strings.

All `ArrowType.Timestamp` columns use millisecond precision and are timezone-naive (Dremio 26.x does not support timezone-aware timestamps in its built-in vector class lookup).

## Prerequisites

- Dremio 26.x (tested on `26.0.5`)
- Azure Cosmos DB account (or local emulator) with NoSQL API
- Master key (primary or secondary) from the Azure portal, **or** the Cosmos DB emulator (blank key)

## Installation

### Docker (recommended for development)

```bash
# Copy the JAR into Dremio's 3rdparty plugins directory
docker cp jars/dremio-cosmosdb-connector-1.0.0.jar try-dremio:/opt/dremio/jars/3rdparty/

# Restart Dremio to pick up the new plugin
docker restart try-dremio
```

### Bare-metal / Kubernetes

```bash
cp jars/dremio-cosmosdb-connector-1.0.0.jar /opt/dremio/jars/3rdparty/
# Then restart Dremio
```

## Configuration

### Via Dremio UI

1. Go to **Sources → Add Source → Azure Cosmos DB**
2. Fill in:
   - **Endpoint**: your Cosmos DB account endpoint (e.g. `https://myaccount.documents.azure.com:443`)
   - **Database**: the database name to expose
   - **Master Key**: primary or secondary key from the Azure portal (leave blank for the emulator)

### Via REST API

```bash
./add-cosmosdb-source.sh
```

Or directly:

```bash
TOKEN=$(curl -s -X POST http://localhost:9047/apiv2/login \
  -H 'Content-Type: application/json' \
  -d '{"userName":"your_user","password":"your_pass"}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

curl -X PUT http://localhost:9047/apiv2/source/cosmosdb \
  -H 'Content-Type: application/json' \
  -H "Authorization: _dremio${TOKEN}" \
  -d '{
    "name": "cosmosdb",
    "type": "COSMOS_DB",
    "config": {
      "endpoint": "https://myaccount.documents.azure.com:443",
      "database": "mydb",
      "masterKey": "YOUR_MASTER_KEY==",
      "pageSize": 100,
      "schemaSampleSize": 100,
      "queryTimeoutSeconds": 120
    }
  }'
```

### Configuration Options

| Field | Default | Description |
|-------|---------|-------------|
| `endpoint` | — | Cosmos DB account URL (HTTP or HTTPS) |
| `database` | — | Database name; all its containers become tables |
| `masterKey` | `""` | Primary/secondary key (blank for emulator) |
| `pageSize` | `100` | Documents per REST API page (max 1000) |
| `schemaSampleSize` | `100` | Documents sampled per container for schema inference |
| `queryTimeoutSeconds` | `120` | HTTP request timeout |

## Usage

```sql
-- List all containers (tables)
SHOW TABLES IN cosmosdb;

-- Query a container
SELECT id, status, subject, priority, assignee, created_at
FROM cosmosdb.tickets
WHERE status = 'open'
ORDER BY created_at DESC
LIMIT 20;

-- Access flattened nested fields
SELECT id, name, contact_email, contact_phone, tier
FROM cosmosdb.customers
WHERE tier = 'enterprise';

-- Aggregate over orders
SELECT status, COUNT(*) AS cnt, SUM(amount) AS total
FROM cosmosdb.orders
GROUP BY status;

-- Cross-source JOIN: Cosmos DB + Iceberg
SELECT t.id, t.subject, c.name AS company
FROM cosmosdb.tickets t
JOIN iceberg_catalog.crm.accounts c ON t.account_id = c.id;
```

## Local Emulator (Development)

The [Azure Cosmos DB Linux Emulator](https://aka.ms/cosmosdb-emulator-linux) (`vnext-preview` tag) runs on ARM64 (Apple Silicon):

```bash
# Start the emulator
docker run -d --name cosmos-emulator \
  -p 8081:8081 \
  mcr.microsoft.com/cosmosdb/linux/azure-cosmos-emulator:vnext-preview

# Add source pointing at the emulator (blank master key)
COSMOS_ENDPOINT=http://host.docker.internal:8081 \
COSMOS_DATABASE=mydb \
COSMOS_KEY="" \
./add-cosmosdb-source.sh
```

> **Note:** The `vnext-preview` emulator uses HTTP only (no TLS) and accepts any master key value, including blank.

## Building from Source

```bash
# Build against the running Dremio Docker container
./rebuild.sh

# Install the built JAR
./install.sh

# Add the source
./add-cosmosdb-source.sh
```

## Architecture

```
CosmosConf                    — source type COSMOS_DB, connection config
CosmosStoragePlugin           — registers plugin, lists containers, infers schema
  └─ CosmosConnection         — HTTP client, HMAC auth, pagination, schema inference
       └─ CosmosScanRule      — ScanCrel → CosmosScanDrel (logical)
            └─ CosmosScanPrule → CosmosScanPrel → CosmosGroupScan → CosmosSubScan
                                                        └─ CosmosScanCreator
                                                             └─ CosmosRecordReader
```

**Key implementation notes:**

- **Auth**: `type=master&ver=1.0&sig=base64(HMAC-SHA256(base64decode(key), "verb\nresourceType\nresourceLink\ndate\n\n"))` — all lowercase, URL-encoded.
- **Schema inference**: Queries `SELECT TOP N * FROM c`, removes system fields (`_rid`, `_self`, `_etag`, `_attachments`, `_ts`), merges field types across sampled documents.
- **Flattening**: Nested objects flattened one level with `_` separator. Deeper nesting and arrays → JSON string.
- **Pagination**: `x-ms-continuation` response header → passed as request header for the next page.
- **Vector writing**: Uses `mutator.getVector()` in `setup()` (never `addField()`).
- **Timestamps**: Detected by ISO-8601 format (`T` + `Z`/`+` suffix); stored as `ArrowType.Timestamp(MILLISECOND, null)`.
- **Operator type**: Both `GroupScan` and `SubScan` return `getOperatorType() = 0`.

## Type Mapping

| Cosmos DB value | Arrow type | Dremio SQL type |
|----------------|-----------|----------------|
| `string` (plain) | `Utf8` | `VARCHAR` |
| `string` (ISO-8601) | `Timestamp(MILLI, null)` | `TIMESTAMP` |
| `boolean` | `Bool` | `BOOLEAN` |
| `integer` / `long` | `Int(64, signed)` | `BIGINT` |
| `double` / `float` | `FloatingPoint(DOUBLE)` | `DOUBLE` |
| `object` (nested) | flattened or `Utf8` | `VARCHAR` (JSON) |
| `array` | `Utf8` | `VARCHAR` (JSON) |

## Limitations

- Read-only (SELECT only — no INSERT/UPDATE/DELETE)
- No predicate pushdown — filtering happens in Dremio after fetching
- One-level nested object flattening; deeper nesting serialized as JSON
- No cross-partition query fan-out — uses `x-ms-documentdb-query-enablecrosspartitionquery: true`
- Schema is inferred at source creation and refresh time; dynamic schema changes require a metadata refresh
