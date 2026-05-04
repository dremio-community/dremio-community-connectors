# Dremio Pinecone Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read support for Pinecone vector database indexes** via the Pinecone REST API. Each Pinecone index is exposed as a SQL table with `_id`, `values` (the embedding vector as a JSON array string), and any metadata fields — with types dynamically inferred by sampling.

---

## Architecture

```
SQL:  SELECT _id, name, price, category
      FROM pinecone_source.products
      WHERE category = 'electronics'

Dremio Planner
  └── PineconeScanRule / PineconeScanPrule
        └── PineconeGroupScan  (one split per index)
              └── PineconeRecordReader
                    ├── Control plane: GET /indexes  → discover index hosts
                    ├── Data plane:    GET /vectors/list  → paginate vector IDs
                    └── Data plane:    POST /vectors/fetch → fetch vectors in batches

PineconeConnection (REST client)
  └── Api-Key header auth
        └── Jackson JSON → Arrow vectors

PineconeTypeConverter
  └── Schema inference: sample 20 vectors, infer Utf8 / Float8 / Bool / BigInt
        └── _id always Utf8, values always Utf8 (JSON array string)
```

### Key Classes

| Class | Role |
|---|---|
| `PineconeConf` | Source config: API key (secret), control plane URL, namespace, page size, sample size, timeout |
| `PineconeConnection` | REST client — discovers indexes via control plane, fetches vectors via per-index data plane host |
| `PineconeRecordReader` | Paginates `list` → `fetch` to stream all vectors; writes Arrow fields |
| `PineconeTypeConverter` | Samples vectors to infer field types; `_id` and `values` always present |
| `PineconeGroupScan` | One `PineconeScanSpec` per index |
| `PineconeStoragePlugin` | Registers the connector; returns `PineconeConnection` |

### Single-JAR Deployment

All dependencies (Jackson, Guava, Protostuff) are provided by the Dremio runtime. The connector JAR is a thin plugin with no bundled driver.

---

## What's Implemented

| Feature | Status | Notes |
|---|---|---|
| **Index discovery** | ✅ Working | `GET /indexes` returns all indexes |
| **Schema inference** | ✅ Working | Samples up to 20 vectors; infers Utf8, Float8, Bool, BigInt |
| **`_id` field** | ✅ Working | Always present as Utf8 |
| **`values` field** | ✅ Working | Embedding vector serialized as JSON array string |
| **Metadata fields** | ✅ Working | All metadata fields discovered and typed |
| **Pagination** | ✅ Working | `list` API paginates with `paginationToken` |
| **Namespace filtering** | ✅ Working | Configurable namespace (default: empty = default namespace) |
| **WHERE / filter pushdown** | ❌ Not supported | Dremio evaluates filters post-fetch |
| **INSERT / UPDATE / DELETE** | ❌ Not supported | Read-only connector |

---

## Quick Start

### 1. Install the connector

```bash
# Docker (default container: try-dremio)
./install.sh --docker try-dremio --prebuilt

# Bare-metal
./install.sh --local /opt/dremio --prebuilt

# Kubernetes
./install.sh --k8s dremio-master-0
```

### 2. Add a Pinecone source in Dremio

Open `http://localhost:9047` → **Sources → + → Pinecone** and fill in:

| Field | Value |
|---|---|
| API Key | Your Pinecone API key |
| Control Plane URL | `https://api.pinecone.io` (default) |
| Namespace | Leave blank for default namespace |
| Page Size | `100` (vectors per list page) |
| Sample Size | `20` (vectors sampled for schema inference) |

### 3. Query

```sql
-- Browse your indexes as tables
SELECT * FROM pinecone_source.products LIMIT 10;

-- Filter by metadata
SELECT _id, name, price, category
FROM pinecone_source.products
WHERE category = 'electronics'
ORDER BY price DESC;

-- Aggregate over metadata
SELECT category, COUNT(*) AS cnt, AVG(price) AS avg_price
FROM pinecone_source.products
GROUP BY category ORDER BY avg_price DESC;

-- Access the raw embedding vector
SELECT _id, values FROM pinecone_source.products LIMIT 5;

-- Boolean metadata
SELECT _id, name, in_stock
FROM pinecone_source.products
WHERE in_stock = true;

-- Cross-source JOIN: Pinecone + Iceberg
SELECT p._id, p.name, p.price, i.segment
FROM pinecone_source.products p
JOIN iceberg_minio.analytics.segments i ON p._id = i.vector_id;
```

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| API Key | — | Pinecone API key (stored as secret) |
| Control Plane URL | `https://api.pinecone.io` | Pinecone control plane endpoint. Override for private deployments. |
| Namespace | _(blank)_ | Vector namespace to query. Leave blank for the default namespace. |
| Page Size | `100` | Number of vector IDs per `list` pagination request |
| Sample Size | `20` | Number of vectors fetched during schema inference |
| Query Timeout (seconds) | `60` | HTTP request timeout |

---

## Building

```bash
mvn clean package -DskipTests
# Deployable JAR:
jars/dremio-pinecone-connector-1.0.0.jar
```

---

## Smoke Tests

25 tests (standard battery + Pinecone-specific) in the companion test harness:

```bash
cd dremio-connector-tests
python3 -m pytest connectors/test_pinecone.py -v
```

**Result: 25/25 tests passing.**

Test coverage includes:
- `_id` and `values` field presence
- Type inference (Float8 price, Bool in_stock/published, BigInt word_count)
- Metadata filtering, aggregations (AVG, SUM, COUNT, MIN, MAX)
- ORDER BY + LIMIT, GROUP BY, exact row/ID counts

---

## References

- [Pinecone REST API Documentation](https://docs.pinecone.io/reference/api/introduction)
- [Pinecone Data Plane API](https://docs.pinecone.io/reference/api/data-plane/list)
