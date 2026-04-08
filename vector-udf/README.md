# Dremio Vector Distance UDF

Scalar UDFs that bring vector similarity and distance operations directly into Dremio SQL. Store embeddings as `VARCHAR` columns in Iceberg, Delta, or Hudi tables and query them with familiar SQL syntax.

## Available Functions

| Function | Description | Range |
|---|---|---|
| `COSINE_SIMILARITY(v1, v2)` | Cosine similarity | [-1, 1] |
| `COSINE_DISTANCE(v1, v2)` | 1 − cosine similarity | [0, 2] |
| `L2_DISTANCE(v1, v2)` | Euclidean distance | [0, ∞) |
| `L2_DISTANCE_SQUARED(v1, v2)` | Squared Euclidean (faster, no sqrt) | [0, ∞) |
| `DOT_PRODUCT(v1, v2)` | Inner product | (−∞, ∞) |
| `L1_DISTANCE(v1, v2)` | Manhattan distance | [0, ∞) |
| `VECTOR_NORM(v)` | L2 magnitude of a vector | [0, ∞) |
| `VECTOR_DIMS(v)` | Number of dimensions | integer |
| `VECTOR_DISTANCE(v1, v2, metric)` | Generic dispatcher | varies |

## Quick Start

```sql
-- Semantic search: top-10 most similar documents to a query embedding
SELECT
    id,
    text,
    COSINE_SIMILARITY(embedding, '[0.12, -0.45, 0.88, ...]') AS score
FROM my_catalog.embeddings
ORDER BY score DESC
LIMIT 10;

-- Find nearest neighbours by L2 distance
SELECT id, text, L2_DISTANCE(embedding, :query_vec) AS dist
FROM my_catalog.embeddings
ORDER BY dist ASC
LIMIT 10;

-- Generic dispatcher
SELECT VECTOR_DISTANCE(v1, v2, 'cosine')    -- cosine similarity
SELECT VECTOR_DISTANCE(v1, v2, 'l2')        -- euclidean
SELECT VECTOR_DISTANCE(v1, v2, 'dot')       -- dot product
SELECT VECTOR_DISTANCE(v1, v2, 'l1')        -- manhattan
```

## Vector Format

Vectors are `VARCHAR` columns containing JSON arrays:
```
"[0.12, -0.45, 0.88, 0.03, ...]"
```

This works with any embedding model output — OpenAI `text-embedding-3-small` (1536-d), Cohere `embed-english-v3` (1024-d), local Ollama models, etc.

## Installation

See [INSTALL.md](INSTALL.md).

## Use Cases

- **Semantic search** over document/product/log embeddings stored in your lakehouse
- **Recommendation systems** — find items similar to a given item vector
- **Anomaly detection** — measure distance from a cluster centroid
- **RAG pipelines** — retrieve relevant context chunks before LLM generation
- **Deduplication** — identify near-duplicate records by embedding similarity
