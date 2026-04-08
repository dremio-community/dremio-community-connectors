# Dremio Vector Distance UDF

Scalar UDFs that bring vector similarity, distance, and arithmetic operations directly into Dremio SQL. Store embeddings as `VARCHAR` columns in Iceberg, Delta, or Hudi tables and query them with familiar SQL syntax.

## Available Functions

### Similarity & Distance
| Function | Description | Output Range |
|---|---|---|
| `COSINE_SIMILARITY(v1, v2)` | Cosine similarity | [-1, 1] |
| `COSINE_DISTANCE(v1, v2)` | 1 − cosine similarity | [0, 2] |
| `L2_DISTANCE(v1, v2)` | Euclidean distance | [0, ∞) |
| `L2_DISTANCE_SQUARED(v1, v2)` | Squared Euclidean — faster for ranking | [0, ∞) |
| `DOT_PRODUCT(v1, v2)` | Inner product | (−∞, ∞) |
| `L1_DISTANCE(v1, v2)` | Manhattan distance | [0, ∞) |
| `VECTOR_DISTANCE(v1, v2, metric)` | Generic dispatcher (`'cosine'`,`'l2'`,`'dot'`,`'l1'`) | varies |

### Arithmetic
| Function | Description | Returns |
|---|---|---|
| `VECTOR_ADD(v1, v2)` | Element-wise addition | VARCHAR |
| `VECTOR_SUBTRACT(v1, v2)` | Element-wise subtraction | VARCHAR |
| `VECTOR_SCALE(v, scalar)` | Multiply every element by a scalar | VARCHAR |

### Slicing & Indexing
| Function | Description | Returns |
|---|---|---|
| `VECTOR_SLICE(v, start, end)` | Sub-vector extraction (Matryoshka support) | VARCHAR |
| `VECTOR_ELEMENT_AT(v, index)` | Single element by index (negative indexing supported) | DOUBLE |

### Utility
| Function | Description | Returns |
|---|---|---|
| `VECTOR_NORM(v)` | L2 magnitude | DOUBLE |
| `VECTOR_NORMALIZE(v)` | Unit-normalize (divide by norm) | VARCHAR |
| `VECTOR_DIMS(v)` | Number of dimensions | INT |
| `VECTOR_IS_VALID(v)` | Is it a valid, parseable vector? | INT (1/0) |

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

-- Matryoshka embeddings: use only the first 256 dims of a 1536-dim vector
SELECT id, COSINE_SIMILARITY(
    VECTOR_SLICE(embedding, 0, 256),
    VECTOR_SLICE(:query_vec, 0, 256)
) AS score
FROM my_catalog.embeddings
ORDER BY score DESC LIMIT 10;

-- Compute centroid of two vectors
SELECT VECTOR_SCALE(VECTOR_ADD('[1.0,0.0]', '[0.0,1.0]'), 0.5)
-- → '[0.5,0.5]'

-- Data quality: validate and inspect embeddings
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN VECTOR_IS_VALID(embedding) = 0 THEN 1 ELSE 0 END) AS invalid,
    AVG(VECTOR_DIMS(embedding)) AS avg_dims,
    SUM(CASE WHEN ABS(VECTOR_NORM(embedding) - 1.0) > 0.01 THEN 1 ELSE 0 END) AS not_normalized
FROM my_catalog.embeddings;
```

## Vector Format

Vectors are `VARCHAR` columns containing JSON arrays:
```
"[0.12, -0.45, 0.88, 0.03, ...]"
```

Compatible with any embedding model — OpenAI `text-embedding-3-small` (1536-d), Cohere `embed-english-v3` (1024-d), local Ollama models, etc.

## Installation

See [INSTALL.md](INSTALL.md).

## Use Cases

- **Semantic search** over document/product/log embeddings stored in your lakehouse
- **Recommendation systems** — find items similar to a given item vector
- **Anomaly detection** — measure distance from a cluster centroid
- **RAG pipelines** — retrieve relevant context chunks before LLM generation
- **Matryoshka embeddings** — truncate to lower dimensions with `VECTOR_SLICE`
- **Centroid computation** — combine `VECTOR_ADD` + `VECTOR_SCALE` for cluster math
- **Data quality** — validate, inspect, and normalize embeddings at scale
- **Deduplication** — identify near-duplicate records by embedding similarity
