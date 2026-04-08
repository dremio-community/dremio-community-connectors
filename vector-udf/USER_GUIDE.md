# User Guide — Dremio Vector UDF

## Function Reference

---

### Similarity & Distance

#### COSINE_SIMILARITY(vec1, vec2) → DOUBLE
Measures the angle between two vectors. Best for semantic similarity.
- **1.0** = identical direction (most similar)
- **0.0** = orthogonal (unrelated)
- **-1.0** = opposite direction

```sql
SELECT COSINE_SIMILARITY('[0.12, 0.45, -0.33]', '[0.11, 0.44, -0.31]') -- → ~0.999
```

#### COSINE_DISTANCE(vec1, vec2) → DOUBLE
`1 − cosine_similarity`. Lower = more similar. Use with `ORDER BY dist ASC`.

```sql
SELECT COSINE_DISTANCE(embedding, :query) AS dist
FROM docs ORDER BY dist ASC LIMIT 10
```

#### L2_DISTANCE(vec1, vec2) → DOUBLE
Euclidean (straight-line) distance in embedding space.

```sql
SELECT L2_DISTANCE('[0.0, 0.0]', '[3.0, 4.0]') -- → 5.0
```

#### L2_DISTANCE_SQUARED(vec1, vec2) → DOUBLE
Same ordering as L2 but skips the `sqrt` — **faster for ranking**.

```sql
SELECT id, L2_DISTANCE_SQUARED(embedding, :query) AS dist
FROM docs ORDER BY dist ASC LIMIT 10
```

#### DOT_PRODUCT(vec1, vec2) → DOUBLE
Raw inner product. For **unit-normalized** vectors this equals cosine similarity and is the fastest metric.

```sql
-- Pre-normalized embeddings: dot product == cosine similarity
SELECT id, DOT_PRODUCT(embedding, :query) AS score
FROM docs ORDER BY score DESC LIMIT 10
```

#### L1_DISTANCE(vec1, vec2) → DOUBLE
Manhattan / "taxicab" distance. Sum of absolute element differences.

#### VECTOR_DISTANCE(vec1, vec2, metric) → DOUBLE
Generic dispatcher — pass the metric as a string literal.

| metric string | Equivalent function |
|---|---|
| `'cosine'` or `'cosine_similarity'` | `COSINE_SIMILARITY` |
| `'cosine_distance'` | `COSINE_DISTANCE` |
| `'l2'` or `'euclidean'` | `L2_DISTANCE` |
| `'l2_squared'` | `L2_DISTANCE_SQUARED` |
| `'dot'` or `'dot_product'` | `DOT_PRODUCT` |
| `'l1'` or `'manhattan'` | `L1_DISTANCE` |

---

### Arithmetic

#### VECTOR_ADD(vec1, vec2) → VARCHAR
Element-wise addition. Both vectors must have the same dimensions.

```sql
SELECT VECTOR_ADD('[1.0, 2.0, 3.0]', '[4.0, 5.0, 6.0]') -- → '[5.0,7.0,9.0]'
```

#### VECTOR_SUBTRACT(vec1, vec2) → VARCHAR
Element-wise subtraction.

```sql
SELECT VECTOR_SUBTRACT('[5.0, 7.0]', '[1.0, 2.0]') -- → '[4.0,5.0]'
```

#### VECTOR_SCALE(vec, scalar) → VARCHAR
Multiply every element by a scalar value.

```sql
SELECT VECTOR_SCALE('[1.0, 2.0, 3.0]', 2.0)  -- → '[2.0,4.0,6.0]'
SELECT VECTOR_SCALE('[2.0, 4.0, 6.0]', 0.5)  -- → '[1.0,2.0,3.0]'
```

**Tip:** Combine `VECTOR_ADD` + `VECTOR_SCALE` for centroid math:
```sql
-- Midpoint between two vectors
SELECT VECTOR_NORMALIZE(
    VECTOR_SCALE(VECTOR_ADD(vec_a, vec_b), 0.5)
)
```

---

### Slicing & Indexing

#### VECTOR_SLICE(vec, start, end) → VARCHAR
Extract a sub-vector from index `start` (inclusive) to `end` (exclusive).
Supports **Matryoshka embeddings** — truncate to lower dimensions for faster search at lower accuracy.

```sql
-- First 256 dims of a 1536-dim OpenAI embedding
SELECT VECTOR_SLICE(embedding, 0, 256)

-- Middle section
SELECT VECTOR_SLICE('[0.1, 0.2, 0.3, 0.4, 0.5]', 1, 4) -- → '[0.2,0.3,0.4]'
```

#### VECTOR_ELEMENT_AT(vec, index) → DOUBLE
Returns the element at the given zero-based index. Supports negative indexing.

```sql
SELECT VECTOR_ELEMENT_AT('[0.1, 0.2, 0.9]', 0)   -- → 0.1  (first)
SELECT VECTOR_ELEMENT_AT('[0.1, 0.2, 0.9]', -1)  -- → 0.9  (last)
SELECT VECTOR_ELEMENT_AT('[0.1, 0.2, 0.9]', 2)   -- → 0.9
```

---

### Utility

#### VECTOR_NORM(vec) → DOUBLE
L2 magnitude of a vector. `1.0` = unit-normalized.

```sql
SELECT VECTOR_NORM('[3.0, 4.0]')  -- → 5.0

-- Find un-normalized embeddings
SELECT id FROM docs WHERE ABS(VECTOR_NORM(embedding) - 1.0) > 0.01
```

#### VECTOR_NORMALIZE(vec) → VARCHAR
Returns a unit-normalized copy (each element divided by the L2 norm).
After normalizing, `DOT_PRODUCT` equals `COSINE_SIMILARITY` and is faster.

```sql
SELECT VECTOR_NORMALIZE('[3.0, 4.0]')            -- → '[0.6,0.8]'
SELECT VECTOR_NORM(VECTOR_NORMALIZE('[3.0,4.0]')) -- → 1.0 (always)
```

#### VECTOR_DIMS(vec) → INT
Returns the number of dimensions.

```sql
SELECT VECTOR_DIMS('[0.1, 0.2, 0.3]') -- → 3
```

#### VECTOR_IS_VALID(vec) → INT (1 or 0)
Returns 1 if the string is a parseable, non-empty vector. Safe on any VARCHAR — never throws.

```sql
SELECT VECTOR_IS_VALID('[0.1, 0.2, 0.3]') -- → 1
SELECT VECTOR_IS_VALID('[]')              -- → 0
SELECT VECTOR_IS_VALID('bad input')       -- → 0
SELECT VECTOR_IS_VALID(NULL)              -- → 0

-- Filter out bad rows before querying
SELECT * FROM docs WHERE VECTOR_IS_VALID(embedding) = 1
```

---

## Common Patterns

### Semantic Search

```sql
SELECT
    doc_id, title,
    COSINE_SIMILARITY(embedding, '[0.12, -0.45, 0.88, ...]') AS score
FROM lakehouse.documents
ORDER BY score DESC
LIMIT 10;
```

### Matryoshka Embedding Search (faster, lower dims)

```sql
-- Use first 256 of 1536 dims — ~6x faster, minimal accuracy loss
SELECT id, title,
    COSINE_SIMILARITY(
        VECTOR_SLICE(embedding, 0, 256),
        VECTOR_SLICE('[...1536-d query vec...]', 0, 256)
    ) AS score
FROM lakehouse.documents
ORDER BY score DESC LIMIT 10;
```

### Nearest Neighbours with Metadata Join

```sql
SELECT p.product_id, p.name, p.price,
    L2_DISTANCE_SQUARED(e.embedding, :query_vec) AS dist
FROM lakehouse.product_embeddings e
JOIN lakehouse.products p ON e.product_id = p.product_id
WHERE p.in_stock = TRUE
ORDER BY dist ASC LIMIT 20;
```

### Centroid Computation

```sql
-- Compute average of two vectors (works with VECTOR_SCALE + VECTOR_ADD)
SELECT VECTOR_SCALE(
    VECTOR_ADD(vec_a, vec_b),
    0.5
) AS midpoint
FROM my_table;
```

### Anomaly Detection

```sql
SELECT event_id, timestamp,
    L2_DISTANCE(embedding, centroid) AS dist_from_center
FROM lakehouse.log_embeddings
WHERE L2_DISTANCE(embedding, centroid) > 1.5
ORDER BY dist_from_center DESC;
```

### Data Quality Audit

```sql
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN VECTOR_IS_VALID(embedding) = 0 THEN 1 ELSE 0 END) AS invalid,
    SUM(CASE WHEN ABS(VECTOR_NORM(embedding) - 1.0) > 0.01 THEN 1 ELSE 0 END) AS not_normalized,
    MIN(VECTOR_DIMS(embedding)) AS min_dims,
    MAX(VECTOR_DIMS(embedding)) AS max_dims
FROM lakehouse.embeddings;
```

---

## Choosing a Metric

| Use case | Recommended | Why |
|---|---|---|
| General semantic similarity | `COSINE_SIMILARITY` | Scale-invariant |
| Pre-normalized embeddings | `DOT_PRODUCT` | Fastest — equals cosine for unit vecs |
| Clustering / spatial | `L2_DISTANCE` | True geometric distance |
| Ranking only | `L2_DISTANCE_SQUARED` | No sqrt, same ordering |
| Sparse / high-dimensional | `L1_DISTANCE` | More robust to outlier dims |
| Matryoshka truncation | `VECTOR_SLICE` + any metric | Reduce dims before distance calc |
