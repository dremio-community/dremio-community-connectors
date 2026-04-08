# User Guide — Dremio Vector UDF

## Function Reference

### COSINE_SIMILARITY(vec1, vec2) → DOUBLE
Measures the angle between two vectors. Best for semantic similarity.
- Returns **1.0** — identical direction (most similar)
- Returns **0.0** — orthogonal (unrelated)
- Returns **-1.0** — opposite direction

```sql
SELECT COSINE_SIMILARITY(
  '[0.12, 0.45, -0.33]',
  '[0.11, 0.44, -0.31]'
) -- → ~0.999 (very similar)
```

### COSINE_DISTANCE(vec1, vec2) → DOUBLE
`1 − cosine_similarity`. Use when you want *lower = more similar* (e.g. `ORDER BY dist ASC`).

```sql
SELECT COSINE_DISTANCE(embedding, :query) AS dist
FROM docs ORDER BY dist ASC LIMIT 10
```

### L2_DISTANCE(vec1, vec2) → DOUBLE
Euclidean distance — straight-line distance in embedding space.

```sql
SELECT L2_DISTANCE('[0.0, 0.0]', '[3.0, 4.0]') -- → 5.0
```

### L2_DISTANCE_SQUARED(vec1, vec2) → DOUBLE
Same as L2 but skips the `sqrt`. **Use this for ranking** — it's faster and preserves order.

```sql
-- Faster nearest-neighbour ranking
SELECT id, L2_DISTANCE_SQUARED(embedding, :query) AS dist
FROM docs ORDER BY dist ASC LIMIT 10
```

### DOT_PRODUCT(vec1, vec2) → DOUBLE
Raw inner product. For **unit-normalized** vectors this is equal to cosine similarity and is the fastest metric.

```sql
-- If your embeddings are pre-normalized (norm = 1.0)
SELECT id, DOT_PRODUCT(embedding, :query) AS score
FROM docs ORDER BY score DESC LIMIT 10
```

### L1_DISTANCE(vec1, vec2) → DOUBLE
Manhattan / "taxicab" distance. Sum of absolute element differences.

### VECTOR_NORM(vec) → DOUBLE
L2 magnitude of a vector. A value of `1.0` means the vector is unit-normalized.

```sql
SELECT id, VECTOR_NORM(embedding) AS magnitude
FROM docs WHERE VECTOR_NORM(embedding) < 0.99  -- find un-normalized rows
```

### VECTOR_DIMS(vec) → INT
Returns the number of dimensions. Useful for validation.

```sql
SELECT VECTOR_DIMS('[0.1, 0.2, 0.3]') -- → 3
```

### VECTOR_DISTANCE(vec1, vec2, metric) → DOUBLE
Generic dispatcher. Supported metric strings:

| metric string | Equivalent function |
|---|---|
| `'cosine'` or `'cosine_similarity'` | `COSINE_SIMILARITY` |
| `'cosine_distance'` | `COSINE_DISTANCE` |
| `'l2'` or `'euclidean'` | `L2_DISTANCE` |
| `'l2_squared'` | `L2_DISTANCE_SQUARED` |
| `'dot'` or `'dot_product'` | `DOT_PRODUCT` |
| `'l1'` or `'manhattan'` | `L1_DISTANCE` |

---

## Common Patterns

### Semantic Search

```sql
-- Find the 10 most semantically similar documents to a query embedding
SELECT
    doc_id,
    title,
    COSINE_SIMILARITY(embedding, '[0.12, -0.45, 0.88, ...]') AS score
FROM lakehouse.documents
WHERE score > 0.75
ORDER BY score DESC
LIMIT 10;
```

### Nearest Neighbours with Metadata Join

```sql
-- Find similar products and join product metadata
SELECT
    p.product_id,
    p.name,
    p.price,
    L2_DISTANCE_SQUARED(e.embedding, :query_vec) AS dist
FROM lakehouse.product_embeddings e
JOIN lakehouse.products p ON e.product_id = p.product_id
WHERE p.in_stock = TRUE
ORDER BY dist ASC
LIMIT 20;
```

### Anomaly Detection

```sql
-- Find records far from their cluster centroid
SELECT
    event_id,
    timestamp,
    L2_DISTANCE(embedding, centroid) AS dist_from_center
FROM lakehouse.log_embeddings
WHERE L2_DISTANCE(embedding, centroid) > 1.5
ORDER BY dist_from_center DESC;
```

### Validate Embedding Quality

```sql
-- Check that all embeddings are unit-normalized (norm ≈ 1.0)
SELECT
    COUNT(*) AS total,
    SUM(CASE WHEN ABS(VECTOR_NORM(embedding) - 1.0) > 0.01 THEN 1 ELSE 0 END) AS not_normalized,
    AVG(VECTOR_DIMS(embedding)) AS avg_dims
FROM lakehouse.embeddings;
```

### Choosing a Metric

| Use case | Recommended metric | Why |
|---|---|---|
| General semantic similarity | `COSINE_SIMILARITY` | Scale-invariant |
| Pre-normalized embeddings | `DOT_PRODUCT` | Fastest, equals cosine for unit vecs |
| Clustering / spatial | `L2_DISTANCE` | True geometric distance |
| Ranking only (no absolute value needed) | `L2_DISTANCE_SQUARED` | Avoids sqrt, same ranking |
| Sparse/high-dimensional features | `L1_DISTANCE` | More robust to outlier dimensions |
