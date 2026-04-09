# Dremio Amazon DynamoDB Connector — Test Results

**Date:** 2026-04-09  
**Dremio Version:** 26.0.5  
**DynamoDB Backend:** amazon/dynamodb-local (in-memory)  
**Test Suite:** `dremio-connector-tests/connectors/test_dynamodb.py`

---

## Summary

```
27 passed in 70.07s
```

**27 / 27 tests passed.**

---

## Test Tables

| Table | Schema | Rows | Key |
|---|---|---|---|
| `users` | user_id(S), name(S), age(N int), score(N float), active(BOOL), country(S), email(S) | 5 | PK: user_id |
| `orders` | order_id(S), user_id(S), total(N), status(S), items(N) | 6 | PK: order_id, SK: user_id |
| `products` | product_id(S), name(S), price(N), category(S), in_stock(BOOL), rating(N) | 6 | PK: product_id |
| `catalog` | category(S), item_id(S), name(S), tags(SS), scores(NS), price(N) | 6 | PK: category, SK: item_id |

---

## Test Results

### Standard battery (13/13)

| # | Test | Result |
|---|---|---|
| 01 | Source health check | ✅ PASSED |
| 02 | Schema — expected columns present | ✅ PASSED |
| 03 | Row count (COUNT *) | ✅ PASSED |
| 04 | SELECT * LIMIT | ✅ PASSED |
| 05 | Column projection | ✅ PASSED |
| 06 | WHERE equals filter (VARCHAR) | ✅ PASSED |
| 07 | WHERE numeric filter (DOUBLE/INT) | ✅ PASSED |
| 08 | WHERE boolean filter | ✅ PASSED |
| 09 | ORDER BY | ✅ PASSED |
| 10 | GROUP BY | ✅ PASSED |
| 11 | Aggregations (MIN, MAX, SUM, AVG, COUNT) | ✅ PASSED |
| 12 | LIMIT pushdown | ✅ PASSED |
| 13 | Cross-table JOIN | ✅ PASSED |

### DynamoDB-specific tests (7/7)

| Test | What it validates | Result |
|---|---|---|
| `test_pk_query_mode_returns_correct_row` | PK EQ → DynamoDB Query API; correct row returned | ✅ PASSED |
| `test_all_rows_returned_with_parallel_splits` | All 5 users returned with splitParallelism=2 | ✅ PASSED |
| `test_count_star_not_schema_change` | COUNT(*) doesn't trigger false schema-change (getVector regression) | ✅ PASSED |
| `test_orders_full_table_scan` | All 6 orders readable; multi-segment scan completeness | ✅ PASSED |
| `test_join_users_orders` | Cross-table JOIN; Alice Smith appears in result | ✅ PASSED |
| `test_avg_score` | AVG(score) = 81.74 across 5 users | ✅ PASSED |
| `test_in_filter_products` | IN list filter on category column | ✅ PASSED |

### New capability tests (7/7)

| Test | What it validates | Result |
|---|---|---|
| `test_ss_list_type_readable` | SS (String Set) → ARRAY\<VARCHAR\> readable in SQL | ✅ PASSED |
| `test_ns_list_type_readable` | NS (Number Set) → ARRAY\<FLOAT8\> readable in SQL | ✅ PASSED |
| `test_catalog_count` | Composite PK+SK table (catalog) returns all 6 rows | ✅ PASSED |
| `test_pk_filter_on_composite_key_table` | PK EQ Query mode on composite-key table | ✅ PASSED |
| `test_integer_n_type_detection` | N column with all-integer samples inferred as BIGINT | ✅ PASSED |
| `test_projection_pushdown` | SELECT subset of columns; no extra columns leaked | ✅ PASSED |
| `test_sort_key_range_predicate` | SK range predicate pushed as KeyConditionExpression | ✅ PASSED |

---

## Key Features Validated

- **Query mode**: `WHERE user_id = 'u1'` routes to DynamoDB Query API, not Scan
- **Sort key pushdown**: `WHERE category = 'accessories' AND item_id >= 'a1'` uses KeyConditionExpression
- **FilterExpression**: Non-key predicates pushed to DynamoDB to reduce data transfer
- **ProjectionExpression**: Only requested column names sent to DynamoDB
- **SS/NS types**: String Set and Number Set fields appear as Arrow List arrays
- **Integer detection**: `age` column (all integers in sample) inferred as BIGINT, not FLOAT8
- **Parallel scan**: splitParallelism=2 tested; all rows returned (no segment dropped)
- **COUNT(*)**: Works correctly without triggering false schema-change signal
- **Schema cache**: TTL-based caching avoids re-sampling on every metadata refresh
