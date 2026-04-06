# Dremio Apache Kafka Connector — Test Results

---

## Latest Run — Full Connector Validation

*Run 2026-04-06 (after filter pushdown, AVRO mode, Schema Registry, mTLS, K8s support)*

### Smoke Test — 14 / 14 Passed ✅ (`test-connection.sh` — original suite)

| # | Test | Result |
|---|------|--------|
| 1 | SELECT * LIMIT 5 | ✅ |
| 2 | SELECT _topic, _partition, _offset, _timestamp | ✅ |
| 3 | _key column accessible | ✅ |
| 4 | _value_raw column accessible | ✅ |
| 5 | _headers column accessible | ✅ |
| 6 | COUNT(*) returns a number | ✅ |
| 7 | WHERE _partition = 0 | ✅ |
| 8 | WHERE _offset >= 0 | ✅ |
| 9 | WHERE _timestamp IS NOT NULL | ✅ |
| 10 | SELECT specific columns only | ✅ |
| 11 | ORDER BY _offset | ✅ |
| 12 | GROUP BY _partition | ✅ |
| 13 | MIN/MAX offset per partition | ✅ |
| 14 | CTAS to Iceberg on MinIO | ✅ |

**Environment:** Dremio OSS 26.0.5 in Docker; Kafka 3.7 (KRaft, no ZooKeeper); 4-partition `orders` topic with 500 JSON records.

---

## Extended Test Suite (27 tests)

The extended `test-connection.sh` adds tests 15–27 covering EXPLAIN PLAN / filter pushdown verification, range filters, compound filters, key nullability, DISTINCT, edge cases, CTEs, subqueries, CAST, and timestamp ranges. These require a running Dremio + Kafka environment matching `docker-compose.yml`.

| # | Test | What It Validates |
|---|------|-------------------|
| 15 | EXPLAIN: KafkaScan in plan | Connector scan node is used (not a fallback) |
| 16 | EXPLAIN: partition filter pushed into KafkaScan | Filter pushdown confirmed in query plan |
| 17 | WHERE _offset BETWEEN 0 AND 1000 | BETWEEN range filter |
| 18 | WHERE _partition = 0 AND _offset >= 0 | Compound filter pushdown |
| 19 | WHERE _key IS NULL | Tombstone / no-key message handling |
| 20 | WHERE _key IS NOT NULL | Keyed message filtering |
| 21 | SELECT DISTINCT _partition | Partition discovery |
| 22 | LIMIT 0 → rowCount = 0 | Empty result set edge case |
| 23 | CTE (WITH clause) | Complex query composition |
| 24 | Subquery (inline view) | Nested query support |
| 25 | CAST(_value_raw AS VARCHAR) | Explicit type casting |
| 26 | WHERE _timestamp >= NOW() - INTERVAL '1' DAY | Timestamp range filter |
| 27 | COUNT/MIN/MAX per partition | Multi-aggregation |

---

## AVRO Mode Tests (`test-avro.sh`)

20 AVRO-specific tests covering Schema Registry integration, decoded field access, and plan validation. Requires a Schema Registry instance (included in `docker-compose.yml`).

| Tests | Category |
|-------|----------|
| A1–A2 | Basic scan + metadata columns in AVRO mode |
| A3–A7 | Decoded field access: order_id, customer, amount, status (enum), ts (timestamp) |
| A8–A11 | Filter, aggregate, ORDER BY, GROUP BY on Avro fields |
| A12–A13 | Mix metadata + Avro fields; partition filter in AVRO mode |
| A14–A15 | EXPLAIN PLAN — KafkaScan + filter in plan in AVRO mode |
| A16–A17 | CTE + subquery over Avro topic |
| A18 | CTAS Avro → Iceberg |
| A19 | `_value_raw` accessible alongside decoded Avro fields |
| A20 | Null handling for nullable union fields |

---

## Unit Tests — 35 Tests Across 3 Files ✅

| File | Tests | Coverage |
|------|-------|---------|
| `KafkaScanSpecTest.java` | ~21 | Filter pushdown, offset range, serialization, backward compatibility |
| `KafkaSchemaRegistryClientTest.java` | ~10 | Auth, caching, error handling, URL normalization |
| `KafkaAvroConverterTest.java` | ~14 | Primitive/complex Avro → Arrow type mapping, nullable unions |

All unit tests pass: `mvn test -q`

---

## Known Limitations

| Limitation | Notes |
|------------|-------|
| **No write support** | Kafka as a sink is not implemented. Dremio treats Kafka as read-only. |
| **Bounded reads only** | Offsets are frozen at plan time. Messages arriving during query execution are excluded by design (deterministic SQL semantics). |
| **No streaming mode** | Each query is a full bounded scan. For streaming, use Flink or Kafka Streams. |
| **AVRO requires Schema Registry** | The Confluent Schema Registry protocol is required for AVRO mode. No schema-file fallback. |
| **INSERT INTO not supported** | Dremio 26.x INSERT INTO requires Iceberg-backed tables. |
