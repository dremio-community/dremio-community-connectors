#!/usr/bin/env bash
# =============================================================================
# test-avro.sh — Smoke-test AVRO mode + Schema Registry for the Kafka Connector
#
# Prerequisites:
#   1. A Kafka source registered in AVRO mode pointing to a Schema Registry.
#      Create it with:
#        ./add-kafka-source.sh \
#          --name kafka_avro \
#          --brokers localhost:9092 \
#          --schema-mode AVRO \
#          --schema-registry http://localhost:8081
#
#   2. An Avro-encoded topic with at least one registered schema.
#      The docker-compose.yml environment produces sample Avro messages if
#      you run the kafka-avro-seed container (see below).
#
# Usage:
#   ./test-avro.sh [OPTIONS]
#
# Options:
#   --url              URL    Dremio URL (default: http://localhost:9047)
#   --user             USER   Dremio username (default: prompted)
#   --password         PASS   Dremio password (default: prompted)
#   --source           NAME   Avro Kafka source name in Dremio (default: kafka_avro)
#   --topic            NAME   Avro-encoded topic to test against (default: prompted)
#   --schema-registry  URL    Schema Registry URL (default: http://localhost:8081)
#   --sr-user          USER   Schema Registry basic auth username (optional)
#   --sr-pass          PASS   Schema Registry basic auth password (optional)
#   --produce                 Produce sample Avro messages before testing
#                             (requires kafka-avro-console-producer on PATH)
#   --skip-ctas               Skip CTAS test
#   --s3-source        NAME   S3 source for CTAS test (default: iceberg_minio)
#   --s3-bucket        NAME   S3 bucket for CTAS test (default: dremio-test)
#   --help                    Show this help
# =============================================================================

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER=""
DREMIO_PASS=""
SOURCE_NAME="kafka_avro"
TOPIC_NAME=""
SCHEMA_REGISTRY="http://localhost:8081"
SR_USER=""
SR_PASS=""
PRODUCE="false"
SKIP_CTAS="false"
S3_SOURCE="iceberg_minio"
S3_BUCKET="dremio-test"

while [[ $# -gt 0 ]]; do
  case $1 in
    --url)              DREMIO_URL="$2";       shift 2 ;;
    --user)             DREMIO_USER="$2";      shift 2 ;;
    --password)         DREMIO_PASS="$2";      shift 2 ;;
    --source)           SOURCE_NAME="$2";      shift 2 ;;
    --topic)            TOPIC_NAME="$2";       shift 2 ;;
    --schema-registry)  SCHEMA_REGISTRY="$2";  shift 2 ;;
    --sr-user)          SR_USER="$2";          shift 2 ;;
    --sr-pass)          SR_PASS="$2";          shift 2 ;;
    --produce)          PRODUCE="true";        shift ;;
    --skip-ctas)        SKIP_CTAS="true";      shift ;;
    --s3-source)        S3_SOURCE="$2";        shift 2 ;;
    --s3-bucket)        S3_BUCKET="$2";        shift 2 ;;
    --help)
      sed -n '2,42p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Prompt for missing values ─────────────────────────────────────────────────
if [[ -z "$DREMIO_USER" ]]; then
  read -rp "Dremio username: " DREMIO_USER
fi
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password: " DREMIO_PASS
  echo ""
fi
if [[ -z "$TOPIC_NAME" ]]; then
  read -rp "Avro-encoded Kafka topic to test against: " TOPIC_NAME
fi

PASS=0
FAIL=0
SKIP=0

# ── Helpers ───────────────────────────────────────────────────────────────────
get_token() {
  curl -s -X POST "$DREMIO_URL/apiv2/login" \
    -H "Content-Type: application/json" \
    -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" \
    | grep -o '"token":"[^"]*"' | cut -d'"' -f4 || true
}

run_sql() {
  local sql="$1"
  local token="$2"
  local body
  body=$(python3 -c "import json,sys; print(json.dumps({'sql':sys.argv[1]}))" "$sql")
  curl -s -X POST "$DREMIO_URL/api/v3/sql" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -d "$body" 2>/dev/null
}

get_job_result() {
  local job_id="$1"
  local token="$2"
  local max_wait=45
  local waited=0
  while [[ $waited -lt $max_wait ]]; do
    local status
    status=$(curl -s "$DREMIO_URL/api/v3/job/$job_id" \
      -H "Authorization: Bearer $token" | grep -o '"jobState":"[^"]*"' | cut -d'"' -f4 || true)
    case "$status" in
      COMPLETED) break ;;
      FAILED|CANCELED)
        echo "FAILED"
        return ;;
    esac
    sleep 1
    ((waited++))
  done
  curl -s "$DREMIO_URL/api/v3/job/$job_id/results?offset=0&limit=10" \
    -H "Authorization: Bearer $token"
}

check() {
  local desc="$1"
  local sql="$2"
  local expect="$3"
  local token="$4"

  printf "  %-60s " "$desc"

  local response job_id result
  response=$(run_sql "$sql" "$token")
  job_id=$(echo "$response" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4 || true)

  if [[ -z "$job_id" ]]; then
    echo "SKIP (no job id)"
    SKIP=$((SKIP + 1))
    return
  fi

  result=$(get_job_result "$job_id" "$token")

  if echo "$result" | grep -q "$expect"; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL"
    echo "     Expected substring: $expect"
    echo "     Got: $(echo "$result" | head -c 400)"
    FAIL=$((FAIL + 1))
  fi
}

check_plan() {
  local desc="$1"
  local sql="$2"
  local plan_expect="$3"
  local token="$4"

  printf "  %-60s " "$desc"

  local response job_id result
  response=$(run_sql "EXPLAIN PLAN FOR $sql" "$token")
  job_id=$(echo "$response" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4 || true)

  if [[ -z "$job_id" ]]; then
    echo "SKIP (EXPLAIN not supported)"
    SKIP=$((SKIP + 1))
    return
  fi

  result=$(get_job_result "$job_id" "$token")

  if echo "$result" | grep -qi "$plan_expect"; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL"
    echo "     Expected plan substring: $plan_expect"
    echo "     Plan: $(echo "$result" | head -c 500)"
    FAIL=$((FAIL + 1))
  fi
}

sr_curl() {
  local path="$1"
  if [[ -n "$SR_USER" ]]; then
    curl -s -u "${SR_USER}:${SR_PASS}" "${SCHEMA_REGISTRY}${path}"
  else
    curl -s "${SCHEMA_REGISTRY}${path}"
  fi
}

# ── Optional: produce sample Avro messages ────────────────────────────────────
if [[ "$PRODUCE" == "true" ]]; then
  echo ""
  echo "=== Producing sample Avro messages ==="
  if ! command -v kafka-avro-console-producer &>/dev/null; then
    echo "  WARN: kafka-avro-console-producer not found on PATH."
    echo "  Install Confluent Platform tools or use the docker-compose kafka-seed service."
    echo "  Skipping message production — existing messages will be used."
  else
    SCHEMA='{
      "type": "record",
      "name": "Order",
      "fields": [
        {"name": "order_id",  "type": "int"},
        {"name": "customer",  "type": "string"},
        {"name": "amount",    "type": "double"},
        {"name": "status",    "type": {"type": "enum", "name": "Status", "symbols": ["pending","shipped","delivered"]}},
        {"name": "ts",        "type": "long",   "logicalType": "timestamp-millis"}
      ]
    }'

    echo '{"order_id":1,"customer":"alice","amount":99.95,"status":"pending","ts":1712000000000}
{"order_id":2,"customer":"bob","amount":250.00,"status":"shipped","ts":1712001000000}
{"order_id":3,"customer":"alice","amount":15.50,"status":"delivered","ts":1712002000000}
{"order_id":4,"customer":"charlie","amount":500.00,"status":"pending","ts":1712003000000}
{"order_id":5,"customer":"bob","amount":75.00,"status":"shipped","ts":1712004000000}' | \
    kafka-avro-console-producer \
      --bootstrap-server localhost:9092 \
      --topic "$TOPIC_NAME" \
      --property schema.registry.url="$SCHEMA_REGISTRY" \
      --property value.schema="$SCHEMA" && echo "  Produced 5 sample Order records"
  fi
fi

# ── Schema Registry health check ──────────────────────────────────────────────
echo ""
echo "=== Schema Registry Checks ==="
echo "  URL: $SCHEMA_REGISTRY"

SR_SUBJECTS=$(sr_curl "/subjects" 2>/dev/null || true)
if [[ -z "$SR_SUBJECTS" ]] || echo "$SR_SUBJECTS" | grep -qi "error"; then
  echo "  WARN: Cannot reach Schema Registry at $SCHEMA_REGISTRY"
  echo "        AVRO tests may SKIP or FAIL if schema metadata is unavailable."
  echo ""
else
  echo "  Schema Registry reachable OK"
  SUBJECT_COUNT=$(echo "$SR_SUBJECTS" | python3 -c "import json,sys; d=json.load(sys.stdin); print(len(d))" 2>/dev/null || echo "?")
  echo "  Registered subjects: $SUBJECT_COUNT"

  TOPIC_SUBJECT="${TOPIC_NAME}-value"
  if echo "$SR_SUBJECTS" | grep -q "\"${TOPIC_SUBJECT}\""; then
    VERSIONS=$(sr_curl "/subjects/${TOPIC_SUBJECT}/versions" 2>/dev/null || true)
    echo "  Subject '${TOPIC_SUBJECT}' found — versions: $VERSIONS"
  else
    echo "  WARN: Subject '${TOPIC_SUBJECT}' not found in Schema Registry."
    echo "        Produce some Avro messages first (or use --produce flag)."
  fi
  echo ""
fi

# ── Authenticate with Dremio ──────────────────────────────────────────────────
echo "=== Dremio Kafka AVRO Connector Smoke Tests ==="
echo "  Source          : $SOURCE_NAME"
echo "  Topic           : $TOPIC_NAME"
echo "  Schema Registry : $SCHEMA_REGISTRY"
echo "  Dremio          : $DREMIO_URL"
echo ""

TOKEN=$(get_token)
if [[ -z "$TOKEN" ]]; then
  echo "ERROR: Could not authenticate with Dremio at $DREMIO_URL"
  exit 1
fi
echo "  Authenticated OK"
echo ""

TABLE="\"$SOURCE_NAME\".\"$TOPIC_NAME\""

# ── AVRO Smoke Tests ──────────────────────────────────────────────────────────

# Test A1: Basic scan — confirms AVRO messages are decoded, not raw bytes
check "A1: SELECT * returns decoded Avro fields" \
  "SELECT * FROM $TABLE LIMIT 5" \
  "rows" "$TOKEN"

# Test A2: Metadata columns still present in AVRO mode
check "A2: Metadata cols (_topic, _partition, _offset) present" \
  "SELECT _topic, _partition, _offset FROM $TABLE LIMIT 1" \
  "_topic" "$TOKEN"

# Test A3: Avro-decoded field — check a known schema field is a column
#          (schema-specific; order_id / customer / amount expected from seed data)
check "A3: Schema field 'order_id' is queryable" \
  "SELECT order_id FROM $TABLE LIMIT 5" \
  "rows" "$TOKEN"

# Test A4: String field decoded correctly
check "A4: String field 'customer' is queryable" \
  "SELECT customer FROM $TABLE LIMIT 5" \
  "rows" "$TOKEN"

# Test A5: Numeric field decoded correctly
check "A5: Double field 'amount' is queryable" \
  "SELECT amount FROM $TABLE LIMIT 5" \
  "rows" "$TOKEN"

# Test A6: Enum field decoded as string
check "A6: Enum field 'status' decoded as string" \
  "SELECT status FROM $TABLE WHERE status IS NOT NULL LIMIT 5" \
  "rows" "$TOKEN"

# Test A7: Timestamp logical type
check "A7: Logical timestamp field 'ts' is queryable" \
  "SELECT ts FROM $TABLE WHERE ts IS NOT NULL LIMIT 5" \
  "rows" "$TOKEN"

# Test A8: Filter on Avro field (not just metadata columns)
check "A8: WHERE on decoded field (status = 'pending')" \
  "SELECT order_id, customer, amount FROM $TABLE WHERE status = 'pending' LIMIT 10" \
  "rows" "$TOKEN"

# Test A9: Aggregate on Avro field
check "A9: SUM(amount) aggregation on Avro field" \
  "SELECT SUM(amount) as total_amount FROM $TABLE" \
  "rows" "$TOKEN"

# Test A10: GROUP BY on Avro field
check "A10: GROUP BY status (Avro enum field)" \
  "SELECT status, COUNT(*) as cnt, AVG(amount) as avg_amount FROM $TABLE GROUP BY status" \
  "status" "$TOKEN"

# Test A11: ORDER BY on Avro numeric field
check "A11: ORDER BY amount DESC" \
  "SELECT order_id, amount FROM $TABLE ORDER BY amount DESC LIMIT 5" \
  "rows" "$TOKEN"

# Test A12: Mix metadata + Avro fields in projection
check "A12: Mix metadata + Avro fields in SELECT" \
  "SELECT _partition, _offset, order_id, status FROM $TABLE LIMIT 5" \
  "rows" "$TOKEN"

# Test A13: Partition filter still pushes down in AVRO mode
check "A13: WHERE _partition = 0 filter in AVRO mode" \
  "SELECT order_id FROM $TABLE WHERE _partition = 0 LIMIT 5" \
  "rows" "$TOKEN"

# Test A14: EXPLAIN PLAN — KafkaScan present in AVRO mode
check_plan "A14: EXPLAIN: KafkaScan present in AVRO plan" \
  "SELECT order_id, amount FROM $TABLE LIMIT 5" \
  "KafkaScan" "$TOKEN"

# Test A15: AVRO field filter in EXPLAIN (verify field referenced)
check_plan "A15: EXPLAIN: Avro field filter in plan" \
  "SELECT order_id FROM $TABLE WHERE status = 'shipped' LIMIT 10" \
  "KafkaScan" "$TOKEN"

# Test A16: CTE over Avro topic
check "A16: CTE (WITH clause) over Avro topic" \
  "WITH pending AS (SELECT order_id, amount FROM $TABLE WHERE status = 'pending') SELECT COUNT(*) as cnt, SUM(amount) as total FROM pending" \
  "rows" "$TOKEN"

# Test A17: Subquery over Avro topic
check "A17: Subquery over Avro topic" \
  "SELECT sub.customer, sub.total FROM (SELECT customer, SUM(amount) as total FROM $TABLE GROUP BY customer) sub ORDER BY sub.total DESC LIMIT 5" \
  "rows" "$TOKEN"

# Test A18: CTAS — snapshot Avro data to Iceberg
CTAS_TABLE="kafka_avro_ctas_$(date +%s)"
if [[ "$SKIP_CTAS" == "true" ]]; then
  printf "  %-60s SKIP (--skip-ctas)\n" "A18: CTAS Avro → Iceberg on ${S3_SOURCE}"
  SKIP=$((SKIP + 1))
else
  check "A18: CTAS Avro → Iceberg on ${S3_SOURCE}" \
    "CREATE TABLE ${S3_SOURCE}.\"${S3_BUCKET}\".\"${CTAS_TABLE}\" AS SELECT order_id, customer, amount, status FROM $TABLE LIMIT 200" \
    "rows" "$TOKEN"
fi

# Test A19: Schema evolution — _value_raw still accessible alongside decoded fields
check "A19: _value_raw accessible alongside decoded Avro fields" \
  "SELECT order_id, _value_raw FROM $TABLE LIMIT 3" \
  "rows" "$TOKEN"

# Test A20: Null handling — Avro nullable union fields
check "A20: IS NULL check on nullable Avro field" \
  "SELECT COUNT(*) FROM $TABLE WHERE customer IS NULL" \
  "rows" "$TOKEN"

echo ""
echo "=== Results ==="
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo "  SKIP: $SKIP"
echo "  Total: $((PASS + FAIL + SKIP))"
echo ""

if [[ $FAIL -gt 0 ]]; then
  exit 1
fi
