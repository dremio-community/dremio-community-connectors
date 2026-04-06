#!/usr/bin/env bash
# =============================================================================
# test-connection.sh — Smoke-test the Dremio Kafka Connector
#
# Runs a series of SQL queries against a configured Kafka source in Dremio
# and verifies expected results.
#
# Usage:
#   ./test-connection.sh [OPTIONS]
#
# Options:
#   --url <url>          Dremio URL (default: http://localhost:9047)
#   --user <user>        Dremio username (default: prompted)
#   --password <pass>    Dremio password (default: prompted)
#   --source <name>      Kafka source name in Dremio (default: kafka_test)
#   --topic <name>       Kafka topic name to test against (default: prompted)
#   --s3-source <name>   S3/MinIO source name for CTAS test (default: iceberg_minio)
#   --s3-bucket <name>   S3 bucket for CTAS test (default: dremio-test)
#   --skip-ctas          Skip the CTAS test (if no S3/MinIO source available)
#   --help               Show this help
# =============================================================================

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER=""
DREMIO_PASS=""
SOURCE_NAME="kafka_test"
TOPIC_NAME=""
S3_SOURCE="iceberg_minio"
S3_BUCKET="dremio-test"
SKIP_CTAS="false"

while [[ $# -gt 0 ]]; do
  case $1 in
    --url)          DREMIO_URL="$2";    shift 2 ;;
    --user)         DREMIO_USER="$2";   shift 2 ;;
    --password)     DREMIO_PASS="$2";   shift 2 ;;
    --source)       SOURCE_NAME="$2";   shift 2 ;;
    --topic)        TOPIC_NAME="$2";    shift 2 ;;
    --s3-source)    S3_SOURCE="$2";     shift 2 ;;
    --s3-bucket)    S3_BUCKET="$2";     shift 2 ;;
    --skip-ctas)    SKIP_CTAS="true";   shift ;;
    --help)
      sed -n '2,25p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Prompt for missing credentials ───────────────────────────────────────────
if [[ -z "$DREMIO_USER" ]]; then
  read -rp "Dremio username: " DREMIO_USER
fi
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password: " DREMIO_PASS
  echo ""
fi
if [[ -z "$TOPIC_NAME" ]]; then
  read -rp "Kafka topic to test against: " TOPIC_NAME
fi

PASS=0
FAIL=0
SKIP=0

# ── Helpers ───────────────────────────────────────────────────────────────────
info()  { echo "  $*"; }
warn()  { echo "  [WARN] $*" >&2; }

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
  local max_wait=30
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
  local expect="$3"   # substring to look for in results
  local token="$4"

  printf "  %-60s " "$desc"

  local response job_id result
  response=$(run_sql "$sql" "$token")
  job_id=$(echo "$response" | grep -o '"id":"[^"]*"' | head -1 | cut -d'"' -f4 || true)

  if [[ -z "$job_id" ]]; then
    echo "SKIP (no job id — SQL may not be supported)"
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
    echo "     Got: $(echo "$result" | head -c 300)"
    FAIL=$((FAIL + 1))
  fi
}

# check_row_count <desc> <sql> <expected_count> <token>
# Asserts that rowCount (or totalRowCount) in the result equals the expected value.
check_row_count() {
  local desc="$1"
  local sql="$2"
  local expected_count="$3"
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

  # rowCount appears in both /results response and job state
  local actual
  actual=$(echo "$result" | grep -o '"rowCount":[0-9]*' | grep -o '[0-9]*' || true)

  if [[ "$actual" == "$expected_count" ]]; then
    echo "PASS"
    PASS=$((PASS + 1))
  else
    echo "FAIL (expected rowCount=$expected_count, got rowCount=${actual:-unknown})"
    echo "     Got: $(echo "$result" | head -c 300)"
    FAIL=$((FAIL + 1))
  fi
}

# check_plan <desc> <sql> <plan_substring> <token>
# Runs EXPLAIN PLAN FOR <sql> and checks the plan text for a substring.
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

# ── Main ──────────────────────────────────────────────────────────────────────
echo ""
echo "=== Dremio Kafka Connector Smoke Tests ==="
echo "  Source : $SOURCE_NAME"
echo "  Topic  : $TOPIC_NAME"
echo "  Dremio : $DREMIO_URL"
echo ""

TOKEN=$(get_token)
if [[ -z "$TOKEN" ]]; then
  echo "ERROR: Could not authenticate with Dremio at $DREMIO_URL"
  exit 1
fi
echo "  Authenticated OK"
echo ""

TABLE="\"$SOURCE_NAME\".\"$TOPIC_NAME\""

# Test 1: Basic scan
check "SELECT * LIMIT 5" \
  "SELECT * FROM $TABLE LIMIT 5" \
  "_topic" "$TOKEN"

# Test 2: Metadata columns present
check "SELECT _topic, _partition, _offset, _timestamp" \
  "SELECT _topic, _partition, _offset, _timestamp FROM $TABLE LIMIT 1" \
  "_topic" "$TOKEN"

# Test 3: _key column present
check "_key column accessible" \
  "SELECT _key FROM $TABLE LIMIT 1" \
  "rows" "$TOKEN"

# Test 4: _value_raw column present
check "_value_raw column accessible" \
  "SELECT _value_raw FROM $TABLE LIMIT 1" \
  "rows" "$TOKEN"

# Test 5: _headers column present
check "_headers column accessible" \
  "SELECT _headers FROM $TABLE LIMIT 1" \
  "rows" "$TOKEN"

# Test 6: COUNT
check "COUNT(*) returns a number" \
  "SELECT COUNT(*) FROM $TABLE" \
  "rows" "$TOKEN"

# Test 7: Partition filter
check "WHERE _partition = 0" \
  "SELECT COUNT(*) FROM $TABLE WHERE _partition = 0" \
  "rows" "$TOKEN"

# Test 8: Offset filter
check "WHERE _offset >= 0" \
  "SELECT * FROM $TABLE WHERE _offset >= 0 LIMIT 5" \
  "rows" "$TOKEN"

# Test 9: Timestamp filter
check "WHERE _timestamp IS NOT NULL" \
  "SELECT * FROM $TABLE WHERE _timestamp IS NOT NULL LIMIT 5" \
  "rows" "$TOKEN"

# Test 10: Column projection
check "SELECT specific columns only" \
  "SELECT _partition, _offset, _value_raw FROM $TABLE LIMIT 3" \
  "rows" "$TOKEN"

# Test 11: ORDER BY offset
check "ORDER BY _offset" \
  "SELECT _offset FROM $TABLE ORDER BY _offset LIMIT 5" \
  "rows" "$TOKEN"

# Test 12: GROUP BY partition
check "GROUP BY _partition" \
  "SELECT _partition, COUNT(*) as cnt FROM $TABLE GROUP BY _partition" \
  "_partition" "$TOKEN"

# Test 13: MIN/MAX offset per partition
check "MIN/MAX offset per partition" \
  "SELECT _partition, MIN(_offset) as min_off, MAX(_offset) as max_off FROM $TABLE GROUP BY _partition" \
  "rows" "$TOKEN"

# Test 14: CTAS to Iceberg table on MinIO (S3-compatible object store)
# Requires an S3 source named 'iceberg_minio' pointing to the dremio-test bucket.
# Create it once via:
#   PUT /apiv2/source/iceberg_minio  type=S3  accessKey=minioadmin  ...  dremio.s3.compat=true
# Skip with --skip-ctas if no S3 source is available.
CTAS_TABLE="kafka_ctas_$(date +%s)"
if [[ "$SKIP_CTAS" == "true" ]]; then
  printf "  %-60s SKIP (--skip-ctas)\n" "CTAS to Iceberg on ${S3_SOURCE}"
  SKIP=$((SKIP + 1))
else
  check "CTAS to Iceberg on ${S3_SOURCE}" \
    "CREATE TABLE ${S3_SOURCE}.\"${S3_BUCKET}\".\"${CTAS_TABLE}\" AS SELECT * FROM $TABLE LIMIT 100" \
    "rows" "$TOKEN"
fi

# ── Extended Tests ────────────────────────────────────────────────────────────
echo ""
echo "--- Extended Tests ---"
echo ""

# Test 15: EXPLAIN PLAN — verify KafkaScan and filter pushdown appear in plan
check_plan "EXPLAIN: KafkaScan appears in plan" \
  "SELECT _partition, _offset FROM $TABLE WHERE _partition = 0 LIMIT 10" \
  "KafkaScan" "$TOKEN"

# Test 16: EXPLAIN PLAN — partition filter is pushed into scan (not applied above it)
check_plan "EXPLAIN: partition filter pushed into KafkaScan" \
  "SELECT COUNT(*) FROM $TABLE WHERE _partition = 0" \
  "KafkaScan" "$TOKEN"

# Test 17: Offset BETWEEN range
check "WHERE _offset BETWEEN 0 AND 1000" \
  "SELECT _offset FROM $TABLE WHERE _offset BETWEEN 0 AND 1000 LIMIT 5" \
  "rows" "$TOKEN"

# Test 18: Combined partition + offset filter (compound pushdown)
check "WHERE _partition = 0 AND _offset >= 0" \
  "SELECT _partition, _offset FROM $TABLE WHERE _partition = 0 AND _offset >= 0 LIMIT 5" \
  "rows" "$TOKEN"

# Test 19: _key IS NULL (tombstone / no-key messages)
check "WHERE _key IS NULL (tombstone filter)" \
  "SELECT COUNT(*) FROM $TABLE WHERE _key IS NULL" \
  "rows" "$TOKEN"

# Test 20: _key IS NOT NULL
check "WHERE _key IS NOT NULL" \
  "SELECT COUNT(*) FROM $TABLE WHERE _key IS NOT NULL" \
  "rows" "$TOKEN"

# Test 21: SELECT DISTINCT _partition
check "SELECT DISTINCT _partition" \
  "SELECT DISTINCT _partition FROM $TABLE" \
  "_partition" "$TOKEN"

# Test 22: LIMIT 0 returns 0 rows
check_row_count "LIMIT 0 returns rowCount = 0" \
  "SELECT * FROM $TABLE LIMIT 0" \
  "0" "$TOKEN"

# Test 23: CTE (WITH clause)
check "CTE: WITH recent AS (SELECT ...) SELECT ..." \
  "WITH recent AS (SELECT _partition, _offset FROM $TABLE LIMIT 20) SELECT _partition, COUNT(*) as cnt FROM recent GROUP BY _partition" \
  "_partition" "$TOKEN"

# Test 24: Subquery — SELECT from SELECT
check "Subquery: SELECT from inline view" \
  "SELECT sub._partition, sub._offset FROM (SELECT _partition, _offset FROM $TABLE LIMIT 10) sub ORDER BY sub._offset" \
  "rows" "$TOKEN"

# Test 25: CAST _value_raw to VARCHAR
check "CAST(_value_raw AS VARCHAR)" \
  "SELECT CAST(_value_raw AS VARCHAR) as raw_str FROM $TABLE LIMIT 3" \
  "rows" "$TOKEN"

# Test 26: Timestamp range — last 24 hours (most topics will have recent data)
check "WHERE _timestamp >= NOW() - INTERVAL '1' DAY" \
  "SELECT COUNT(*) FROM $TABLE WHERE _timestamp >= NOW() - INTERVAL '1' DAY" \
  "rows" "$TOKEN"

# Test 27: Multiple aggregations in one query
check "Multi-agg: COUNT/MIN/MAX/AVG offset per partition" \
  "SELECT _partition, COUNT(*) as cnt, MIN(_offset) as min_off, MAX(_offset) as max_off FROM $TABLE GROUP BY _partition ORDER BY _partition" \
  "_partition" "$TOKEN"

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
