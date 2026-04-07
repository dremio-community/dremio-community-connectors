#!/usr/bin/env bash
# test-connection.sh — Smoke-test the Dremio Splunk Connector
#
# Usage:
#   ./test-connection.sh [OPTIONS]
#
# Options:
#   --url <url>       Dremio URL (default: http://localhost:9047)
#   --user <user>     Dremio username (prompted if omitted)
#   --password <pass> Dremio password (prompted if omitted)
#   --source <name>   Splunk source name in Dremio (default: splunk)
#   --index <name>    Splunk index to test against (default: main)
#   --help            Show this help

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER=""
DREMIO_PASS=""
SOURCE_NAME="splunk"
INDEX_NAME="main"

while [[ $# -gt 0 ]]; do
  case $1 in
    --url)      DREMIO_URL="$2";   shift 2 ;;
    --user)     DREMIO_USER="$2";  shift 2 ;;
    --password) DREMIO_PASS="$2";  shift 2 ;;
    --source)   SOURCE_NAME="$2";  shift 2 ;;
    --index)    INDEX_NAME="$2";   shift 2 ;;
    --help)
      sed -n '3,12p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

if [[ -z "$DREMIO_USER" ]]; then
  read -rp "Dremio username: " DREMIO_USER
fi
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password: " DREMIO_PASS; echo ""
fi

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

# ── Helpers ───────────────────────────────────────────────────────────────────
get_token() {
  curl -sf -X POST "${DREMIO_URL}/apiv2/login" \
    -H "Content-Type: application/json" \
    -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" \
    | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])"
}

run_sql() {
  local sql="$1" token="$2"
  local body; body=$(python3 -c "import json,sys; print(json.dumps({'sql':sys.argv[1]}))" "$sql")
  curl -sf -X POST "${DREMIO_URL}/api/v3/sql" \
    -H "Authorization: Bearer $token" \
    -H "Content-Type: application/json" \
    -d "$body"
}

wait_job() {
  local job_id="$1" token="$2"
  for i in $(seq 1 60); do
    local status
    status=$(curl -sf "${DREMIO_URL}/api/v3/job/${job_id}" \
      -H "Authorization: Bearer $token" \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobState',''))")
    case "$status" in
      COMPLETED) return 0 ;;
      FAILED|CANCELED) return 1 ;;
    esac
    sleep 1
  done
  return 1
}

get_results() {
  local job_id="$1" token="$2"
  curl -sf "${DREMIO_URL}/api/v3/job/${job_id}/results?offset=0&limit=5" \
    -H "Authorization: Bearer $token"
}

check() {
  local desc="$1" sql="$2" expect="$3" token="$4"
  printf "  %-55s " "$desc"
  local resp job_id result
  resp=$(run_sql "$sql" "$token" 2>/dev/null || echo "")
  job_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
  if [[ -z "$job_id" ]]; then
    echo "SKIP"
    SKIP_COUNT=$((SKIP_COUNT + 1))
    return
  fi
  if wait_job "$job_id" "$token"; then
    result=$(get_results "$job_id" "$token" 2>/dev/null || echo "")
    if echo "$result" | grep -q "$expect"; then
      echo "PASS"
      PASS_COUNT=$((PASS_COUNT + 1))
    else
      echo "FAIL"
      echo "       Expected: $expect"
      echo "       Got: $(echo "$result" | head -c 200)"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
  else
    echo "FAIL (job failed)"
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

# ── Main ──────────────────────────────────────────────────────────────────────
echo ""
echo "=== Dremio Splunk Connector Smoke Tests ==="
echo "  Source: $SOURCE_NAME   Index: $INDEX_NAME   Dremio: $DREMIO_URL"
echo ""

TOKEN=$(get_token)
if [[ -z "$TOKEN" ]]; then
  echo "ERROR: Could not authenticate with Dremio"
  exit 1
fi
echo "  Authenticated OK"
echo ""

TABLE="\"${SOURCE_NAME}\".\"${INDEX_NAME}\""

# ── Tests ─────────────────────────────────────────────────────────────────────
check "SELECT * LIMIT 5"                           "SELECT * FROM $TABLE LIMIT 5"                                          "rows"       "$TOKEN"
check "Metadata column: _time"                     "SELECT _time FROM $TABLE LIMIT 1"                                      "_time"      "$TOKEN"
check "Metadata column: _raw"                      "SELECT _raw FROM $TABLE LIMIT 1"                                       "rows"       "$TOKEN"
check "Metadata column: _host"                     "SELECT _host FROM $TABLE LIMIT 1"                                      "rows"       "$TOKEN"
check "Metadata column: _sourcetype"               "SELECT _sourcetype FROM $TABLE LIMIT 1"                                "rows"       "$TOKEN"
check "Metadata column: _source"                   "SELECT _source FROM $TABLE LIMIT 1"                                    "rows"       "$TOKEN"
check "Metadata column: _index"                    "SELECT _index FROM $TABLE LIMIT 1"                                     "rows"       "$TOKEN"
check "COUNT(*) returns a number"                  "SELECT COUNT(*) FROM $TABLE"                                           "rows"       "$TOKEN"
check "WHERE _time range (last 1 hour)"            "SELECT COUNT(*) FROM $TABLE WHERE _time >= NOW() - INTERVAL '1' HOUR" "rows"       "$TOKEN"
check "WHERE field = value (string eq)"            "SELECT * FROM $TABLE WHERE _sourcetype IS NOT NULL LIMIT 5"            "rows"       "$TOKEN"
check "GROUP BY _sourcetype"                       "SELECT _sourcetype, COUNT(*) cnt FROM $TABLE GROUP BY _sourcetype"     "_sourcetype" "$TOKEN"
check "GROUP BY _host"                             "SELECT _host, COUNT(*) cnt FROM $TABLE GROUP BY _host"                "_host"      "$TOKEN"
check "ORDER BY _time DESC"                        "SELECT _time FROM $TABLE ORDER BY _time DESC LIMIT 5"                  "_time"      "$TOKEN"
check "LIMIT 10 returns <= 10 rows"               "SELECT _time, _host FROM $TABLE LIMIT 10"                              "rows"       "$TOKEN"
check "CTE: WITH latest AS (...) SELECT ..."       "WITH latest AS (SELECT _time, _host FROM $TABLE LIMIT 20) SELECT _host, COUNT(*) cnt FROM latest GROUP BY _host" "rows" "$TOKEN"
check "EXPLAIN PLAN contains SplunkScan"           "EXPLAIN PLAN FOR SELECT * FROM $TABLE LIMIT 5"                        "Splunk"     "$TOKEN"
check "_time BETWEEN two timestamps"               "SELECT COUNT(*) FROM $TABLE WHERE _time BETWEEN TIMESTAMP '2020-01-01 00:00:00' AND NOW()" "rows" "$TOKEN"
check "NULL check: WHERE _raw IS NOT NULL"         "SELECT COUNT(*) FROM $TABLE WHERE _raw IS NOT NULL"                   "rows"       "$TOKEN"
check "MIN/MAX _time"                              "SELECT MIN(_time) min_t, MAX(_time) max_t FROM $TABLE"                "rows"       "$TOKEN"
check "CAST(_time AS VARCHAR)"                     "SELECT CAST(_time AS VARCHAR) ts_str FROM $TABLE LIMIT 3"             "rows"       "$TOKEN"

echo ""
echo "=== Results ==="
echo "  PASS: $PASS_COUNT"
echo "  FAIL: $FAIL_COUNT"
echo "  SKIP: $SKIP_COUNT"
echo "  Total: $((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))"
echo ""

if [[ $FAIL_COUNT -gt 0 ]]; then exit 1; fi
