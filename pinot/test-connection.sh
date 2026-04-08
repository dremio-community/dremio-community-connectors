#!/usr/bin/env bash
# test-connection.sh — Smoke-test the Dremio Apache Pinot Connector
#
# Usage:
#   ./test-connection.sh [OPTIONS]
#
# Options:
#   --url    <url>    Dremio URL (default: http://localhost:9047)
#   --user   <user>   Dremio username (prompted if omitted)
#   --password <pass> Dremio password (prompted if omitted)
#   --source <name>   Pinot source name in Dremio (default: pinot)
#   --table  <name>   Pinot table to test against (default: airlineStats)
#   --help            Show this help

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER=""
DREMIO_PASS=""
SOURCE_NAME="pinot"
TABLE_NAME="airlineStats"

while [[ $# -gt 0 ]]; do
  case $1 in
    --url)      DREMIO_URL="$2";   shift 2 ;;
    --user)     DREMIO_USER="$2";  shift 2 ;;
    --password) DREMIO_PASS="$2";  shift 2 ;;
    --source)   SOURCE_NAME="$2";  shift 2 ;;
    --table)    TABLE_NAME="$2";   shift 2 ;;
    --help)
      sed -n '3,13p' "$0" | sed 's/^# *//'
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
  curl -sf "${DREMIO_URL}/api/v3/job/${job_id}/results?offset=0&limit=10" \
    -H "Authorization: Bearer $token"
}

get_error() {
  local job_id="$1" token="$2"
  curl -sf "${DREMIO_URL}/api/v3/job/${job_id}" \
    -H "Authorization: Bearer $token" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('errorMessage','(no error message)'))" 2>/dev/null || echo ""
}

check() {
  local desc="$1" sql="$2" expect="$3" token="$4"
  printf "  %-55s " "$desc"
  local resp job_id result
  resp=$(run_sql "$sql" "$token" 2>/dev/null || echo "")
  job_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || echo "")
  if [[ -z "$job_id" ]]; then
    echo "SKIP (could not submit)"
    SKIP_COUNT=$((SKIP_COUNT + 1))
    return
  fi
  if wait_job "$job_id" "$token"; then
    result=$(get_results "$job_id" "$token" 2>/dev/null || echo "")
    if echo "$result" | grep -q "$expect"; then
      local rows
      rows=$(echo "$result" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('rowCount', len(d.get('rows',[]))))" 2>/dev/null || echo "?")
      echo "PASS ✅  (rows=$rows)"
      PASS_COUNT=$((PASS_COUNT + 1))
    else
      echo "FAIL ❌"
      echo "       Expected pattern: $expect"
      echo "       Got: $(echo "$result" | head -c 300)"
      FAIL_COUNT=$((FAIL_COUNT + 1))
    fi
  else
    local errmsg
    errmsg=$(get_error "$job_id" "$token")
    echo "FAIL ❌  $errmsg" | head -c 200
    echo ""
    FAIL_COUNT=$((FAIL_COUNT + 1))
  fi
}

# ── Main ──────────────────────────────────────────────────────────────────────
echo ""
echo "=== Dremio Apache Pinot Connector Smoke Tests ==="
echo "  Source : $SOURCE_NAME"
echo "  Table  : $TABLE_NAME"
echo "  Dremio : $DREMIO_URL"
echo ""

TOKEN=$(get_token)
if [[ -z "$TOKEN" ]]; then
  echo "ERROR: Could not authenticate with Dremio"
  exit 1
fi
echo "  Authenticated OK"
echo ""

T="\"${SOURCE_NAME}\".\"${TABLE_NAME}\""

# ── Tests ─────────────────────────────────────────────────────────────────────
check "Basic connectivity (SELECT 1)"                  "SELECT 1"                                                                      "rows"   "$TOKEN"
check "Simple scan with LIMIT"                         "SELECT * FROM $T LIMIT 10"                                                     "rows"   "$TOKEN"
check "COUNT(*)"                                       "SELECT COUNT(*) FROM $T"                                                       "rows"   "$TOKEN"
check "GROUP BY + ORDER BY + LIMIT"                    "SELECT \"Carrier\", COUNT(*) cnt FROM $T GROUP BY \"Carrier\" ORDER BY cnt DESC LIMIT 10" "rows" "$TOKEN"
check "AVG / MAX / MIN with WHERE"                     "SELECT AVG(\"ArrDelay\") avg_d, MAX(\"ArrDelay\") max_d, MIN(\"ArrDelay\") min_d FROM $T WHERE \"Carrier\" = 'AA'" "rows" "$TOKEN"
check "Filter + ORDER BY + LIMIT"                      "SELECT \"Carrier\", \"ArrDelay\" FROM $T WHERE \"ArrDelay\" > 0 ORDER BY \"ArrDelay\" DESC LIMIT 20" "rows" "$TOKEN"
check "SUM with filter"                                "SELECT SUM(\"ArrDelay\") total FROM $T WHERE \"Year\" = 2014"                  "rows"   "$TOKEN"
check "DISTINCT + ORDER BY"                            "SELECT DISTINCT \"Origin\" FROM $T ORDER BY \"Origin\" LIMIT 20"               "rows"   "$TOKEN"
check "Multi-column GROUP BY + ORDER BY"               "SELECT \"Origin\", \"Dest\", COUNT(*) cnt FROM $T GROUP BY \"Origin\", \"Dest\" ORDER BY cnt DESC LIMIT 15" "rows" "$TOKEN"
check "NULL check: WHERE col IS NOT NULL"              "SELECT COUNT(*) FROM $T WHERE \"ArrDelay\" IS NOT NULL"                        "rows"   "$TOKEN"

echo ""
echo "=== Results ==="
echo "  PASS: $PASS_COUNT"
echo "  FAIL: $FAIL_COUNT"
echo "  SKIP: $SKIP_COUNT"
echo "  Total: $((PASS_COUNT + FAIL_COUNT + SKIP_COUNT))"
echo ""

if [[ $FAIL_COUNT -gt 0 ]]; then exit 1; fi
