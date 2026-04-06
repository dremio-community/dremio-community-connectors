#!/usr/bin/env bash
# =============================================================================
# test-connection.sh — Dremio ClickHouse Connector smoke test
#
# Verifies that the ClickHouse connector is installed and reachable by running
# a small set of representative queries via the Dremio REST API and checking
# that they complete successfully.
#
# Usage:
#   ./test-connection.sh                                    # interactive
#   ./test-connection.sh --source clickhouse_test \
#                        --table  testdb.orders   \
#                        --url    http://localhost:9047 \
#                        --user   admin --password <your-password>
#
# Exit codes:
#   0 — all checks passed
#   1 — one or more checks failed
# =============================================================================

set -euo pipefail

# ── Colours ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()   { echo -e "  ${GREEN}✓${RESET}  $*"; }
fail() { echo -e "  ${RED}✗${RESET}  $*"; FAILURES=$((FAILURES+1)); }
info() { echo -e "  ${CYAN}→${RESET}  $*"; }

FAILURES=0

# ── Argument defaults ─────────────────────────────────────────────────────────
DREMIO_URL="http://localhost:9047"
DREMIO_USER=""
DREMIO_PASS=""
SOURCE_NAME=""
TABLE_PATH=""     # e.g. testdb.orders

while [[ $# -gt 0 ]]; do
  case "$1" in
    --url)      DREMIO_URL="$2";   shift 2 ;;
    --user)     DREMIO_USER="$2";  shift 2 ;;
    --password) DREMIO_PASS="$2";  shift 2 ;;
    --source)   SOURCE_NAME="$2";  shift 2 ;;
    --table)    TABLE_PATH="$2";   shift 2 ;;
    --help|-h)
      sed -n '2,20p' "$0" | sed 's/^# \?//'
      exit 0 ;;
    *) echo "Unknown flag: $1"; exit 1 ;;
  esac
done

# ── Interactive prompts for missing values ─────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   ClickHouse Connector — Smoke Test${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════${RESET}"
echo ""

[[ -z "$DREMIO_USER" ]]   && read -rp "  Dremio username: " DREMIO_USER
[[ -z "$DREMIO_PASS" ]]   && read -rsp "  Dremio password: " DREMIO_PASS && echo ""
[[ -z "$SOURCE_NAME" ]]   && read -rp "  ClickHouse source name (e.g. clickhouse_test): " SOURCE_NAME
[[ -z "$TABLE_PATH" ]]    && read -rp "  Table path (e.g. testdb.orders): " TABLE_PATH

FULL_TABLE="${SOURCE_NAME}.${TABLE_PATH}"
echo ""

# ── Authenticate ──────────────────────────────────────────────────────────────
info "Authenticating with Dremio at ${DREMIO_URL}..."
LOGIN=$(curl -sf -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" 2>/dev/null) || {
  echo -e "${RED}ERROR: Could not reach Dremio at ${DREMIO_URL}${RESET}"
  exit 1
}
TOKEN=$(echo "$LOGIN" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('token',''))" 2>/dev/null)
if [[ -z "$TOKEN" ]]; then
  echo -e "${RED}ERROR: Authentication failed — check username/password${RESET}"
  exit 1
fi
ok "Authenticated as '${DREMIO_USER}'"

# ── Query helper ──────────────────────────────────────────────────────────────
run_query() {
  local label="$1"
  local sql="$2"
  local expect_rows="${3:-}"   # optional: minimum rows expected
  local result job_id job_status row_count err

  result=$(curl -sf -X POST "${DREMIO_URL}/api/v3/sql" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "{\"sql\": $(python3 -c "import json,sys; print(json.dumps(sys.argv[1]))" "$sql")}" 2>/dev/null)

  job_id=$(echo "$result" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
  if [[ -z "$job_id" ]]; then
    fail "${label} — failed to submit query"
    return
  fi

  for i in $(seq 1 40); do
    sleep 1
    job_status=$(curl -sf -H "Authorization: _dremio${TOKEN}" \
      "${DREMIO_URL}/api/v3/job/${job_id}" 2>/dev/null | \
      python3 -c "import sys,json; print(json.load(sys.stdin).get('jobState','?'))" 2>/dev/null)
    case "$job_status" in
      COMPLETED)
        row_count=$(curl -sf -H "Authorization: _dremio${TOKEN}" \
          "${DREMIO_URL}/api/v3/job/${job_id}/results?offset=0&limit=1" 2>/dev/null | \
          python3 -c "import sys,json; print(json.load(sys.stdin).get('rowCount',0))" 2>/dev/null)
        if [[ -n "$expect_rows" && "$row_count" -lt "$expect_rows" ]]; then
          fail "${label} — completed but got ${row_count} rows (expected ≥${expect_rows})"
        else
          ok "${label} — ${row_count} row(s)"
        fi
        return ;;
      FAILED|CANCELED)
        err=$(curl -sf -H "Authorization: _dremio${TOKEN}" \
          "${DREMIO_URL}/api/v3/job/${job_id}" 2>/dev/null | \
          python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('errorMessage','?')[:200])" 2>/dev/null)
        fail "${label} — ${job_status}: ${err}"
        return ;;
    esac
  done
  fail "${label} — timed out after 40s"
}

# ── Tests ─────────────────────────────────────────────────────────────────────
echo -e "${BOLD}[1] Connectivity${RESET}"
run_query "Basic SELECT" \
  "SELECT * FROM ${FULL_TABLE} LIMIT 5" 1

echo ""
echo -e "${BOLD}[2] Aggregation pushdown${RESET}"
run_query "COUNT(*)" \
  "SELECT COUNT(*) AS n FROM ${FULL_TABLE}" 1
run_query "SUM / AVG / MIN / MAX" \
  "SELECT SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM ${FULL_TABLE}" 1
run_query "MEDIAN" \
  "SELECT MEDIAN(amount) AS med FROM ${FULL_TABLE}" 1
run_query "GROUP BY + ORDER BY + LIMIT" \
  "SELECT region, COUNT(*) AS n, SUM(amount) AS total FROM ${FULL_TABLE} GROUP BY region ORDER BY total DESC LIMIT 5" 1
run_query "APPROX_COUNT_DISTINCT" \
  "SELECT APPROX_COUNT_DISTINCT(user_id) AS approx FROM ${FULL_TABLE}" 1

echo ""
echo -e "${BOLD}[3] Filter pushdown${RESET}"
run_query "Timestamp filter" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE order_date >= TIMESTAMP '2020-01-01 00:00:00'" 1
run_query "String equality" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE status = 'pending'" 0
run_query "IS NULL" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE shipped_date IS NULL" 0
run_query "IN list" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE status IN ('pending','shipped')" 0

echo ""
echo -e "${BOLD}[4] String functions (pushdown)${RESET}"
run_query "UPPER / LOWER" \
  "SELECT UPPER(status), LOWER(status) FROM ${FULL_TABLE} LIMIT 3" 1
run_query "INITCAP" \
  "SELECT INITCAP(status) FROM ${FULL_TABLE} LIMIT 3" 1
run_query "SUBSTRING 2-arg" \
  "SELECT SUBSTRING(status, 1) FROM ${FULL_TABLE} LIMIT 3" 1
run_query "SUBSTRING 3-arg" \
  "SELECT SUBSTRING(status, 1, 3) FROM ${FULL_TABLE} LIMIT 3" 1
run_query "SPLIT_PART" \
  "SELECT SPLIT_PART(status, 'i', 1) AS part FROM ${FULL_TABLE} LIMIT 3" 1
run_query "REGEXP_LIKE" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE REGEXP_LIKE(status, '.*ship.*')" 1
run_query "REGEXP_EXTRACT" \
  "SELECT REGEXP_EXTRACT(status, '(.*)', 1) AS p FROM ${FULL_TABLE} LIMIT 3" 1

echo ""
echo -e "${BOLD}[5] Date functions${RESET}"
run_query "EXTRACT YEAR/MONTH/DAY" \
  "SELECT EXTRACT(YEAR FROM order_date), EXTRACT(MONTH FROM order_date), EXTRACT(DAY FROM order_date) FROM ${FULL_TABLE} LIMIT 3" 1
run_query "DATE_TRUNC" \
  "SELECT DATE_TRUNC('month', order_date) AS mo FROM ${FULL_TABLE} LIMIT 3" 1
run_query "TO_CHAR YYYY-MM-DD" \
  "SELECT TO_CHAR(order_date, 'YYYY-MM-DD') AS d FROM ${FULL_TABLE} LIMIT 3" 1
run_query "TO_CHAR MON YYYY" \
  "SELECT TO_CHAR(order_date, 'MON YYYY') AS mo FROM ${FULL_TABLE} LIMIT 3" 1

echo ""
echo -e "${BOLD}[6] Conditional expressions${RESET}"
run_query "CASE WHEN / THEN / ELSE" \
  "SELECT CASE WHEN amount > 100.0 THEN 'big' ELSE 'small' END AS size FROM ${FULL_TABLE} LIMIT 5" 1
run_query "CASE WHEN multi-branch" \
  "SELECT CASE WHEN amount > 500 THEN 'large' WHEN amount > 100 THEN 'medium' ELSE 'small' END AS bucket, COUNT(*) FROM ${FULL_TABLE} GROUP BY 1" 1

echo ""
echo -e "${BOLD}[7] LIKE / pattern matching${RESET}"
run_query "LIKE basic" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE status LIKE 'ship%'" 0
run_query "LIKE ESCAPE" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE CAST(amount AS VARCHAR) LIKE '1%' ESCAPE '\\'" 0
run_query "REGEXP_LIKE case-insensitive (ILIKE workaround)" \
  "SELECT COUNT(*) FROM ${FULL_TABLE} WHERE REGEXP_LIKE(status, '(?i)PEND.*')" 0

echo ""
echo -e "${BOLD}[8] Math functions${RESET}"
run_query "ABS / CEIL / FLOOR / ROUND" \
  "SELECT ABS(-5.5), CEIL(3.2), FLOOR(3.9), ROUND(3.567, 2) FROM ${FULL_TABLE} LIMIT 1" 1
run_query "SQRT / POWER / LOG" \
  "SELECT SQRT(CAST(amount AS DOUBLE)), LOG10(100.0) FROM ${FULL_TABLE} LIMIT 1" 1

# ── Summary ───────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════${RESET}"
if [[ "$FAILURES" -eq 0 ]]; then
  echo -e "${BOLD}${GREEN}   All checks passed ✓${RESET}"
else
  echo -e "${BOLD}${RED}   ${FAILURES} check(s) failed ✗${RESET}"
fi
echo -e "${BOLD}${CYAN}════════════════════════════════════════${RESET}"
echo ""

exit "$FAILURES"
