#!/usr/bin/env bash
# =============================================================================
# refresh-schema.sh — Force schema re-inference for Kafka topics in Dremio
#
# When a Kafka topic's message schema evolves, Dremio may cache the old schema.
# This script calls the Dremio REST API to refresh topic metadata, triggering
# re-inference on the next query.
#
# Usage: ./refresh-schema.sh [OPTIONS]
#   --url <url>          Dremio URL (default: http://localhost:9047)
#   --user <user>        Dremio username (prompted if omitted)
#   --password <pass>    Dremio password (prompted if omitted)
#   --source <name>      Kafka source name in Dremio (prompted if omitted)
#   --topic <name>       Topic to refresh (use --all to refresh every topic)
#   --all                Refresh all topics in the source
#   --help               Show help
# =============================================================================

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER=""
DREMIO_PASS=""
SOURCE_NAME=""
TOPIC_NAME=""
REFRESH_ALL=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --url)      DREMIO_URL="$2";   shift 2 ;;
    --user)     DREMIO_USER="$2";  shift 2 ;;
    --password) DREMIO_PASS="$2";  shift 2 ;;
    --source)   SOURCE_NAME="$2";  shift 2 ;;
    --topic)    TOPIC_NAME="$2";   shift 2 ;;
    --all)      REFRESH_ALL=true;  shift   ;;
    --help)
      sed -n '2,16p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# ── Colour helpers (same as test-connection.sh style) ────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Colour

info()    { echo -e "  ${BLUE}${*}${NC}"; }
ok()      { echo -e "  ${GREEN}OK${NC}  ${*}"; }
warn()    { echo -e "  ${YELLOW}WARN${NC} ${*}"; }
fail()    { echo -e "  ${RED}FAIL${NC} ${*}"; }

# ── Prompt for missing values ────────────────────────────────────────────────
if [[ -z "$DREMIO_USER" ]]; then
  read -rp "Dremio username: " DREMIO_USER
fi
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password: " DREMIO_PASS
  echo ""
fi
if [[ -z "$SOURCE_NAME" ]]; then
  read -rp "Kafka source name in Dremio: " SOURCE_NAME
fi

if [[ "$REFRESH_ALL" == "false" && -z "$TOPIC_NAME" ]]; then
  echo ""
  echo "  Specify --topic <name> to refresh a single topic, or --all to refresh all topics."
  echo ""
  read -rp "Topic name (or leave blank and re-run with --all): " TOPIC_NAME
  if [[ -z "$TOPIC_NAME" ]]; then
    echo "No topic specified. Use --all to refresh all topics."
    exit 1
  fi
fi

REFRESHED=0
FAILED=0

# ── Authenticate ─────────────────────────────────────────────────────────────
echo ""
echo "=== Dremio Kafka Schema Refresh ==="
echo "  Source : $SOURCE_NAME"
echo "  Dremio : $DREMIO_URL"
echo ""

TOKEN=$(curl -s -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('token',''))" 2>/dev/null || true)

if [[ -z "$TOKEN" ]]; then
  fail "Could not authenticate with Dremio at $DREMIO_URL"
  exit 1
fi
info "Authenticated OK"
echo ""

# ── Helper: run a SQL statement and wait for completion ───────────────────────
run_sql_wait() {
  local sql="$1"
  local body
  body=$(python3 -c "import json,sys; print(json.dumps({'sql':sys.argv[1]}))" "$sql")

  local resp job_id
  resp=$(curl -s -X POST "$DREMIO_URL/api/v3/sql" \
    -H "Authorization: _dremio$TOKEN" \
    -H "Content-Type: application/json" -d "$body" 2>/dev/null || true)
  job_id=$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null || true)
  [ -z "$job_id" ] && { echo "SUBMIT_FAIL"; return; }

  for i in $(seq 1 30); do
    sleep 1
    local st
    st=$(curl -s -H "Authorization: _dremio$TOKEN" "$DREMIO_URL/api/v3/job/${job_id}" \
      | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobState','?'))" 2>/dev/null)
    case "$st" in
      COMPLETED) echo "OK"; return ;;
      FAILED|CANCELED)
        local err
        err=$(curl -s -H "Authorization: _dremio$TOKEN" "$DREMIO_URL/api/v3/job/${job_id}" \
          | python3 -c "import sys,json; print(json.load(sys.stdin).get('errorMessage','?')[:120])" 2>/dev/null)
        echo "FAILED: $err"; return ;;
    esac
  done
  echo "TIMEOUT"
}

# ── Helper: trigger metadata refresh for a topic via SQL ─────────────────────
refresh_dataset() {
  local topic="$1"
  local label="$2"

  local result
  result=$(run_sql_wait "ALTER TABLE ${SOURCE_NAME}.\"${topic}\" REFRESH METADATA")

  if [[ "$result" == "OK" ]]; then
    ok "Refreshed: $label"
    REFRESHED=$((REFRESHED + 1))
  else
    fail "Refresh failed ($result): $label"
    FAILED=$((FAILED + 1))
  fi
}

# ── Refresh single topic ──────────────────────────────────────────────────────
refresh_topic() {
  local topic="$1"
  refresh_dataset "$topic" "$SOURCE_NAME/$topic"
}

# ── Refresh all topics ────────────────────────────────────────────────────────
refresh_all_topics() {
  local source_path
  source_path=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$SOURCE_NAME', safe=''))")

  local response
  response=$(curl -s "$DREMIO_URL/api/v3/catalog/by-path/$source_path" \
    -H "Authorization: _dremio$TOKEN" 2>/dev/null || true)

  if [[ -z "$response" ]]; then
    fail "Could not list source: $SOURCE_NAME"
    exit 1
  fi

  # Extract dataset children (type == "DATASET")
  local topics
  topics=$(echo "$response" | python3 -c "
import sys, json
d = json.load(sys.stdin)
children = d.get('children', [])
for child in children:
    if child.get('type') == 'DATASET':
        path = child.get('path', [])
        if len(path) >= 2:
            print(path[-1])
" 2>/dev/null || true)

  if [[ -z "$topics" ]]; then
    warn "No dataset children found under source '$SOURCE_NAME'."
    warn "Topics may not be promoted yet — try browsing the source in Dremio UI first."
    return
  fi

  local count
  count=$(echo "$topics" | wc -l | tr -d ' ')
  info "Found $count topic(s) in source '$SOURCE_NAME'"
  echo ""

  while IFS= read -r topic; do
    [[ -z "$topic" ]] && continue
    info "Refreshing topic '$topic'..."
    refresh_dataset "$topic" "$SOURCE_NAME/$topic"
  done <<< "$topics"
}

# ── Main ──────────────────────────────────────────────────────────────────────
if [[ "$REFRESH_ALL" == "true" ]]; then
  info "Refreshing all topics in source '$SOURCE_NAME'..."
  echo ""
  refresh_all_topics
else
  info "Refreshing topic '$TOPIC_NAME'..."
  echo ""
  refresh_topic "$TOPIC_NAME"
fi

echo ""
echo "=== Results ==="
echo "  Refreshed : $REFRESHED"
echo "  Failed    : $FAILED"
echo ""

if [[ $FAILED -gt 0 ]]; then
  exit 1
fi
