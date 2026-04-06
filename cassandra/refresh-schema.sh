#!/usr/bin/env bash
# refresh-schema.sh — Trigger a Dremio metadata refresh for a Cassandra source
#
# PURPOSE
#   After a Cassandra schema change (new column, new table, type change),
#   this script brings Dremio's catalog up to date without waiting for the
#   automatic refresh timer (default: 1 hour).
#
# WHAT IT DOES
#   1. SCHEMA REFRESH (always):
#      Calls POST /api/v3/catalog/{id}/refresh for every dataset already
#      known to Dremio. Picks up column additions, removals, and type changes.
#
#   2. TABLE DISCOVERY (--full mode):
#      Compares Dremio's known table list with the live Cassandra schema.
#      For each new table, runs a LIMIT 0 query via Dremio's SQL API —
#      this triggers Dremio's auto-discovery mechanism and registers the table.
#      Requires access to cqlsh (--docker CONTAINER or --cassandra HOST).
#
# USAGE
#   ./refresh-schema.sh --source <source_name> [options]
#
# OPTIONS
#   --source    NAME        Dremio source name to refresh (required)
#   --dremio    URL         Dremio base URL (default: http://localhost:9047)
#   --user      USERNAME    Dremio username (default: dremio)
#   --password  PASSWORD    Dremio password (prompted interactively if omitted)
#   --keyspace  KEYSPACE    Limit refresh to one keyspace (default: all keyspaces)
#   --full                  Also discover new tables (requires Cassandra access)
#   --docker    CONTAINER   Docker container running cqlsh (used with --full)
#   --cassandra HOST        Cassandra host for cqlsh (used with --full, no Docker)
#   --cass-port PORT        Cassandra port (default: 9042)
#   --dry-run               Print what would be done without making changes
#   -h, --help              Show this help
#
# EXAMPLES
#   # Refresh schemas for all known tables
#   ./refresh-schema.sh --source cassandra_test
#
#   # Refresh schemas + discover new tables via Docker cqlsh
#   ./refresh-schema.sh --source cassandra_test --full --docker cassandra-test
#
#   # Refresh a single keyspace only
#   ./refresh-schema.sh --source cassandra_test --keyspace my_keyspace
#
#   # Non-interactive (CI/CD usage)
#   ./refresh-schema.sh --source cassandra_test --user mark --password s3cr3t --full --docker cassandra-test
#
# EXIT CODES
#   0  All refreshes succeeded (or dry-run completed)
#   1  Authentication or source-lookup failure
#   2  One or more dataset refreshes failed

set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# ── Colour helpers ─────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; RESET='\033[0m'
ok()    { echo -e "  ${GREEN}✓${RESET} $*"; }
warn()  { echo -e "  ${YELLOW}⚠${RESET} $*"; }
err()   { echo -e "${RED}✗ ERROR:${RESET} $*" >&2; exit 1; }
info()  { echo -e "  ${CYAN}→${RESET} $*"; }
header(){ echo -e "\n${CYAN}$*${RESET}"; }

# ── Defaults ───────────────────────────────────────────────────────────────────
DREMIO_URL="http://localhost:9047"
SOURCE_NAME=""
USERNAME="dremio"
PASSWORD=""
KEYSPACE_FILTER=""
FULL_REFRESH=false
DOCKER_CONTAINER=""
CASSANDRA_HOST=""
CASSANDRA_PORT="9042"
DRY_RUN=false

# ── Argument parsing ───────────────────────────────────────────────────────────
usage() {
  grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --source)    SOURCE_NAME="$2";      shift 2 ;;
    --dremio)    DREMIO_URL="${2%/}";   shift 2 ;;
    --user)      USERNAME="$2";         shift 2 ;;
    --password)  PASSWORD="$2";         shift 2 ;;
    --keyspace)  KEYSPACE_FILTER="$2";  shift 2 ;;
    --full)      FULL_REFRESH=true;     shift   ;;
    --docker)    DOCKER_CONTAINER="$2"; shift 2 ;;
    --cassandra) CASSANDRA_HOST="$2";   shift 2 ;;
    --cass-port) CASSANDRA_PORT="$2";   shift 2 ;;
    --dry-run)   DRY_RUN=true;          shift   ;;
    -h|--help)   usage; exit 0 ;;
    *) err "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ -z "$SOURCE_NAME" ]] && { usage; err "--source is required"; }

# ── Password prompt ────────────────────────────────────────────────────────────
if [[ -z "$PASSWORD" ]]; then
  read -rsp "Dremio password for '$USERNAME': " PASSWORD </dev/tty
  echo >&2
fi

# ── Authenticate ───────────────────────────────────────────────────────────────
header "Connecting to Dremio at $DREMIO_URL"

AUTH_JSON=$(curl -sf -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$USERNAME\",\"password\":\"$PASSWORD\"}" 2>/dev/null) \
  || err "Could not reach Dremio at $DREMIO_URL — is it running?"

TOKEN=$(echo "$AUTH_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('token',''))" 2>/dev/null)
[[ -z "$TOKEN" ]] && err "Authentication failed — check username/password"
ok "Authenticated as '$USERNAME'"

# Auth header function — token can contain shell-special chars, quote carefully
auth_h() { echo "Authorization: _dremio${TOKEN}"; }

# ── Look up source ─────────────────────────────────────────────────────────────
header "Looking up source '$SOURCE_NAME'"

SOURCE_ENTRY=$(curl -sf -H "$(auth_h)" \
  "$DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME" 2>/dev/null) \
  || err "Source '$SOURCE_NAME' not found. Check the name in Dremio → Sources."

SOURCE_TYPE=$(echo "$SOURCE_ENTRY" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('type',''))")
SOURCE_ID=$(echo "$SOURCE_ENTRY" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('id',''))")

[[ "$SOURCE_TYPE" != "APACHE_CASSANDRA" ]] && \
  warn "Source type is '$SOURCE_TYPE' (expected APACHE_CASSANDRA) — proceeding anyway"

ok "Found: '$SOURCE_NAME' (id: ${SOURCE_ID:0:8}…, type: $SOURCE_TYPE)"

# ── Enumerate keyspaces ────────────────────────────────────────────────────────
KEYSPACES=$(echo "$SOURCE_ENTRY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('children', []):
    if c.get('type') == 'CONTAINER':
        ks = c['path'][-1]
        print(ks)
")

if [[ -z "$KEYSPACES" ]]; then
  warn "No keyspaces found in source '$SOURCE_NAME'"
  warn "The source may need to be saved/refreshed once from the UI first."
  exit 0
fi

if [[ -n "$KEYSPACE_FILTER" ]]; then
  KEYSPACES=$(echo "$KEYSPACES" | grep -x "$KEYSPACE_FILTER" || true)
  [[ -z "$KEYSPACES" ]] && err "Keyspace '$KEYSPACE_FILTER' not found in source '$SOURCE_NAME'"
fi

# ── Schema refresh loop ────────────────────────────────────────────────────────
header "Refreshing dataset schemas"

TOTAL=0; REFRESHED=0; FAILED=0

for KS in $KEYSPACES; do
  echo ""
  info "Keyspace: $KS"

  KS_ENTRY=$(curl -sf -H "$(auth_h)" \
    "$DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME/$KS" 2>/dev/null) || {
    warn "Could not read keyspace '$KS' — skipping"
    continue
  }

  # Read each dataset id + name pair (space-separated, one per line)
  while IFS=' ' read -r DATASET_ID TABLE_NAME; do
    [[ -z "$DATASET_ID" ]] && continue
    TOTAL=$((TOTAL + 1))

    if [[ "$DRY_RUN" == "true" ]]; then
      ok "[dry-run] Would refresh $KS.$TABLE_NAME ($DATASET_ID)"
      REFRESHED=$((REFRESHED + 1))
      continue
    fi

    HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
      -X POST -H "$(auth_h)" \
      "$DREMIO_URL/api/v3/catalog/$DATASET_ID/refresh")

    if [[ "$HTTP" == "204" ]]; then
      ok "Refreshed $KS.$TABLE_NAME"
      REFRESHED=$((REFRESHED + 1))
    else
      warn "Failed to refresh $KS.$TABLE_NAME (HTTP $HTTP)"
      FAILED=$((FAILED + 1))
    fi
  done < <(echo "$KS_ENTRY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('children', []):
    if c.get('type') == 'DATASET':
        print(c['id'] + ' ' + c['path'][-1])
")
done

# ── Schema refresh summary ─────────────────────────────────────────────────────
echo ""
echo "Schema refresh: $REFRESHED/$TOTAL datasets refreshed${FAILED:+ ($FAILED failed)}"
SCHEMA_EXIT=0
[[ $FAILED -gt 0 ]] && { warn "Some refreshes failed — check Dremio logs"; SCHEMA_EXIT=2; }

# ── Table discovery (--full mode) ──────────────────────────────────────────────
if [[ "$FULL_REFRESH" == "true" ]]; then
  header "Discovering new tables"

  # Build cqlsh command.
  # cqlsh positional args are: [host] [port] — both optional.
  # In Docker mode without an explicit host, omit host+port entirely so
  # cqlsh connects to localhost:9042 inside the container (its default).
  CQLSH_CMD=""
  if [[ -n "$DOCKER_CONTAINER" ]]; then
    CQLSH_CMD="docker exec $DOCKER_CONTAINER cqlsh"
    if [[ -n "$CASSANDRA_HOST" ]]; then
      CQLSH_CMD="$CQLSH_CMD $CASSANDRA_HOST $CASSANDRA_PORT"
    fi
  elif [[ -n "$CASSANDRA_HOST" ]]; then
    command -v cqlsh &>/dev/null || err "cqlsh not found on PATH. Install it or use --docker."
    CQLSH_CMD="cqlsh $CASSANDRA_HOST $CASSANDRA_PORT"
  else
    err "--full requires Cassandra access. Add --docker CONTAINER or --cassandra HOST."
  fi

  DISCOVERED=0; DISCOVER_FAILED=0

  for KS in $KEYSPACES; do
    echo ""
    info "Scanning keyspace: $KS"

    # Get all tables from Cassandra
    CASS_TABLES=$($CQLSH_CMD -e \
      "SELECT table_name FROM system_schema.tables WHERE keyspace_name='$KS';" \
      2>/dev/null \
      | grep -vE "^\s*(table_name|-+|\([0-9]+ rows?\)|\s*$)" \
      | awk '{print $1}' \
      | grep -v "^$" \
      || true)

    if [[ -z "$CASS_TABLES" ]]; then
      warn "No tables found in Cassandra keyspace '$KS' (or cqlsh failed)"
      continue
    fi

    # Get tables already known to Dremio
    KS_ENTRY=$(curl -sf -H "$(auth_h)" \
      "$DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME/$KS" 2>/dev/null)
    KNOWN_TABLES=$(echo "$KS_ENTRY" | python3 -c "
import sys, json
d = json.load(sys.stdin)
for c in d.get('children', []):
    if c.get('type') == 'DATASET':
        print(c['path'][-1])
")

    for TABLE in $CASS_TABLES; do
      if echo "$KNOWN_TABLES" | grep -qx "$TABLE"; then
        continue  # already registered
      fi

      if [[ "$DRY_RUN" == "true" ]]; then
        ok "[dry-run] Would discover new table: $KS.$TABLE"
        DISCOVERED=$((DISCOVERED + 1))
        continue
      fi

      info "New table found: $KS.$TABLE — triggering discovery..."

      # Running LIMIT 0 against an unknown table causes Dremio to auto-discover it
      SQL="SELECT * FROM \\\"$SOURCE_NAME\\\".\\\"$KS\\\".\\\"$TABLE\\\" LIMIT 0"
      JOB_RESP=$(curl -sf -X POST "$DREMIO_URL/api/v3/sql" \
        -H "$(auth_h)" -H "Content-Type: application/json" \
        -d "{\"sql\": \"$SQL\"}" 2>/dev/null) || {
        warn "Failed to submit discovery query for $KS.$TABLE"
        DISCOVER_FAILED=$((DISCOVER_FAILED + 1))
        continue
      }

      JOB_ID=$(echo "$JOB_RESP" | python3 -c \
        "import sys,json; print(json.load(sys.stdin).get('id',''))")

      if [[ -z "$JOB_ID" ]]; then
        warn "No job ID returned for $KS.$TABLE discovery"
        DISCOVER_FAILED=$((DISCOVER_FAILED + 1))
        continue
      fi

      # Poll for completion (up to 30s)
      STATUS="RUNNING"
      for _ in 1 2 3 4 5 6; do
        sleep 5
        STATUS=$(curl -sf -H "$(auth_h)" \
          "$DREMIO_URL/api/v3/job/$JOB_ID" 2>/dev/null \
          | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobState','UNKNOWN'))")
        [[ "$STATUS" != "RUNNING" && "$STATUS" != "ENQUEUED" && "$STATUS" != "PLANNING" ]] && break
      done

      if [[ "$STATUS" == "COMPLETED" ]]; then
        ok "Discovered: $KS.$TABLE"
        DISCOVERED=$((DISCOVERED + 1))
      else
        warn "Discovery query for $KS.$TABLE ended with status: $STATUS"
        DISCOVER_FAILED=$((DISCOVER_FAILED + 1))
      fi
    done
  done

  echo ""
  echo "Table discovery: $DISCOVERED new table(s) registered${DISCOVER_FAILED:+ ($DISCOVER_FAILED failed)}"
fi

# ── Exit ───────────────────────────────────────────────────────────────────────
echo ""
if [[ $SCHEMA_EXIT -eq 0 ]]; then
  echo -e "${GREEN}Done.${RESET} Dremio schema is up to date."
else
  echo -e "${YELLOW}Done with warnings.${RESET} Check output above for failures."
fi
exit $SCHEMA_EXIT
