#!/usr/bin/env bash
# add-cassandra-source.sh — Add a new Apache Cassandra source to Dremio via REST API
#
# PURPOSE
#   Dremio 26.x's "Add Source" dialog has CASSANDRA listed as a disabled
#   placeholder (reserved for a future built-in connector). This script
#   bypasses the UI and creates the source directly via the Dremio v2 API.
#
# WHAT IT DOES
#   Calls PUT /apiv2/source/{name} to create or update a Cassandra source
#   with the configuration you supply. On success, Dremio immediately
#   connects and discovers all keyspaces and tables.
#
# USAGE
#   ./add-cassandra-source.sh --name <source_name> --cassandra <host>[,<host>...] [options]
#
# OPTIONS
#   --name        NAME         Dremio source name to create (required)
#   --cassandra   HOST[,HOST]  Cassandra contact point host(s) or IP(s), comma-separated (required)
#   --dremio      URL          Dremio base URL (default: http://localhost:9047)
#   --user        USERNAME     Dremio username (default: dremio)
#   --password    PASSWORD     Dremio password (prompted interactively if omitted)
#   --cass-port   PORT         Cassandra native transport port (default: 9042)
#   --datacenter  DC           Local datacenter name (default: auto-detect)
#   --cass-user   USERNAME     Cassandra username (default: none / no auth)
#   --cass-pass   PASSWORD     Cassandra password (default: none / no auth)
#   --keyspace    KEYSPACE     Comma-separated keyspaces to exclude (default: none extra)
#   --parallelism N            Token-range split parallelism (default: 8)
#   --timeout     MS           CQL read timeout in ms (default: 30000)
#   --ssl                      Enable SSL/TLS (default: off)
#   --compression  ALGO        CQL wire compression: NONE (default), LZ4, or SNAPPY
#   --no-async-prefetch        Disable async CQL page prefetch (default: enabled)
#   --force                    If the source already exists (or is unavailable), delete it
#                              and recreate it with the new configuration. Without this flag
#                              the script exits if the source already exists.
#   --dry-run                  Print the JSON payload without submitting
#   -h, --help                 Show this help
#
# EXAMPLES
#   # Minimal — connect to local Cassandra, auto-detect everything
#   ./add-cassandra-source.sh --name my_cassandra --cassandra localhost
#
#   # Multi-node cluster — comma-separated contact points (no spaces)
#   ./add-cassandra-source.sh --name prod_cassandra \
#     --cassandra 10.0.0.10,10.0.0.11,10.0.0.12
#
#   # Named datacenter, custom parallelism, Cassandra auth
#   ./add-cassandra-source.sh \
#     --name prod_cassandra \
#     --cassandra 10.0.0.10,10.0.0.11 \
#     --datacenter datacenter1 \
#     --cass-user cassandra \
#     --cass-pass secret \
#     --parallelism 16
#
#   # Non-interactive (CI/CD)
#   ./add-cassandra-source.sh \
#     --name ci_cassandra \
#     --cassandra cassandra-host \
#     --user dremio_admin \
#     --password dremio_pass \
#     --cass-user cassandra \
#     --cass-pass cassandra
#
#   # Force delete and recreate — use when source is unavailable or has a stale IP
#   ./add-cassandra-source.sh \
#     --name cassandra_test \
#     --cassandra cassandra-test \
#     --user mark \
#     --password mypassword \
#     --force
#
# EXIT CODES
#   0  Source created / updated successfully
#   1  Authentication, connection, or source state failure
#   2  Source creation failed

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
DREMIO_USER="dremio"
DREMIO_PASS=""
CASS_HOST=""
CASS_PORT="9042"
CASS_DATACENTER=""
CASS_USER=""
CASS_PASS=""
EXCLUDED_KEYSPACES=""
PARALLELISM="8"
TIMEOUT_MS="30000"
FETCH_SIZE="1000"
SSL_ENABLED="false"
COMPRESSION="NONE"
ASYNC_PREFETCH="true"
DRY_RUN=false
FORCE=false

# ── Argument parsing ───────────────────────────────────────────────────────────
usage() {
  grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'
}

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)        SOURCE_NAME="$2";        shift 2 ;;
    --dremio)      DREMIO_URL="${2%/}";     shift 2 ;;
    --user)        DREMIO_USER="$2";        shift 2 ;;
    --password)    DREMIO_PASS="$2";        shift 2 ;;
    --cassandra)   CASS_HOST="$2";          shift 2 ;;
    --cass-port)   CASS_PORT="$2";          shift 2 ;;
    --datacenter)  CASS_DATACENTER="$2";    shift 2 ;;
    --cass-user)   CASS_USER="$2";          shift 2 ;;
    --cass-pass)   CASS_PASS="$2";          shift 2 ;;
    --keyspace)    EXCLUDED_KEYSPACES="$2"; shift 2 ;;
    --parallelism) PARALLELISM="$2";        shift 2 ;;
    --timeout)     TIMEOUT_MS="$2";         shift 2 ;;
    --ssl)              SSL_ENABLED="true";   shift   ;;
    --compression)      COMPRESSION="$2";    shift 2 ;;
    --no-async-prefetch) ASYNC_PREFETCH="false"; shift ;;
    --force)            FORCE=true;          shift   ;;
    --dry-run)          DRY_RUN=true;        shift   ;;
    -h|--help)     usage; exit 0 ;;
    *) err "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ -z "$SOURCE_NAME" ]] && { usage; err "--name is required"; }
[[ -z "$CASS_HOST"   ]] && { usage; err "--cassandra is required"; }

# ── Dremio password prompt ─────────────────────────────────────────────────────
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS </dev/tty
  echo >&2
fi

# ── Authenticate ───────────────────────────────────────────────────────────────
header "Connecting to Dremio at $DREMIO_URL"

AUTH_JSON=$(curl -sf -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" 2>/dev/null) \
  || err "Could not reach Dremio at $DREMIO_URL — is it running?"

TOKEN=$(echo "$AUTH_JSON" | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(d.get('token',''))" 2>/dev/null)
[[ -z "$TOKEN" ]] && err "Authentication failed — check username/password"
ok "Authenticated as '$DREMIO_USER'"

# ── Check if source already exists ────────────────────────────────────────────
header "Checking source '$SOURCE_NAME'"

EXISTING=$(curl -sf \
  -H "Authorization: _dremio${TOKEN}" \
  "$DREMIO_URL/apiv2/source/$SOURCE_NAME" 2>/dev/null) || true

# Check if the response is a valid source JSON (has an "id" field)
EXISTING_ID=""
if [[ -n "$EXISTING" ]]; then
  EXISTING_ID=$(echo "$EXISTING" | python3 -c \
    "import sys,json
try:
    d = json.load(sys.stdin)
    print(d.get('id',''))
except:
    print('')
" 2>/dev/null)
fi

if [[ -n "$EXISTING_ID" ]]; then
  if [[ "$FORCE" == "false" ]]; then
    echo ""
    ok "Source '$SOURCE_NAME' already exists (id: ${EXISTING_ID:0:8}…)"
    info "To update its settings, open the Dremio UI:"
    info "  Sources → $SOURCE_NAME → ⚙ (settings icon) → Edit"
    info "Or re-run with --force to delete and recreate it."
    echo ""
    echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is already configured and ready."
    exit 0
  fi

  # --force: delete the existing source, then recreate
  warn "Source '$SOURCE_NAME' exists — --force specified, deleting it first..."

  # Get the version tag required for deletion (optimistic concurrency lock)
  CATALOG_RESP=$(curl -sf \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/api/v3/catalog/${EXISTING_ID}" 2>/dev/null) || true

  VERSION_TAG=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('tag',''))
except:
    print('')
" 2>/dev/null)

  if [[ -z "$VERSION_TAG" ]]; then
    err "Could not retrieve version tag for '$SOURCE_NAME' (id: ${EXISTING_ID:0:8}…).
       Try deleting the source manually in the Dremio UI first, then re-run without --force."
  fi

  ENCODED_TAG=$(python3 -c "import urllib.parse, sys; print(urllib.parse.quote('$VERSION_TAG'))" 2>/dev/null)

  DEL_HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/apiv2/source/$SOURCE_NAME?version=${ENCODED_TAG}" 2>/dev/null)

  if [[ "$DEL_HTTP" != "200" ]] && [[ "$DEL_HTTP" != "204" ]]; then
    err "Delete failed (HTTP $DEL_HTTP). The source may be locked by an active query.
       Wait a few seconds and retry, or delete via the Dremio UI."
  fi
  ok "Deleted existing source '$SOURCE_NAME' (HTTP $DEL_HTTP)"

elif [[ -n "$EXISTING" ]]; then
  # Got a response but no valid id — source is in an error/unavailable state
  if [[ "$FORCE" == "false" ]]; then
    warn "Source '$SOURCE_NAME' exists but appears unavailable."
    info "Re-run with --force to delete and recreate it, or edit it in the Dremio UI."
    exit 1
  fi
  warn "Source '$SOURCE_NAME' is unavailable — attempting recovery via catalog API..."

  # Look up the source by path in the v3 catalog API (works even when source is bad)
  CATALOG_RESP=$(curl -sf \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME" 2>/dev/null) || true

  EXISTING_ID=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('id',''))
except:
    print('')
" 2>/dev/null)

  if [[ -z "$EXISTING_ID" ]]; then
    err "Cannot find '$SOURCE_NAME' in the catalog. It may have already been deleted.
       Try: curl -s -H 'Authorization: _dremio${TOKEN}' $DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME"
  fi

  VERSION_TAG=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('tag',''))
except:
    print('')
" 2>/dev/null)

  if [[ -z "$VERSION_TAG" ]]; then
    # tag may require a second lookup via the full catalog entry
    FULL_RESP=$(curl -sf \
      -H "Authorization: _dremio${TOKEN}" \
      "$DREMIO_URL/api/v3/catalog/${EXISTING_ID}" 2>/dev/null) || true
    VERSION_TAG=$(echo "$FULL_RESP" | python3 -c \
      "import sys,json
try:
    print(json.load(sys.stdin).get('tag',''))
except:
    print('')
" 2>/dev/null)
  fi

  if [[ -z "$VERSION_TAG" ]]; then
    err "Could not retrieve version tag for unavailable source '$SOURCE_NAME'.
       Delete it manually via the Dremio UI (Sources → ⚙ → Delete Source) and re-run."
  fi

  ENCODED_TAG=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$VERSION_TAG'))" 2>/dev/null)

  DEL_HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/apiv2/source/$SOURCE_NAME?version=${ENCODED_TAG}" 2>/dev/null)

  if [[ "$DEL_HTTP" != "200" ]] && [[ "$DEL_HTTP" != "204" ]]; then
    err "Delete failed (HTTP $DEL_HTTP).
       Try deleting via the Dremio UI: Sources → $SOURCE_NAME → ⚙ → Delete Source"
  fi
  ok "Deleted unavailable source '$SOURCE_NAME' (HTTP $DEL_HTTP)"
fi

ACTION="Creating"

# ── Build payload ──────────────────────────────────────────────────────────────
header "${ACTION} source '$SOURCE_NAME'"

PAYLOAD=$(python3 -c "
import json, sys

ssl_on = '$SSL_ENABLED' == 'true'

# Core config — only fields accepted by Dremio's validation layer
# (sslHostnameVerification, sslTruststorePath, fallbackDatacenters, etc.
#  must not be sent as empty strings — Dremio rejects them)
config = {
    'host':                      '$CASS_HOST',
    'port':                      $CASS_PORT,
    'datacenter':                '$CASS_DATACENTER',
    'readTimeoutMs':             $TIMEOUT_MS,
    'fetchSize':                 $FETCH_SIZE,
    'excludedKeyspaces':         '$EXCLUDED_KEYSPACES',
    'consistencyLevel':          'LOCAL_ONE',
    'sslEnabled':                ssl_on,
    'speculativeExecutionEnabled': False,
    'speculativeExecutionDelayMs': 500,
    'splitParallelism':          $PARALLELISM,
    'metadataCacheTtlSeconds':   60,
    'compressionAlgorithm':      '$COMPRESSION',
    'asyncPagePrefetch':         '$ASYNC_PREFETCH' == 'true',
}

# Add Cassandra auth only if credentials supplied
if '$CASS_USER':
    config['username'] = '$CASS_USER'
if '$CASS_PASS':
    config['password'] = '$CASS_PASS'

source = {
    'name':   '$SOURCE_NAME',
    'type':   'APACHE_CASSANDRA',
    'config': config,
    'metadataPolicy': {
        'updateMode':                        'PREFETCH_QUERIED',
        'namesRefreshMillis':                3600000,
        'authTTLMillis':                     86400000,
        'datasetDefinitionRefreshAfterMillis': 3600000,
        'datasetDefinitionExpireAfterMillis':  10800000,
        'deleteUnavailableDatasets':         True,
        'autoPromoteDatasets':               False,
    }
}

print(json.dumps(source, indent=2))
")

if [[ "$DRY_RUN" == "true" ]]; then
  echo ""
  info "Dry-run: would submit this payload to PUT $DREMIO_URL/apiv2/source/$SOURCE_NAME:"
  echo "$PAYLOAD"
  exit 0
fi

info "Submitting source configuration..."

HTTP=$(curl -s -o /tmp/_dremio_source_resp.json -w "%{http_code}" \
  -X PUT "$DREMIO_URL/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" 2>/dev/null)

if [[ "$HTTP" != "200" ]]; then
  ERR_MSG=$(python3 -c \
    "import sys,json; print(json.load(open('/tmp/_dremio_source_resp.json')).get('errorMessage','unknown'))" \
    2>/dev/null || cat /tmp/_dremio_source_resp.json)
  err "Source creation failed (HTTP $HTTP): $ERR_MSG
       Hint: verify Cassandra is reachable from the Dremio container:
         docker exec try-dremio bash -c 'curl -s telnet://$CASS_HOST:$CASS_PORT'"
fi

RESPONSE=$(cat /tmp/_dremio_source_resp.json)

# ── Verify ─────────────────────────────────────────────────────────────────────
SOURCE_TYPE=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('type',''))" 2>/dev/null)
SOURCE_ID=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
NUM_DATASETS=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('numberOfDatasets', '?'))" 2>/dev/null)

[[ "$SOURCE_TYPE" != "APACHE_CASSANDRA" ]] && warn "Unexpected source type in response: '$SOURCE_TYPE'"

echo ""
ok "${ACTION} complete: '$SOURCE_NAME' (id: ${SOURCE_ID:0:8}…)"
info "Datasets discovered: $NUM_DATASETS"
info "Query with: SELECT * FROM \"$SOURCE_NAME\".\"<keyspace>\".\"<table>\" LIMIT 10;"
echo ""
echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is ready in Dremio."
