#!/usr/bin/env bash
# add-salesforce-source.sh — Add a Salesforce (REST) source to Dremio via REST API
#
# PURPOSE
#   Creates or updates a Salesforce (REST) source in Dremio without going through
#   the UI. Authenticates to Dremio, then calls PUT /apiv2/source/{name} with the
#   Salesforce credentials and configuration you supply.
#
# USAGE
#   ./add-salesforce-source.sh --name <source_name> --sf-user <email> [options]
#
# REQUIRED
#   --name          NAME        Dremio source name to create
#   --sf-user       EMAIL       Salesforce username (email address)
#   --sf-pass       PASSWORD    Salesforce password (prompted interactively if omitted)
#   --sf-token      TOKEN       Salesforce security token
#   --client-id     KEY         Connected App Consumer Key
#   --client-secret SECRET      Connected App Consumer Secret (prompted if omitted)
#
# OPTIONAL
#   --dremio        URL         Dremio base URL (default: http://localhost:9047)
#   --user          USERNAME    Dremio username (default: dremio)
#   --password      PASSWORD    Dremio password (prompted interactively if omitted)
#   --login-url     URL         Salesforce login URL (default: https://login.salesforce.com)
#   --api-version   VERSION     Salesforce API version (default: 59.0)
#   --page-size     N           Records per page, max 2000 (default: 2000)
#   --parallelism   N           Parallel reader splits (default: 4)
#   --timeout       SECONDS     Query timeout in seconds (default: 120)
#   --excluded      OBJECTS     Comma-separated SObject names to hide
#   --force                     Delete and recreate if source already exists
#   --dry-run                   Print payload without submitting
#   -h, --help                  Show this help
#
# EXAMPLES
#   # Minimal — will prompt for passwords interactively
#   ./add-salesforce-source.sh --name salesforce \
#     --sf-user me@example.com --sf-token myToken \
#     --client-id 3MVG... --client-secret ABC123
#
#   # Fully non-interactive (CI/CD)
#   ./add-salesforce-source.sh \
#     --name salesforce \
#     --sf-user me@example.com --sf-pass mypass --sf-token myToken \
#     --client-id 3MVG... --client-secret ABC123 \
#     --user dremio_admin --password dremio_pass
#
#   # Sandbox org
#   ./add-salesforce-source.sh --name sf_sandbox \
#     --sf-user me@example.com.sandbox --sf-pass mypass --sf-token myToken \
#     --client-id 3MVG... --client-secret ABC123 \
#     --login-url https://test.salesforce.com
#
# EXIT CODES
#   0  Source created successfully
#   1  Authentication or connection failure
#   2  Source creation failed

set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; RESET='\033[0m'
ok()    { echo -e "  ${GREEN}✓${RESET} $*"; }
warn()  { echo -e "  ${YELLOW}⚠${RESET} $*"; }
err()   { echo -e "${RED}✗ ERROR:${RESET} $*" >&2; exit 1; }
info()  { echo -e "  ${CYAN}→${RESET} $*"; }
header(){ echo -e "\n${CYAN}$*${RESET}"; }

# Defaults
DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
SF_USER=""
SF_PASS=""
SF_TOKEN=""
CLIENT_ID=""
CLIENT_SECRET=""
LOGIN_URL="https://login.salesforce.com"
API_VERSION="59.0"
PAGE_SIZE="2000"
PARALLELISM="4"
TIMEOUT_SECONDS="120"
EXCLUDED_OBJECTS=""
DRY_RUN=false
FORCE=false

usage() { grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)           SOURCE_NAME="$2";       shift 2 ;;
    --dremio)         DREMIO_URL="${2%/}";    shift 2 ;;
    --user)           DREMIO_USER="$2";       shift 2 ;;
    --password)       DREMIO_PASS="$2";       shift 2 ;;
    --sf-user)        SF_USER="$2";           shift 2 ;;
    --sf-pass)        SF_PASS="$2";           shift 2 ;;
    --sf-token)       SF_TOKEN="$2";          shift 2 ;;
    --client-id)      CLIENT_ID="$2";         shift 2 ;;
    --client-secret)  CLIENT_SECRET="$2";     shift 2 ;;
    --login-url)      LOGIN_URL="$2";         shift 2 ;;
    --api-version)    API_VERSION="$2";       shift 2 ;;
    --page-size)      PAGE_SIZE="$2";         shift 2 ;;
    --parallelism)    PARALLELISM="$2";       shift 2 ;;
    --timeout)        TIMEOUT_SECONDS="$2";   shift 2 ;;
    --excluded)       EXCLUDED_OBJECTS="$2";  shift 2 ;;
    --force)          FORCE=true;             shift   ;;
    --dry-run)        DRY_RUN=true;           shift   ;;
    -h|--help)        usage; exit 0 ;;
    *) err "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ -z "$SOURCE_NAME" ]] && { usage; err "--name is required"; }
[[ -z "$SF_USER"     ]] && { usage; err "--sf-user is required"; }
[[ -z "$SF_TOKEN"    ]] && { usage; err "--sf-token is required"; }
[[ -z "$CLIENT_ID"   ]] && { usage; err "--client-id is required"; }

# Interactive password prompts
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS </dev/tty; echo >&2
fi
if [[ -z "$SF_PASS" ]]; then
  read -rsp "Salesforce password for '$SF_USER': " SF_PASS </dev/tty; echo >&2
fi
if [[ -z "$CLIENT_SECRET" ]]; then
  read -rsp "Connected App Consumer Secret: " CLIENT_SECRET </dev/tty; echo >&2
fi

# Authenticate to Dremio
header "Connecting to Dremio at $DREMIO_URL"

AUTH_JSON=$(curl -sf -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" 2>/dev/null) \
  || err "Could not reach Dremio at $DREMIO_URL — is it running?"

TOKEN=$(echo "$AUTH_JSON" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)
[[ -z "$TOKEN" ]] && err "Authentication failed — check Dremio username/password"
ok "Authenticated as '$DREMIO_USER'"

# Check if source exists
header "Checking source '$SOURCE_NAME'"

EXISTING=$(curl -sf -H "Authorization: _dremio${TOKEN}" \
  "$DREMIO_URL/apiv2/source/$SOURCE_NAME" 2>/dev/null) || true
EXISTING_ID=$(echo "$EXISTING" | python3 -c \
  "import sys,json
try: print(json.load(sys.stdin).get('id',''))
except: print('')" 2>/dev/null)

if [[ -n "$EXISTING_ID" ]]; then
  if [[ "$FORCE" == "false" ]]; then
    ok "Source '$SOURCE_NAME' already exists (id: ${EXISTING_ID:0:8}…)"
    info "Re-run with --force to delete and recreate it."
    exit 0
  fi
  warn "Source '$SOURCE_NAME' exists — --force: deleting it first..."
  CATALOG_RESP=$(curl -sf -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/api/v3/catalog/${EXISTING_ID}" 2>/dev/null) || true
  VERSION_TAG=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try: print(json.load(sys.stdin).get('tag',''))
except: print('')" 2>/dev/null)
  [[ -z "$VERSION_TAG" ]] && err "Could not get version tag — delete manually via Dremio UI"
  ENCODED_TAG=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$VERSION_TAG'))")
  DEL_HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/apiv2/source/$SOURCE_NAME?version=${ENCODED_TAG}" 2>/dev/null)
  [[ "$DEL_HTTP" != "200" && "$DEL_HTTP" != "204" ]] && \
    err "Delete failed (HTTP $DEL_HTTP) — delete manually via Dremio UI"
  ok "Deleted existing source (HTTP $DEL_HTTP)"
fi

# Build payload
header "Creating source '$SOURCE_NAME'"

PAYLOAD=$(python3 -c "
import json

config = {
    'loginUrl':           '$LOGIN_URL',
    'username':           '$SF_USER',
    'password':           '$SF_PASS',
    'securityToken':      '$SF_TOKEN',
    'clientId':           '$CLIENT_ID',
    'clientSecret':       '$CLIENT_SECRET',
    'apiVersion':         '$API_VERSION',
    'recordsPerPage':     $PAGE_SIZE,
    'splitParallelism':   $PARALLELISM,
    'queryTimeoutSeconds': $TIMEOUT_SECONDS,
    'excludedObjects':    '$EXCLUDED_OBJECTS',
}

source = {
    'name':   '$SOURCE_NAME',
    'type':   'SALESFORCE_REST',
    'config': config,
    'metadataPolicy': {
        'updateMode':                          'PREFETCH_QUERIED',
        'namesRefreshMillis':                  3600000,
        'authTTLMillis':                       86400000,
        'datasetDefinitionRefreshAfterMillis': 3600000,
        'datasetDefinitionExpireAfterMillis':  10800000,
        'deleteUnavailableDatasets':           True,
        'autoPromoteDatasets':                 False,
    }
}

print(json.dumps(source, indent=2))
")

if [[ "$DRY_RUN" == "true" ]]; then
  info "Dry-run payload:"
  echo "$PAYLOAD"
  exit 0
fi

info "Submitting source configuration..."

HTTP=$(curl -s -o /tmp/_sf_source_resp.json -w "%{http_code}" \
  -X PUT "$DREMIO_URL/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" 2>/dev/null)

if [[ "$HTTP" != "200" ]]; then
  ERR_MSG=$(python3 -c \
    "import sys,json; print(json.load(open('/tmp/_sf_source_resp.json')).get('errorMessage','unknown'))" \
    2>/dev/null || cat /tmp/_sf_source_resp.json)
  err "Source creation failed (HTTP $HTTP): $ERR_MSG"
fi

RESPONSE=$(cat /tmp/_sf_source_resp.json)
SOURCE_ID=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
NUM_DATASETS=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('numberOfDatasets','?'))" 2>/dev/null)

echo ""
ok "Source '$SOURCE_NAME' created (id: ${SOURCE_ID:0:8}…)"
info "SObjects discovered: $NUM_DATASETS"
info "Query with: SELECT Id, Name FROM \"$SOURCE_NAME\".Account LIMIT 10;"
echo ""
echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is ready in Dremio."
