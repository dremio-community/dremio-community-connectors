#!/usr/bin/env bash
# add-stripe-source.sh — Add a Stripe source to Dremio via REST API
#
# PURPOSE
#   Creates or updates a Stripe source in Dremio without going through the UI.
#   Authenticates to Dremio, then calls PUT /apiv2/source/{name} with your
#   Stripe API key and any optional configuration.
#
# USAGE
#   ./add-stripe-source.sh --name <source_name> --api-key <key> [options]
#
# REQUIRED
#   --name       NAME     Dremio source name to create (e.g. stripe, stripe_test)
#   --api-key    KEY      Stripe secret API key (sk_live_... or sk_test_...)
#
# OPTIONAL
#   --dremio     URL      Dremio base URL (default: http://localhost:9047)
#   --user       USERNAME Dremio username (default: dremio)
#   --password   PASSWORD Dremio password (prompted interactively if omitted)
#   --base-url   URL      Override Stripe API base URL (default: https://api.stripe.com)
#                         Use http://stripe-mock:12111 for testing with stripe-mock
#   --page-size  N        Records per Stripe API call, max 100 (default: 100)
#   --timeout    SECONDS  HTTP request timeout in seconds (default: 60)
#   --force              Delete and recreate if source already exists
#   --dry-run            Print payload without submitting
#   -h, --help           Show this help
#
# EXAMPLES
#   # Production key (interactive password prompt)
#   ./add-stripe-source.sh --name stripe --api-key sk_live_xxx
#
#   # Test key, non-interactive
#   ./add-stripe-source.sh \
#     --name stripe_test \
#     --api-key sk_test_xxx \
#     --user mark --password mypassword
#
#   # Point at stripe-mock for local testing
#   ./add-stripe-source.sh \
#     --name stripe_mock \
#     --api-key sk_test_mock \
#     --base-url http://stripe-mock:12111
#
#   # CI/CD — fully non-interactive
#   ./add-stripe-source.sh \
#     --name stripe \
#     --api-key "$STRIPE_SECRET_KEY" \
#     --user dremio_admin \
#     --password "$DREMIO_PASS"
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
STRIPE_API_KEY=""
STRIPE_BASE_URL="https://api.stripe.com"
PAGE_SIZE="100"
TIMEOUT_SECONDS="60"
DRY_RUN=false
FORCE=false

usage() { grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)      SOURCE_NAME="$2";      shift 2 ;;
    --api-key)   STRIPE_API_KEY="$2";   shift 2 ;;
    --dremio)    DREMIO_URL="${2%/}";   shift 2 ;;
    --user)      DREMIO_USER="$2";      shift 2 ;;
    --password)  DREMIO_PASS="$2";      shift 2 ;;
    --base-url)  STRIPE_BASE_URL="$2";  shift 2 ;;
    --page-size) PAGE_SIZE="$2";        shift 2 ;;
    --timeout)   TIMEOUT_SECONDS="$2";  shift 2 ;;
    --force)     FORCE=true;            shift   ;;
    --dry-run)   DRY_RUN=true;          shift   ;;
    -h|--help)   usage; exit 0 ;;
    *) err "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ -z "$SOURCE_NAME"    ]] && { usage; err "--name is required"; }
[[ -z "$STRIPE_API_KEY" ]] && { usage; err "--api-key is required"; }

# Interactive password prompt
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS </dev/tty; echo >&2
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
    'apiKey':               '$STRIPE_API_KEY',
    'baseUrl':              '$STRIPE_BASE_URL',
    'pageSize':             $PAGE_SIZE,
    'queryTimeoutSeconds':  $TIMEOUT_SECONDS,
}

source = {
    'name':   '$SOURCE_NAME',
    'type':   'STRIPE',
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

HTTP=$(curl -s -o /tmp/_stripe_source_resp.json -w "%{http_code}" \
  -X PUT "$DREMIO_URL/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" 2>/dev/null)

if [[ "$HTTP" != "200" ]]; then
  ERR_MSG=$(python3 -c \
    "import sys,json; print(json.load(open('/tmp/_stripe_source_resp.json')).get('errorMessage','unknown'))" \
    2>/dev/null || cat /tmp/_stripe_source_resp.json)
  err "Source creation failed (HTTP $HTTP): $ERR_MSG"
fi

RESPONSE=$(cat /tmp/_stripe_source_resp.json)
SOURCE_ID=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)

echo ""
ok "Source '$SOURCE_NAME' created (id: ${SOURCE_ID:0:8}…)"
info "Tables: charges, customers, subscriptions, invoices, payment_intents, products, prices, refunds, balance_transactions"
info "Query with: SELECT id, amount, currency, status FROM \"$SOURCE_NAME\".charges LIMIT 10;"
echo ""
echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is ready in Dremio."
