#!/usr/bin/env bash
# add-dynamodb-source.sh — Add a new Amazon DynamoDB source to Dremio via REST API
#
# USAGE
#   ./add-dynamodb-source.sh --name <source_name> [options]
#
# OPTIONS
#   --name        NAME         Dremio source name to create (required)
#   --dremio      URL          Dremio base URL (default: http://localhost:9047)
#   --user        USERNAME     Dremio username (default: dremio)
#   --password    PASSWORD     Dremio password (prompted interactively if omitted)
#   --region      REGION       AWS region (default: us-east-1)
#   --endpoint    URL          DynamoDB endpoint override (default: blank = AWS)
#   --access-key  KEY          AWS Access Key ID (default: blank = IAM/instance profile)
#   --secret-key  SECRET       AWS Secret Access Key (default: blank)
#   --sample-size N            Schema sample size (default: 100)
#   --parallelism N            Split parallelism (default: 4)
#   --timeout     SECONDS      Read timeout in seconds (default: 30)
#   --page-size   N            Max items per page (default: 1000)
#   --cache-ttl   SECONDS      Schema cache TTL (default: 60)
#   --force                    Delete and recreate if source already exists
#   --dry-run                  Print JSON payload without submitting
#   -h, --help                 Show this help
#
# EXAMPLES
#   # DynamoDB Local (Docker)
#   ./add-dynamodb-source.sh \
#     --name dynamodb_local \
#     --endpoint http://dynamodb-local:8000 \
#     --access-key fakeKey --secret-key fakeSecret
#
#   # AWS DynamoDB (IAM role / instance profile — leave credentials blank)
#   ./add-dynamodb-source.sh --name dynamodb_prod --region us-east-1
#
#   # AWS DynamoDB (static credentials)
#   ./add-dynamodb-source.sh \
#     --name dynamodb_prod \
#     --region us-east-1 \
#     --access-key AKIAIOSFODNN7EXAMPLE \
#     --secret-key wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
#
#   # Force recreate (delete existing source first)
#   ./add-dynamodb-source.sh --name dynamodb_local \
#     --endpoint http://dynamodb-local:8000 \
#     --access-key fakeKey --secret-key fakeSecret --force

set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

# ── Defaults ──────────────────────────────────────────────────────────────────
DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
REGION="us-east-1"
ENDPOINT=""
ACCESS_KEY=""
SECRET_KEY=""
SAMPLE_SIZE=100
PARALLELISM=4
TIMEOUT_SECONDS=30
PAGE_SIZE=1000
CACHE_TTL=60
FORCE=false
DRY_RUN=false

# ── Colours ──────────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*" >&2; exit 1; }
info() { echo -e "${CYAN}→${RESET} $*"; }

# ── Parse arguments ───────────────────────────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)        SOURCE_NAME="$2";  shift 2 ;;
    --dremio)      DREMIO_URL="$2";   shift 2 ;;
    --user)        DREMIO_USER="$2";  shift 2 ;;
    --password)    DREMIO_PASS="$2";  shift 2 ;;
    --region)      REGION="$2";       shift 2 ;;
    --endpoint)    ENDPOINT="$2";     shift 2 ;;
    --access-key)  ACCESS_KEY="$2";   shift 2 ;;
    --secret-key)  SECRET_KEY="$2";   shift 2 ;;
    --sample-size) SAMPLE_SIZE="$2";  shift 2 ;;
    --parallelism) PARALLELISM="$2";  shift 2 ;;
    --timeout)     TIMEOUT_SECONDS="$2"; shift 2 ;;
    --page-size)   PAGE_SIZE="$2";    shift 2 ;;
    --cache-ttl)   CACHE_TTL="$2";    shift 2 ;;
    --force)       FORCE=true;        shift ;;
    --dry-run)     DRY_RUN=true;      shift ;;
    -h|--help)
      grep "^#" "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) err "Unknown option: $1 — use --help for usage" ;;
  esac
done

[[ -n "$SOURCE_NAME" ]] || err "--name is required"

# ── Prompt for Dremio password if not provided ────────────────────────────────
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '${DREMIO_USER}': " DREMIO_PASS
  echo ""
fi

echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   Dremio Amazon DynamoDB Source Setup${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════${RESET}"
echo ""
info "Source name : ${SOURCE_NAME}"
info "Region      : ${REGION}"
[[ -n "$ENDPOINT" ]] && info "Endpoint    : ${ENDPOINT}"
[[ -n "$ACCESS_KEY" ]] && info "Access Key  : ${ACCESS_KEY:0:8}…"
[[ -z "$ACCESS_KEY" ]] && info "Credentials : default chain (IAM / instance profile)"

# ── Login to Dremio ───────────────────────────────────────────────────────────
info "Logging into Dremio at ${DREMIO_URL}…"
TOKEN=$(curl -s -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('token',''))" 2>/dev/null)

[[ -n "$TOKEN" ]] || err "Login failed. Check Dremio URL, username, and password."
ok "Login successful"
AUTH="Authorization: _dremio${TOKEN}"

# ── Handle --force: delete existing source ────────────────────────────────────
EXISTING=$(curl -s -H "$AUTH" "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" 2>/dev/null)
EXISTING_TAG=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tag',''))" 2>/dev/null || true)

if [[ -n "$EXISTING_TAG" ]]; then
  if $FORCE; then
    info "Deleting existing source '${SOURCE_NAME}' (tag: ${EXISTING_TAG})…"
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
      -H "$AUTH" "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}?version=${EXISTING_TAG}")
    [[ "$HTTP" == "204" || "$HTTP" == "200" ]] || err "Failed to delete source (HTTP ${HTTP})"
    ok "Existing source deleted"
    sleep 1
  else
    warn "Source '${SOURCE_NAME}' already exists (tag: ${EXISTING_TAG})."
    warn "Use --force to delete and recreate it."
    exit 0
  fi
fi

# ── Build JSON payload ────────────────────────────────────────────────────────
PAYLOAD=$(python3 - <<PYEOF
import json
config = {
    "region": "${REGION}",
    "endpointOverride": "${ENDPOINT}",
    "accessKeyId": "${ACCESS_KEY}",
    "secretAccessKey": "${SECRET_KEY}",
    "sampleSize": ${SAMPLE_SIZE},
    "splitParallelism": ${PARALLELISM},
    "readTimeoutSeconds": ${TIMEOUT_SECONDS},
    "maxPageSize": ${PAGE_SIZE},
    "metadataCacheTtlSeconds": ${CACHE_TTL},
}
payload = {
    "name": "${SOURCE_NAME}",
    "config": config,
    "type": "AMAZON_DYNAMODB",
    "metadataPolicy": {
        "updateMode": "PREFETCH_QUERIED",
        "namesRefreshMillis": 3600000,
        "authTTLMillis": 86400000,
        "datasetDefinitionRefreshAfterMillis": 3600000,
        "datasetDefinitionExpireAfterMillis": 10800000,
        "deleteUnavailableDatasets": True,
        "autoPromoteDatasets": False,
    },
}
print(json.dumps(payload))
PYEOF
)

if $DRY_RUN; then
  echo ""
  echo "=== DRY RUN — payload that would be submitted ==="
  echo "$PAYLOAD" | python3 -m json.tool
  exit 0
fi

# ── Create source ─────────────────────────────────────────────────────────────
info "Creating source '${SOURCE_NAME}'…"
RESP=$(curl -s -X PUT -H "$AUTH" -H "Content-Type: application/json" \
  "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" -d "$PAYLOAD")

ERR_MSG=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('errorMessage',''))" 2>/dev/null || true)
if [[ -n "$ERR_MSG" ]]; then
  err "Source creation failed: ${ERR_MSG}"
fi

STATE=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('state',{}).get('status','unknown'))" 2>/dev/null || true)
ok "Source '${SOURCE_NAME}' created (state: ${STATE})"

echo ""
echo -e "  Test with: ${BOLD}SELECT * FROM ${SOURCE_NAME}.<table_name> LIMIT 10${RESET}"
echo ""
info "Note: Tables appear in the catalog after the first names refresh."
info "If tables don't appear immediately, wait a moment or query a table directly."
