#!/usr/bin/env bash
# add-servicenow-source.sh — Add a new ServiceNow source to Dremio via REST API
#
# USAGE
#   ./add-servicenow-source.sh --name <source_name> --instance-url <instance_url> \
#     --username <username> --password <password> [options]
#
# OPTIONS
#   --name           NAME       Dremio source name to create (required)
#   --instance-url   URL        ServiceNow instance URL (required)
#   --username       USERNAME   ServiceNow integration user (required)
#   --password       PASSWORD   ServiceNow password (prompted interactively if omitted)
#   --dremio           URL        Dremio base URL (default: http://localhost:9047)
#   --user             USERNAME   Dremio username (default: dremio)
#   --dremio-password  PASSWORD   Dremio password (prompted interactively if omitted)
#   --page-size   N          Records per API page (default: 100)
#   --timeout     SECONDS    Query timeout (default: 120)
#   --force                  Delete and recreate if source already exists
#   --dry-run                Print JSON payload without submitting
#   -h, --help               Show this help
#
# EXAMPLES
#   ./add-servicenow-source.sh \
#     --name servicenow \
#     --instance-url https://dev12345.service-now.com \
#     --username admin \
#     --password secret

set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
INSTANCE_URL=""
USERNAME=""
PASSWORD=""
PAGE_SIZE=100
TIMEOUT=120
FORCE=false
DRY_RUN=false

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'
ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*" >&2; exit 1; }
info() { echo -e "${CYAN}→${RESET} $*"; }

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)          SOURCE_NAME="$2";  shift 2 ;;
    --instance-url)  INSTANCE_URL="$2"; shift 2 ;;
    --username)      USERNAME="$2";     shift 2 ;;
    --password)      PASSWORD="$2";     shift 2 ;;
    --dremio)        DREMIO_URL="$2";  shift 2 ;;
    --user)          DREMIO_USER="$2"; shift 2 ;;
    --dremio-password) DREMIO_PASS="$2"; shift 2 ;;
    --page-size)  PAGE_SIZE="$2";   shift 2 ;;
    --timeout)    TIMEOUT="$2";     shift 2 ;;
    --force)      FORCE=true;       shift ;;
    --dry-run)    DRY_RUN=true;     shift ;;
    -h|--help)
      grep "^#" "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) err "Unknown option: $1 — use --help for usage" ;;
  esac
done

[[ -n "$SOURCE_NAME" ]] || err "--name is required"
[[ -n "$INSTANCE_URL"   ]] || err "--instance-url is required"
[[ -n "$USERNAME"       ]] || err "--username is required"

if [[ -z "$PASSWORD" ]]; then
  read -rsp "ServiceNow password: " PASSWORD </dev/tty
  echo ""
fi

if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '${DREMIO_USER}': " DREMIO_PASS </dev/tty
  echo ""
fi

echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   Dremio ServiceNow Source Setup${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════${RESET}"
echo ""
info "Source name  : ${SOURCE_NAME}"
info "Instance URL : ${INSTANCE_URL}"
info "Username     : ${USERNAME}"
info "Page size    : ${PAGE_SIZE}"
info "Timeout      : ${TIMEOUT}s"

info "Logging into Dremio at ${DREMIO_URL}…"
TOKEN=$(curl -s -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)
[[ -n "$TOKEN" ]] || err "Login failed. Check Dremio URL, username, and password."
ok "Login successful"
AUTH="Authorization: _dremio${TOKEN}"

EXISTING=$(curl -s -H "$AUTH" "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" 2>/dev/null)
EXISTING_TAG=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tag',''))" 2>/dev/null || true)

if [[ -n "$EXISTING_TAG" ]]; then
  if $FORCE; then
    info "Deleting existing source '${SOURCE_NAME}'…"
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" -X DELETE \
      -H "$AUTH" "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}?version=${EXISTING_TAG}")
    [[ "$HTTP" == "204" || "$HTTP" == "200" ]] || err "Failed to delete source (HTTP ${HTTP})"
    ok "Existing source deleted"; sleep 1
  else
    warn "Source '${SOURCE_NAME}' already exists. Use --force to recreate it."
    exit 0
  fi
fi

PAYLOAD=$(python3 - <<PYEOF
import json
payload = {
    "name": "${SOURCE_NAME}",
    "type": "SERVICENOW_REST",
    "config": {
        "instanceUrl": "${INSTANCE_URL}",
        "username": "${USERNAME}",
        "password": "${PASSWORD}",
        "pageSize": ${PAGE_SIZE},
        "queryTimeoutSeconds": ${TIMEOUT},
    },
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
  echo ""; echo "=== DRY RUN — payload that would be submitted ==="
  echo "$PAYLOAD" | python3 -m json.tool
  exit 0
fi

info "Creating source '${SOURCE_NAME}'…"
RESP=$(curl -s -X PUT -H "$AUTH" -H "Content-Type: application/json" \
  "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" -d "$PAYLOAD")

ERR_MSG=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('errorMessage',''))" 2>/dev/null || true)
[[ -n "$ERR_MSG" ]] && err "Source creation failed: ${ERR_MSG}"

ok "Source '${SOURCE_NAME}' created"
echo ""
echo -e "  Tables: incident, task, sys_user, problem, change_request"
echo ""
