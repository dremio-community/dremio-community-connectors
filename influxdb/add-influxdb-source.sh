#!/usr/bin/env bash
# add-influxdb-source.sh — Add an InfluxDB 3 source to Dremio via REST API
#
# PURPOSE
#   Creates or updates an InfluxDB source in Dremio without going through the UI.
#   Authenticates to Dremio, then calls PUT /apiv2/source/{name} with the
#   InfluxDB connection details you supply.
#
# USAGE
#   ./add-influxdb-source.sh --name <source_name> --host <url> --database <db> --token <token>
#
# REQUIRED
#   --name        NAME      Dremio source name to create
#   --host        URL       InfluxDB 3 host URL (default: http://localhost:8181)
#   --database    DB        InfluxDB database name
#   --token       TOKEN     InfluxDB API token (prompted interactively if omitted)
#
# OPTIONAL
#   --dremio      URL       Dremio base URL (default: http://localhost:9047)
#   --user        USERNAME  Dremio username (default: dremio)
#   --password    PASSWORD  Dremio password (prompted interactively if omitted)
#   --page-size   N         Rows per page, default 1000
#   --timeout     SECONDS   Query timeout in seconds, default 120
#   --force                 Delete and recreate if source already exists
#   --dry-run               Print payload without submitting
#   -h, --help              Show this help
#
# EXAMPLES
#   # Local InfluxDB 3 Core (Docker)
#   ./add-influxdb-source.sh \
#     --name influxdb_sensors \
#     --host http://localhost:8181 \
#     --database sensors \
#     --token apiv3_xxx...
#
#   # Non-interactive (CI/CD)
#   ./add-influxdb-source.sh \
#     --name influxdb_prod \
#     --host https://influxdb.example.com:8181 \
#     --database metrics \
#     --token "$INFLUXDB_TOKEN" \
#     --user dremio_admin \
#     --password "$DREMIO_PASS"
#
#   # InfluxDB inside same Docker network as Dremio
#   ./add-influxdb-source.sh \
#     --name influxdb_sensors \
#     --host http://influxdb3:8181 \
#     --database sensors \
#     --token "$INFLUXDB_TOKEN"
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

DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
INFLUX_HOST="http://localhost:8181"
INFLUX_DATABASE=""
INFLUX_TOKEN=""
PAGE_SIZE=1000
TIMEOUT=120
FORCE=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)       SOURCE_NAME="$2";      shift 2 ;;
    --host)       INFLUX_HOST="$2";      shift 2 ;;
    --database)   INFLUX_DATABASE="$2";  shift 2 ;;
    --token)      INFLUX_TOKEN="$2";     shift 2 ;;
    --dremio)     DREMIO_URL="$2";       shift 2 ;;
    --user)       DREMIO_USER="$2";      shift 2 ;;
    --password)   DREMIO_PASS="$2";      shift 2 ;;
    --page-size)  PAGE_SIZE="$2";        shift 2 ;;
    --timeout)    TIMEOUT="$2";          shift 2 ;;
    --force)      FORCE=true;            shift ;;
    --dry-run)    DRY_RUN=true;          shift ;;
    -h|--help)
      grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) err "Unknown argument: $1 — use --help for usage" ;;
  esac
done

echo ""
echo -e "${CYAN}══════════════════════════════════════════${RESET}"
echo -e "${CYAN}   Add InfluxDB Source to Dremio${RESET}"
echo -e "${CYAN}══════════════════════════════════════════${RESET}"

[[ -z "$SOURCE_NAME"      ]] && err "--name is required"
[[ -z "$INFLUX_DATABASE"  ]] && err "--database is required"

if [[ -z "$INFLUX_TOKEN" ]]; then
  read -rsp "  InfluxDB API token: " INFLUX_TOKEN </dev/tty; echo ""
  [[ -z "$INFLUX_TOKEN" ]] && err "Token cannot be empty"
fi

if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "  Dremio password for '$DREMIO_USER': " DREMIO_PASS </dev/tty; echo ""
  [[ -z "$DREMIO_PASS" ]] && err "Dremio password cannot be empty"
fi

# ── Authenticate to Dremio ────────────────────────────────────────────────────
header "Authenticating to Dremio at $DREMIO_URL"

TOKEN_RESP=$(curl -sf -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" 2>/dev/null) \
  || err "Login failed — check Dremio URL, username, and password"

DREMIO_TOKEN=$(echo "$TOKEN_RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)
[[ -n "$DREMIO_TOKEN" ]] || err "Could not extract Dremio token from login response"
ok "Authenticated to Dremio"
AUTH="Authorization: _dremio${DREMIO_TOKEN}"

# ── Check for existing source ─────────────────────────────────────────────────
EXISTING=$(curl -sf -H "$AUTH" "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" 2>/dev/null || true)
EXISTING_TAG=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin).get('tag',''))" 2>/dev/null || true)

if [[ -n "$EXISTING_TAG" ]]; then
  if $FORCE; then
    info "Deleting existing source '${SOURCE_NAME}'..."
    HTTP=$(curl -sf -o /dev/null -w "%{http_code}" -X DELETE \
      -H "$AUTH" "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}?version=${EXISTING_TAG}" 2>/dev/null)
    [[ "$HTTP" == "204" || "$HTTP" == "200" ]] || err "Failed to delete source (HTTP ${HTTP})"
    ok "Existing source deleted"
    sleep 1
  else
    warn "Source '${SOURCE_NAME}' already exists. Use --force to recreate."
    exit 0
  fi
fi

# ── Build payload ─────────────────────────────────────────────────────────────
PAYLOAD=$(python3 - <<PYEOF
import json
p = {
    "name": "${SOURCE_NAME}",
    "type": "INFLUXDB",
    "config": {
        "host":                 "${INFLUX_HOST}",
        "database":             "${INFLUX_DATABASE}",
        "token":                "${INFLUX_TOKEN}",
        "pageSize":              ${PAGE_SIZE},
        "queryTimeoutSeconds":   ${TIMEOUT}
    }
}
print(json.dumps(p, indent=2))
PYEOF
)

if $DRY_RUN; then
  echo ""
  echo -e "${CYAN}Dry-run payload:${RESET}"
  echo "$PAYLOAD"
  exit 0
fi

# ── Create source ─────────────────────────────────────────────────────────────
header "Creating source '${SOURCE_NAME}'"
info "Host:     ${INFLUX_HOST}"
info "Database: ${INFLUX_DATABASE}"

RESP=$(curl -sf -X PUT \
  -H "$AUTH" -H "Content-Type: application/json" \
  "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" \
  -d "$PAYLOAD" 2>/dev/null) \
  || err "Source creation failed — check Dremio logs"

STATUS=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('state',{}).get('status','unknown'))" 2>/dev/null || echo "unknown")
ERR_MSG=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('errorMessage',''))" 2>/dev/null || true)

if [[ -n "$ERR_MSG" ]]; then
  err "Dremio returned error: $ERR_MSG"
fi

if [[ "$STATUS" == "good" ]]; then
  ok "Source '${SOURCE_NAME}' created — status: good"
else
  warn "Source created but status is '${STATUS}' — check the Dremio UI for details"
fi

echo ""
echo -e "${GREEN}Done.${RESET} Open Dremio and browse ${CYAN}${SOURCE_NAME}${RESET} to see your measurements."
echo ""
echo "  Quick verify:"
echo "    SELECT * FROM \"${SOURCE_NAME}\".your_measurement LIMIT 5;"
echo ""
