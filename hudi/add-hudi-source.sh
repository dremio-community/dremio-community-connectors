#!/usr/bin/env bash
# add-hudi-source.sh — Add a new Apache Hudi source to Dremio via REST API
#
# USAGE
#   ./add-hudi-source.sh --name <source_name> --root-path <path> [options]
#
# OPTIONS
#   --name              NAME      Dremio source name to create (required)
#   --root-path         PATH      Root path containing Hudi tables, e.g. s3://bucket/hudi (required)
#   --dremio            URL       Dremio base URL (default: http://localhost:9047)
#   --user              USERNAME  Dremio username (default: dremio)
#   --password          PASSWORD  Dremio password (prompted interactively if omitted)
#   --table-type        TYPE      Default table type: COPY_ON_WRITE or MERGE_ON_READ (default: COPY_ON_WRITE)
#   --record-key        FIELD     Default record key field (default: id)
#   --partition-field   FIELD     Default partition path field (default: blank = non-partitioned)
#   --precombine-field  FIELD     Default precombine field (default: ts)
#   --write-parallelism N         Write parallelism buckets (default: 4)
#   --force                       Delete and recreate if source already exists
#   --dry-run                     Print JSON payload without submitting
#   -h, --help                    Show this help
#
# EXAMPLES
#   # S3-backed Hudi tables
#   ./add-hudi-source.sh \
#     --name hudi \
#     --root-path s3://my-bucket/hudi-tables
#
#   # Local filesystem (development)
#   ./add-hudi-source.sh \
#     --name hudi_local \
#     --root-path /mnt/datalake/hudi \
#     --table-type MERGE_ON_READ \
#     --record-key uuid \
#     --partition-field created_date

set -euo pipefail
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
ROOT_PATH=""
TABLE_TYPE="COPY_ON_WRITE"
RECORD_KEY="id"
PARTITION_FIELD=""
PRECOMBINE_FIELD="ts"
WRITE_PARALLELISM=4
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
    --name)             SOURCE_NAME="$2";      shift 2 ;;
    --root-path)        ROOT_PATH="$2";        shift 2 ;;
    --dremio)           DREMIO_URL="$2";       shift 2 ;;
    --user)             DREMIO_USER="$2";      shift 2 ;;
    --password)         DREMIO_PASS="$2";      shift 2 ;;
    --table-type)       TABLE_TYPE="$2";       shift 2 ;;
    --record-key)       RECORD_KEY="$2";       shift 2 ;;
    --partition-field)  PARTITION_FIELD="$2";  shift 2 ;;
    --precombine-field) PRECOMBINE_FIELD="$2"; shift 2 ;;
    --write-parallelism) WRITE_PARALLELISM="$2"; shift 2 ;;
    --force)            FORCE=true;            shift ;;
    --dry-run)          DRY_RUN=true;          shift ;;
    -h|--help)
      grep "^#" "$0" | grep -v '^#!/' | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) err "Unknown option: $1 — use --help for usage" ;;
  esac
done

[[ -n "$SOURCE_NAME" ]] || err "--name is required"
[[ -n "$ROOT_PATH"   ]] || err "--root-path is required"

if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '${DREMIO_USER}': " DREMIO_PASS </dev/tty
  echo ""
fi

echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   Dremio Apache Hudi Source Setup${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════${RESET}"
echo ""
info "Source name  : ${SOURCE_NAME}"
info "Root path    : ${ROOT_PATH}"
info "Table type   : ${TABLE_TYPE}"
info "Record key   : ${RECORD_KEY}"
[[ -n "$PARTITION_FIELD" ]] && info "Partition    : ${PARTITION_FIELD}" || info "Partition    : none (non-partitioned)"

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
    "type": "HUDI",
    "config": {
        "rootPath": "${ROOT_PATH}",
        "defaultTableType": "${TABLE_TYPE}",
        "defaultRecordKeyField": "${RECORD_KEY}",
        "defaultPartitionPathField": "${PARTITION_FIELD}",
        "defaultPrecombineField": "${PRECOMBINE_FIELD}",
        "writeParallelism": ${WRITE_PARALLELISM},
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
echo -e "  Hudi tables under ${ROOT_PATH} will appear in the Dremio catalog."
echo -e "  Test with: ${BOLD}SHOW TABLES IN ${SOURCE_NAME}${RESET}"
echo ""
