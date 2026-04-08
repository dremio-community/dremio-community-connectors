#!/usr/bin/env bash
# add-pinot-source.sh — Register an Apache Pinot source in Dremio via REST API
#
# PURPOSE
#   Creates (or updates) a Pinot source in Dremio without using the UI.
#   After running, every Pinot table appears as a queryable table under the source.
#
# USAGE
#   ./add-pinot-source.sh --name <source_name> --host <pinot_controller_host> [options]
#
# CONNECTION OPTIONS
#   --name         NAME      Dremio source name to create (required)
#   --host         HOSTNAME  Pinot Controller hostname or IP (required)
#   --port         PORT      Pinot Controller REST API port (default: 9000)
#   --ssl                    Enable TLS/HTTPS (uses jdbc:pinot+ssl://)
#   --broker-list  LIST      Comma-separated broker addresses host:port,... (optional)
#   --dremio       URL       Dremio base URL (default: http://localhost:9047)
#   --user         USERNAME  Dremio username (default: dremio)
#   --password     PASSWORD  Dremio password (prompted if omitted)
#
# AUTHENTICATION
#   --pinot-user   USERNAME  Pinot username for basic auth (optional)
#   --pinot-pass   PASSWORD  Pinot password for basic auth (optional)
#
# PERFORMANCE OPTIONS
#   --fetch-size   N         Rows fetched per page (default: 500)
#   --max-idle     N         Max idle JDBC connections (default: 8)
#   --idle-time    SECONDS   Idle connection timeout in seconds (default: 60)
#
# EXAMPLES
#   ./add-pinot-source.sh --name pinot --host localhost
#   ./add-pinot-source.sh --name pinot --host pinot.example.com --port 9000 --ssl
#   ./add-pinot-source.sh --name pinot --host pinot.example.com --pinot-user admin --pinot-pass secret
#   ./add-pinot-source.sh --name pinot --host pinot.example.com --broker-list broker1:8099,broker2:8099

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
PINOT_HOST=""
PINOT_PORT="9000"
USE_TLS="false"
BROKER_LIST=""
PINOT_USER=""
PINOT_PASS=""
FETCH_SIZE="500"
MAX_IDLE="8"
IDLE_TIME="60"

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)         SOURCE_NAME="$2";   shift 2 ;;
    --host)         PINOT_HOST="$2";    shift 2 ;;
    --port)         PINOT_PORT="$2";    shift 2 ;;
    --ssl)          USE_TLS="true";     shift ;;
    --broker-list)  BROKER_LIST="$2";   shift 2 ;;
    --dremio)       DREMIO_URL="$2";    shift 2 ;;
    --user)         DREMIO_USER="$2";   shift 2 ;;
    --password)     DREMIO_PASS="$2";   shift 2 ;;
    --pinot-user)   PINOT_USER="$2";    shift 2 ;;
    --pinot-pass)   PINOT_PASS="$2";    shift 2 ;;
    --fetch-size)   FETCH_SIZE="$2";    shift 2 ;;
    --max-idle)     MAX_IDLE="$2";      shift 2 ;;
    --idle-time)    IDLE_TIME="$2";     shift 2 ;;
    --help|-h)
      sed -n '4,33p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Validation
if [[ -z "$SOURCE_NAME" ]]; then
  echo "ERROR: --name is required"; exit 1
fi
if [[ -z "$PINOT_HOST" ]]; then
  echo "ERROR: --host is required"; exit 1
fi

# Prompt for missing Dremio password
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS; echo ""
fi

# Get Dremio auth token
TOKEN=$(curl -sf -X POST "${DREMIO_URL}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

if [[ -z "$TOKEN" ]]; then
  echo "ERROR: Failed to authenticate with Dremio at $DREMIO_URL"
  exit 1
fi

echo "Authenticated with Dremio OK"

# Build source JSON payload
PAYLOAD=$(python3 - <<PYEOF
import json

config = {
    "host":        "$PINOT_HOST",
    "port":        int("$PINOT_PORT"),
    "useTls":      "$USE_TLS" == "true",
    "username":    "$PINOT_USER",
    "password":    "$PINOT_PASS",
    "brokerList":  "$BROKER_LIST",
    "fetchSize":   int("$FETCH_SIZE"),
    "maxIdleConns": int("$MAX_IDLE"),
    "idleTimeSec": int("$IDLE_TIME"),
}

source = {
    "entityType": "source",
    "name": "$SOURCE_NAME",
    "type": "PINOT",
    "config": config,
    "metadataPolicy": {
        "authTTLMs": 86400000,
        "namesRefreshMs": 3600000,
        "datasetRefreshAfterMs": 1800000,
        "datasetExpireAfterMs": 10800000,
        "datasetUpdateMode": "PREFETCH_QUERIED",
        "deleteUnavailableDatasets": True,
        "autoPromoteDatasets": False,
    }
}
print(json.dumps(source, indent=2))
PYEOF
)

# Check if source already exists
EXISTING=$(curl -s "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" \
  -H "Authorization: _dremio${TOKEN}" 2>/dev/null || echo "")

if echo "$EXISTING" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null | grep -q "$SOURCE_NAME"; then
  echo "Source '$SOURCE_NAME' already exists — updating..."
  SOURCE_ID=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")
  SOURCE_TAG=$(echo "$EXISTING" | python3 -c "import sys,json; print(json.load(sys.stdin)['tag'])")
  PAYLOAD=$(echo "$PAYLOAD" | python3 -c "
import sys,json
d=json.load(sys.stdin)
d['id']='${SOURCE_ID}'
d['tag']='${SOURCE_TAG}'
print(json.dumps(d))
")
  HTTP_CODE=$(curl -s -o /tmp/pinot_source_resp.json -w "%{http_code}" \
    -X PUT "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")
else
  echo "Creating new source '$SOURCE_NAME'..."
  HTTP_CODE=$(curl -s -o /tmp/pinot_source_resp.json -w "%{http_code}" \
    -X POST "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")
fi

if [[ "$HTTP_CODE" == "200" ]] || [[ "$HTTP_CODE" == "201" ]]; then
  echo ""
  echo "✓ Pinot source '$SOURCE_NAME' registered successfully"
  echo ""
  echo "  Query examples:"
  echo "    SELECT * FROM \"${SOURCE_NAME}\".\"airlineStats\" LIMIT 10"
  echo "    SELECT COUNT(*) FROM \"${SOURCE_NAME}\".\"<your_table>\""
  echo "    SELECT \"Carrier\", COUNT(*) cnt FROM \"${SOURCE_NAME}\".\"airlineStats\" GROUP BY \"Carrier\" ORDER BY cnt DESC LIMIT 10"
  echo ""
else
  echo "ERROR: HTTP $HTTP_CODE"
  cat /tmp/pinot_source_resp.json 2>/dev/null
  exit 1
fi
