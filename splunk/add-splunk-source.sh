#!/usr/bin/env bash
# add-splunk-source.sh — Register a Splunk source in Dremio via REST API
#
# PURPOSE
#   Creates (or updates) a Splunk source in Dremio without using the UI.
#   After running, every visible Splunk index appears as a table under the source.
#
# USAGE
#   ./add-splunk-source.sh --name <source_name> --host <splunk_host> [options]
#
# CONNECTION OPTIONS
#   --name         NAME      Dremio source name to create (required)
#   --host         HOSTNAME  Splunk server hostname (required)
#   --port         PORT      Splunk management port (default: 8089; Cloud: 443)
#   --ssl                    Enable HTTPS (default: true)
#   --no-ssl                 Disable HTTPS
#   --cloud                  Splunk Cloud mode (forces port=443, SSL=true)
#   --dremio       URL       Dremio base URL (default: http://localhost:9047)
#   --user         USERNAME  Dremio username (default: dremio)
#   --password     PASSWORD  Dremio password (prompted if omitted)
#
# AUTHENTICATION (choose one)
#   --splunk-user  USERNAME  Splunk username (use with --splunk-pass)
#   --splunk-pass  PASSWORD  Splunk password
#   --token        TOKEN     Splunk bearer token (takes priority over user/pass)
#
# SCAN OPTIONS
#   --earliest     TIME      Default earliest time for unfiltered scans (default: -24h)
#   --max-events   N         Max events per query (default: 50000)
#   --sample       N         Events to sample per index for schema inference (default: 200)
#   --cache-ttl    SECONDS   Metadata cache TTL (default: 300)
#
# FILTERING
#   --exclude      REGEX     Regex for indexes to exclude (default: ^(_.*|history|fishbucket)$)
#   --include      REGEX     Regex for indexes to include (default: all)
#
# EXAMPLES
#   ./add-splunk-source.sh --name splunk --host splunk.example.com
#   ./add-splunk-source.sh --name splunk_cloud --host myco.splunkcloud.com --cloud --token eyJ...
#   ./add-splunk-source.sh --name splunk_dev --host localhost --no-ssl --splunk-user admin --splunk-pass changeme

set -euo pipefail

DREMIO_URL="http://localhost:9047"
DREMIO_USER="dremio"
DREMIO_PASS=""
SOURCE_NAME=""
SPLUNK_HOST=""
SPLUNK_PORT="8089"
USE_SSL="true"
SPLUNK_CLOUD="false"
SPLUNK_USER=""
SPLUNK_PASS=""
SPLUNK_TOKEN=""
DEFAULT_EARLIEST="-24h"
MAX_EVENTS="50000"
SAMPLE_EVENTS="200"
CACHE_TTL="300"
EXCLUDE_PATTERN="^(_.*|history|fishbucket|lastchanceindex|cim_modactions)\$"
INCLUDE_PATTERN=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)         SOURCE_NAME="$2";         shift 2 ;;
    --host)         SPLUNK_HOST="$2";         shift 2 ;;
    --port)         SPLUNK_PORT="$2";         shift 2 ;;
    --ssl)          USE_SSL="true";           shift ;;
    --no-ssl)       USE_SSL="false";          shift ;;
    --cloud)        SPLUNK_CLOUD="true"; USE_SSL="true"; SPLUNK_PORT="443"; shift ;;
    --dremio)       DREMIO_URL="$2";          shift 2 ;;
    --user)         DREMIO_USER="$2";         shift 2 ;;
    --password)     DREMIO_PASS="$2";         shift 2 ;;
    --splunk-user)  SPLUNK_USER="$2";         shift 2 ;;
    --splunk-pass)  SPLUNK_PASS="$2";         shift 2 ;;
    --token)        SPLUNK_TOKEN="$2";        shift 2 ;;
    --earliest)     DEFAULT_EARLIEST="$2";    shift 2 ;;
    --max-events)   MAX_EVENTS="$2";          shift 2 ;;
    --sample)       SAMPLE_EVENTS="$2";       shift 2 ;;
    --cache-ttl)    CACHE_TTL="$2";           shift 2 ;;
    --exclude)      EXCLUDE_PATTERN="$2";     shift 2 ;;
    --include)      INCLUDE_PATTERN="$2";     shift 2 ;;
    --help|-h)
      sed -n '4,40p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

# Validation
if [[ -z "$SOURCE_NAME" ]]; then
  echo "ERROR: --name is required"; exit 1
fi
if [[ -z "$SPLUNK_HOST" ]]; then
  echo "ERROR: --host is required"; exit 1
fi

# Prompt for missing passwords
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS; echo ""
fi
if [[ -z "$SPLUNK_TOKEN" && -z "$SPLUNK_USER" ]]; then
  echo "No Splunk credentials supplied."
  echo "  Enter a username for password auth, OR press Enter to use a token instead."
  read -rp "Splunk username (or blank for token): " SPLUNK_USER
  if [[ -n "$SPLUNK_USER" ]]; then
    read -rsp "Splunk password: " SPLUNK_PASS; echo ""
  else
    read -rsp "Splunk bearer token: " SPLUNK_TOKEN; echo ""
  fi
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
    "hostname":              "$SPLUNK_HOST",
    "port":                  int("$SPLUNK_PORT"),
    "useSsl":               "$USE_SSL" == "true",
    "splunkCloud":          "$SPLUNK_CLOUD" == "true",
    "disableSslVerification": False,
    "username":             "$SPLUNK_USER",
    "password":             "$SPLUNK_PASS",
    "authToken":            "$SPLUNK_TOKEN",
    "defaultEarliest":      "$DEFAULT_EARLIEST",
    "defaultMaxEvents":     int("$MAX_EVENTS"),
    "sampleEventsForSchema": int("$SAMPLE_EVENTS"),
    "metadataCacheTtlSeconds": int("$CACHE_TTL"),
    "indexExcludePattern":  "$EXCLUDE_PATTERN",
    "indexIncludePattern":  "$INCLUDE_PATTERN",
    "searchMode":           "normal",
    "resultsPageSize":      5000,
    "connectionTimeoutSeconds": 30,
    "readTimeoutSeconds":   300,
}

source = {
    "entityType": "source",
    "name": "$SOURCE_NAME",
    "type": "SPLUNK",
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
  HTTP_CODE=$(curl -s -o /tmp/splunk_source_resp.json -w "%{http_code}" \
    -X PUT "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")
else
  echo "Creating new source '$SOURCE_NAME'..."
  HTTP_CODE=$(curl -s -o /tmp/splunk_source_resp.json -w "%{http_code}" \
    -X POST "${DREMIO_URL}/apiv2/source/${SOURCE_NAME}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")
fi

if [[ "$HTTP_CODE" == "200" ]] || [[ "$HTTP_CODE" == "201" ]]; then
  echo ""
  echo "✓ Splunk source '$SOURCE_NAME' registered successfully"
  echo ""
  echo "  Query example:"
  echo "    SELECT * FROM \"${SOURCE_NAME}\".\"main\" WHERE _time >= NOW() - INTERVAL '1' HOUR LIMIT 100"
  echo ""
else
  echo "ERROR: HTTP $HTTP_CODE"
  cat /tmp/splunk_source_resp.json 2>/dev/null
  exit 1
fi
