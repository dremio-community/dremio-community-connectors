#!/usr/bin/env bash
# Add a Google Pub/Sub source to Dremio
#
# USAGE
#   ./add-pubsub-source.sh [OPTIONS]
#
# EXAMPLES
#   ./add-pubsub-source.sh                                   # emulator defaults
#   ./add-pubsub-source.sh --name prod_pubsub --project my-gcp-project
#   ./add-pubsub-source.sh --name pubsub_sa --project my-project \
#       --credentials /secrets/sa-key.json
set -euo pipefail

DREMIO_HOST="${DREMIO_HOST:-http://localhost:9047}"
DREMIO_USER="${DREMIO_USER:-mark}"
DREMIO_PASS="${DREMIO_PASS:-dremio}"

SOURCE_NAME="pubsub_test"
PROJECT_ID="test-project"
CREDENTIALS_FILE=""
EMULATOR_HOST="pubsub-emulator:8681"
SCHEMA_MODE="JSON"
SAMPLE_MESSAGES=20
MAX_MESSAGES=1000
PULL_TIMEOUT=10
INCLUDE_PATTERN=""
EXCLUDE_PATTERN="_dremio_"
CACHE_TTL=60

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --name NAME              Source name in Dremio (default: pubsub_test)
  --project PROJECT        GCP project ID (default: test-project)
  --credentials FILE       Path to SA JSON key file (default: blank = ADC)
  --emulator HOST:PORT     Emulator host:port (default: pubsub-emulator:8681)
                           Use '' to connect to production GCP
  --schema-mode MODE       JSON or RAW (default: JSON)
  --sample-messages N      Messages to sample for schema inference (default: 20)
  --max-messages N         Max messages per query (default: 1000)
  --pull-timeout N         Pull timeout in seconds (default: 10)
  --include REGEX          Include only matching subscriptions
  --exclude REGEX          Exclude matching subscriptions (default: _dremio_)
  --cache-ttl N            Schema cache TTL in seconds (default: 60)
  --dremio-host URL        Dremio base URL (default: http://localhost:9047)
  --user USER              Dremio username (default: mark)
  --password PASS          Dremio password
  -h, --help               Show this help
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)           SOURCE_NAME="$2";       shift 2 ;;
    --project)        PROJECT_ID="$2";         shift 2 ;;
    --credentials)    CREDENTIALS_FILE="$2";   shift 2 ;;
    --emulator)       EMULATOR_HOST="$2";      shift 2 ;;
    --schema-mode)    SCHEMA_MODE="$2";        shift 2 ;;
    --sample-messages) SAMPLE_MESSAGES="$2";  shift 2 ;;
    --max-messages)   MAX_MESSAGES="$2";       shift 2 ;;
    --pull-timeout)   PULL_TIMEOUT="$2";       shift 2 ;;
    --include)        INCLUDE_PATTERN="$2";    shift 2 ;;
    --exclude)        EXCLUDE_PATTERN="$2";    shift 2 ;;
    --cache-ttl)      CACHE_TTL="$2";          shift 2 ;;
    --dremio-host)    DREMIO_HOST="$2";        shift 2 ;;
    --user)           DREMIO_USER="$2";        shift 2 ;;
    --password)       DREMIO_PASS="$2";        shift 2 ;;
    -h|--help)        usage ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

echo "Authenticating with Dremio at ${DREMIO_HOST}..."
TOKEN=$(curl -sf -X POST "${DREMIO_HOST}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "Creating source '${SOURCE_NAME}'..."
BODY=$(python3 -c "
import json, sys
cfg = {
  'projectId':                  '${PROJECT_ID}',
  'credentialsFile':            '${CREDENTIALS_FILE}',
  'emulatorHost':               '${EMULATOR_HOST}',
  'schemaMode':                 '${SCHEMA_MODE}',
  'sampleMessagesForSchema':    ${SAMPLE_MESSAGES},
  'defaultMaxMessages':         ${MAX_MESSAGES},
  'pullTimeoutSeconds':         ${PULL_TIMEOUT},
  'subscriptionIncludePattern': '${INCLUDE_PATTERN}',
  'subscriptionExcludePattern': '${EXCLUDE_PATTERN}',
  'metadataCacheTtlSeconds':    ${CACHE_TTL},
}
payload = {'name': '${SOURCE_NAME}', 'type': 'GOOGLE_PUBSUB', 'config': cfg}
print(json.dumps(payload))
")

HTTP_CODE=$(curl -sf -o /dev/null -w "%{http_code}" \
  -X PUT "${DREMIO_HOST}/apiv2/source/${SOURCE_NAME}" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$BODY" || echo "000")

if [[ "$HTTP_CODE" == "200" ]]; then
  echo "Source '${SOURCE_NAME}' created successfully."
else
  echo "Failed (HTTP ${HTTP_CODE}). Trying again with response..."
  curl -s -X PUT "${DREMIO_HOST}/apiv2/source/${SOURCE_NAME}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$BODY"
  echo
  exit 1
fi

echo ""
echo "Try these queries:"
echo "  SELECT * FROM ${SOURCE_NAME}.\"orders-dremio\" LIMIT 10;"
echo "  SELECT order_id, customer, amount, status FROM ${SOURCE_NAME}.\"orders-dremio\" LIMIT 5;"
