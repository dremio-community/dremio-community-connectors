#!/usr/bin/env bash
# Creates a Zendesk source in Dremio via REST API.
# Usage: ./add-zendesk-source.sh
set -euo pipefail

DREMIO_HOST="${DREMIO_HOST:-localhost}"
DREMIO_PORT="${DREMIO_PORT:-9047}"
DREMIO_USER="${DREMIO_USER:-mark}"
DREMIO_PASS="${DREMIO_PASS:-critter77}"

ZENDESK_SUBDOMAIN="${ZENDESK_SUBDOMAIN:-}"
ZENDESK_EMAIL="${ZENDESK_EMAIL:-}"
ZENDESK_API_TOKEN="${ZENDESK_API_TOKEN:-}"
SOURCE_NAME="${SOURCE_NAME:-zendesk}"

if [[ -z "$ZENDESK_SUBDOMAIN" || -z "$ZENDESK_EMAIL" || -z "$ZENDESK_API_TOKEN" ]]; then
  echo "ERROR: Set ZENDESK_SUBDOMAIN, ZENDESK_EMAIL, and ZENDESK_API_TOKEN environment variables."
  exit 1
fi

BASE="http://${DREMIO_HOST}:${DREMIO_PORT}"

echo "Authenticating to Dremio..."
TOKEN=$(curl -sf -X POST "${BASE}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "Creating Zendesk source '${SOURCE_NAME}'..."
curl -sf -X PUT "${BASE}/apiv2/source/${SOURCE_NAME}" \
  -H "Content-Type: application/json" \
  -H "Authorization: _dremio${TOKEN}" \
  -d "{
    \"name\": \"${SOURCE_NAME}\",
    \"type\": \"ZENDESK_REST\",
    \"config\": {
      \"subdomain\": \"${ZENDESK_SUBDOMAIN}\",
      \"email\": \"${ZENDESK_EMAIL}\",
      \"apiToken\": \"${ZENDESK_API_TOKEN}\",
      \"pageSize\": 100,
      \"queryTimeoutSeconds\": 120
    }
  }" | python3 -m json.tool

echo ""
echo "✅ Zendesk source '${SOURCE_NAME}' created."
echo "Tables available: tickets, users, organizations, groups, ticket_metrics, satisfaction_ratings"
