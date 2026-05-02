#!/usr/bin/env bash
# Quick-add a Dataverse source to a running Dremio instance via REST API.
# Usage: ./add-dataverse-source.sh

set -euo pipefail

DREMIO_HOST="${DREMIO_HOST:-localhost}"
DREMIO_PORT="${DREMIO_PORT:-9047}"
DREMIO_USER="${DREMIO_USER:-admin}"
DREMIO_PASS="${DREMIO_PASS:-admin}"

SOURCE_NAME="${SOURCE_NAME:-dataverse}"
ORG_URL="${ORG_URL:-https://yourorg.api.crm.dynamics.com}"
TENANT_ID="${TENANT_ID:-your-tenant-id}"
CLIENT_ID="${CLIENT_ID:-your-client-id}"
CLIENT_SECRET="${CLIENT_SECRET:-your-client-secret}"

BASE="http://$DREMIO_HOST:$DREMIO_PORT"

echo "Logging in to Dremio at $BASE ..."
TOKEN=$(curl -sf -X POST "$BASE/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "Creating Dataverse source '$SOURCE_NAME' ..."
curl -sf -X PUT "$BASE/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio$TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$SOURCE_NAME\",
    \"type\": \"DATAVERSE\",
    \"config\": {
      \"organizationUrl\": \"$ORG_URL\",
      \"tenantId\": \"$TENANT_ID\",
      \"clientId\": \"$CLIENT_ID\",
      \"clientSecret\": \"$CLIENT_SECRET\",
      \"apiVersion\": \"9.2\",
      \"recordsPerPage\": 5000,
      \"queryTimeoutSeconds\": 120,
      \"excludedEntities\": \"\"
    }
  }" | python3 -m json.tool

echo ""
echo "Done. Source '$SOURCE_NAME' added."
