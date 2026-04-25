#!/usr/bin/env bash
set -euo pipefail

DREMIO_HOST="${DREMIO_HOST:-http://localhost:9047}"
DREMIO_USER="${DREMIO_USER:-mark}"
DREMIO_PASS="${DREMIO_PASS:-critter77}"

COSMOS_ENDPOINT="${COSMOS_ENDPOINT:-http://localhost:8081}"
COSMOS_DATABASE="${COSMOS_DATABASE:-testdb}"
COSMOS_KEY="${COSMOS_KEY:-}"
SOURCE_NAME="${SOURCE_NAME:-cosmosdb}"

echo "=== Authenticating with Dremio ==="
TOKEN=$(curl -sf -X POST "$DREMIO_HOST/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "=== Adding Cosmos DB source '$SOURCE_NAME' ==="
curl -sf -X PUT "$DREMIO_HOST/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio$TOKEN" \
  -H "Content-Type: application/json" \
  -d "{
    \"name\": \"$SOURCE_NAME\",
    \"type\": \"COSMOS_DB\",
    \"config\": {
      \"endpoint\": \"$COSMOS_ENDPOINT\",
      \"database\": \"$COSMOS_DATABASE\",
      \"masterKey\": \"$COSMOS_KEY\",
      \"pageSize\": 100,
      \"schemaSampleSize\": 50,
      \"queryTimeoutSeconds\": 120
    },
    \"metadataPolicy\": {
      \"authTTLMs\": 86400000,
      \"namesRefreshMs\": 3600000,
      \"datasetRefreshAfterMs\": 3600000,
      \"datasetExpireAfterMs\": 10800000,
      \"datasetUpdateMode\": \"PREFETCH_QUERIED\",
      \"deleteUnavailableDatasets\": true,
      \"autoPromoteDatasets\": true
    }
  }" | python3 -m json.tool

echo ""
echo "Source '$SOURCE_NAME' added. Run: SELECT * FROM $SOURCE_NAME.tickets LIMIT 10"
