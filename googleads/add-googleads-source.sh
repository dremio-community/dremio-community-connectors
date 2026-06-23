#!/usr/bin/env bash
# Register a Google Ads source in Dremio
#
# USAGE
#   ./add-googleads-source.sh [OPTIONS]
#
# EXAMPLES
#   ./add-googleads-source.sh \
#     --name google_ads \
#     --developer-token YOUR_DEV_TOKEN \
#     --client-id YOUR_CLIENT_ID \
#     --client-secret YOUR_CLIENT_SECRET \
#     --refresh-token YOUR_REFRESH_TOKEN \
#     --customer-id 1234567890
set -euo pipefail

DREMIO_HOST="${DREMIO_HOST:-http://localhost:9047}"
DREMIO_USER="${DREMIO_USER:-mark}"
DREMIO_PASS="${DREMIO_PASS:-dremio}"

SOURCE_NAME="google_ads"
DEVELOPER_TOKEN=""
CLIENT_ID=""
CLIENT_SECRET=""
REFRESH_TOKEN=""
CUSTOMER_ID=""
LOGIN_CUSTOMER_ID=""
DATE_RANGE_DAYS=30

usage() {
  cat <<EOF
Usage: $0 [OPTIONS]

Options:
  --name NAME                  Source name in Dremio (default: google_ads)
  --developer-token TOKEN      Google Ads developer token (required)
  --client-id ID               OAuth2 client ID (required)
  --client-secret SECRET       OAuth2 client secret (required)
  --refresh-token TOKEN        OAuth2 refresh token (required)
  --customer-id ID             Google Ads customer ID, no dashes (required)
  --login-customer-id ID       Manager/MCC account ID (optional)
  --date-range-days N          Days back for performance reports (default: 30)
  --dremio-host URL            Dremio base URL (default: http://localhost:9047)
  --user USER                  Dremio username (default: mark)
  --password PASS              Dremio password
  -h, --help                   Show this help
EOF
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --name)              SOURCE_NAME="$2";       shift 2 ;;
    --developer-token)   DEVELOPER_TOKEN="$2";   shift 2 ;;
    --client-id)         CLIENT_ID="$2";          shift 2 ;;
    --client-secret)     CLIENT_SECRET="$2";      shift 2 ;;
    --refresh-token)     REFRESH_TOKEN="$2";      shift 2 ;;
    --customer-id)       CUSTOMER_ID="$2";        shift 2 ;;
    --login-customer-id) LOGIN_CUSTOMER_ID="$2";  shift 2 ;;
    --date-range-days)   DATE_RANGE_DAYS="$2";    shift 2 ;;
    --dremio-host)       DREMIO_HOST="$2";        shift 2 ;;
    --user)              DREMIO_USER="$2";        shift 2 ;;
    --password)          DREMIO_PASS="$2";        shift 2 ;;
    -h|--help)           usage ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

[[ -z "$DEVELOPER_TOKEN" || -z "$CLIENT_ID" || -z "$CLIENT_SECRET" || \
   -z "$REFRESH_TOKEN"   || -z "$CUSTOMER_ID" ]] && {
  echo "Error: --developer-token, --client-id, --client-secret, --refresh-token, and --customer-id are required"
  echo "Run $0 --help for usage"
  exit 1
}

echo "Authenticating with Dremio at ${DREMIO_HOST}..."
TOKEN=$(curl -sf -X POST "${DREMIO_HOST}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${DREMIO_USER}\",\"password\":\"${DREMIO_PASS}\"}" \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

echo "Creating source '${SOURCE_NAME}'..."
BODY=$(python3 -c "
import json
cfg = {
  'developerToken':   '${DEVELOPER_TOKEN}',
  'clientId':         '${CLIENT_ID}',
  'clientSecret':     '${CLIENT_SECRET}',
  'refreshToken':     '${REFRESH_TOKEN}',
  'customerId':       '${CUSTOMER_ID}',
  'loginCustomerId':  '${LOGIN_CUSTOMER_ID}',
  'dateRangeDays':    ${DATE_RANGE_DAYS},
}
payload = {'name': '${SOURCE_NAME}', 'type': 'GOOGLE_ADS', 'config': cfg}
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
  echo "Failed (HTTP ${HTTP_CODE}). Response:"
  curl -s -X PUT "${DREMIO_HOST}/apiv2/source/${SOURCE_NAME}" \
    -H "Authorization: _dremio${TOKEN}" \
    -H "Content-Type: application/json" \
    -d "$BODY"
  echo
  exit 1
fi

echo ""
echo "Try these queries in Dremio:"
echo "  SELECT * FROM ${SOURCE_NAME}.campaigns LIMIT 20;"
echo "  SELECT * FROM ${SOURCE_NAME}.campaign_performance LIMIT 50;"
echo "  SELECT campaign_name, SUM(clicks) AS total_clicks, SUM(cost_micros)/1000000.0 AS spend"
echo "    FROM ${SOURCE_NAME}.campaign_performance GROUP BY campaign_name ORDER BY spend DESC;"
