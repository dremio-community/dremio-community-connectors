#!/usr/bin/env bash
# add-clickhouse-source.sh — Register a ClickHouse source in Dremio via REST API
#
# PURPOSE
#   Creates (or recreates) a ClickHouse source in Dremio without touching the UI.
#   After running, every ClickHouse database and table is immediately browsable
#   and queryable from Dremio.
#
# WHAT IT DOES
#   Calls PUT /apiv2/source/{name} to create or update the source with the
#   configuration you supply. On success, Dremio connects and discovers all
#   non-excluded ClickHouse databases.
#
# USAGE
#   ./add-clickhouse-source.sh --name <source_name> --host <host> [options]
#
# CONNECTION OPTIONS
#   --name         NAME          Dremio source name to create (required)
#   --host         HOST          ClickHouse host or IP (required)
#   --port         PORT          HTTP port (default: 8123; use 8443 for SSL / Cloud)
#   --database     DB            Default database (default: default)
#   --ch-user      USERNAME      ClickHouse username (default: default)
#   --ch-pass      PASSWORD      ClickHouse password (prompted interactively if omitted)
#   --dremio       URL           Dremio base URL (default: http://localhost:9047)
#   --user         USERNAME      Dremio username (default: dremio)
#   --password     PASSWORD      Dremio password (prompted interactively if omitted)
#
# SSL / TLS
#   --ssl                        Enable SSL/TLS (sets useSsl = true)
#   --ssl-truststore PATH        Path to CA certificate or JKS/PKCS12 truststore
#   --ssl-ts-pass  PASSWORD      Truststore password (if using JKS/PKCS12)
#
# CLICKHOUSE CLOUD
#   --cloud                      ClickHouse Cloud mode — forces port 8443 and SSL.
#                                Use when connecting to cloud.clickhouse.com.
#                                Overrides --port and --ssl.
#
# PERFORMANCE
#   --max-idle     N             Max idle connections in pool (default: 8, max: 100)
#   --connect-timeout N          Connection timeout in seconds (default: 30, max: 600)
#   --socket-timeout  N          Socket / query timeout in seconds (default: 300, max: 3600)
#   --no-compression             Disable HTTP LZ4 result compression (default: enabled)
#   --fetch-block  N             Rows per response block from ClickHouse (default: 65536)
#
# CATALOG FILTERING
#   --exclude-dbs  LIST          Comma-separated DB names to hide from Dremio catalog
#                                (system, information_schema, and
#                                _temporary_and_external_tables are always hidden)
#                                Example: "staging,raw_ingest,tmp"
#
# ADVANCED
#   --jdbc-props   TEXT          Additional JDBC driver properties (key=value, one per
#                                line or semicolon-separated). Applied after all built-in
#                                properties. Example: "session_timezone=UTC;max_threads=4"
#   --force                      Delete and recreate if source already exists
#   --dry-run                    Print the JSON payload without submitting
#   -h, --help                   Show this help
#
# EXAMPLES
#   # Local ClickHouse, default user, no auth
#   ./add-clickhouse-source.sh --name ch_local --host localhost
#
#   # Named database, custom credentials
#   ./add-clickhouse-source.sh \
#     --name analytics \
#     --host clickhouse.internal \
#     --database analytics_db \
#     --ch-user analyst \
#     --ch-pass secret
#
#   # SSL with CA certificate
#   ./add-clickhouse-source.sh \
#     --name ch_prod \
#     --host clickhouse.prod.internal \
#     --ch-user dremio \
#     --ch-pass secret \
#     --ssl \
#     --ssl-truststore /etc/ssl/ca.crt
#
#   # ClickHouse Cloud
#   ./add-clickhouse-source.sh \
#     --name clickhouse_cloud \
#     --host abc123.us-east-1.aws.clickhouse.cloud \
#     --ch-user default \
#     --ch-pass MyCloudPassword \
#     --cloud
#
#   # Performance tuning + hide staging databases
#   ./add-clickhouse-source.sh \
#     --name ch_warehouse \
#     --host dwh.internal \
#     --socket-timeout 600 \
#     --fetch-block 131072 \
#     --exclude-dbs "staging,raw_ingest,tmp"
#
#   # Additional JDBC properties (session timezone, memory limit)
#   ./add-clickhouse-source.sh \
#     --name ch_tuned \
#     --host clickhouse.internal \
#     --jdbc-props "session_timezone=UTC;max_memory_usage=10000000000"
#
#   # Force recreate (e.g. after host change)
#   ./add-clickhouse-source.sh --name ch_local --host new-host.internal --force
#
#   # Non-interactive (CI/CD)
#   ./add-clickhouse-source.sh \
#     --name ci_clickhouse \
#     --host clickhouse:8123 \
#     --ch-user default \
#     --ch-pass "" \
#     --user dremio_admin \
#     --password dremio_pass
#
#   # Dry-run — print payload without submitting
#   ./add-clickhouse-source.sh --name ch_local --host localhost --dry-run
#
# EXIT CODES
#   0  Source created successfully
#   1  Authentication or connection failure
#   2  Source creation failed

set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

# ── Colour helpers ──────────────────────────────────────────────────────────────
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; CYAN='\033[0;36m'; RESET='\033[0m'
ok()     { echo -e "  ${GREEN}✓${RESET} $*"; }
warn()   { echo -e "  ${YELLOW}⚠${RESET} $*"; }
err()    { echo -e "${RED}✗ ERROR:${RESET} $*" >&2; exit 1; }
info()   { echo -e "  ${CYAN}→${RESET} $*"; }
header() { echo -e "\n${CYAN}$*${RESET}"; }

# ── Defaults ────────────────────────────────────────────────────────────────────
DREMIO_URL="http://localhost:9047"
SOURCE_NAME=""
DREMIO_USER="dremio"
DREMIO_PASS=""
# ClickHouse connection (Tags 1-5)
CH_HOST=""
CH_PORT=8123
CH_DATABASE="default"
CH_USER="default"
CH_PASS=""
# SSL / TLS (Tags 6-8)
USE_SSL=false
SSL_TRUSTSTORE_PATH=""
SSL_TRUSTSTORE_PASS=""
# Connection pool / timeouts (Tags 9-11)
MAX_IDLE=8
CONNECT_TIMEOUT=30
SOCKET_TIMEOUT=300
# Performance (Tags 12-13)
ENABLE_COMPRESSION=true
FETCH_BLOCK=65536
# Catalog filtering (Tag 14)
EXCLUDE_DBS=""
# ClickHouse Cloud (Tag 15)
CH_CLOUD=false
# Additional JDBC properties (Tag 16)
JDBC_PROPS=""
# Script control
DRY_RUN=false
FORCE=false

# ── Argument parsing ────────────────────────────────────────────────────────────
usage() { grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)            SOURCE_NAME="$2";       shift 2 ;;
    --host)            CH_HOST="$2";           shift 2 ;;
    --port)            CH_PORT="$2";           shift 2 ;;
    --database)        CH_DATABASE="$2";       shift 2 ;;
    --ch-user)         CH_USER="$2";           shift 2 ;;
    --ch-pass)         CH_PASS="$2";           shift 2 ;;
    --dremio)          DREMIO_URL="${2%/}";    shift 2 ;;
    --user)            DREMIO_USER="$2";       shift 2 ;;
    --password)        DREMIO_PASS="$2";       shift 2 ;;
    --ssl)             USE_SSL=true;           shift ;;
    --ssl-truststore)  SSL_TRUSTSTORE_PATH="$2"; shift 2 ;;
    --ssl-ts-pass)     SSL_TRUSTSTORE_PASS="$2"; shift 2 ;;
    --cloud)           CH_CLOUD=true;          shift ;;
    --max-idle)        MAX_IDLE="$2";          shift 2 ;;
    --connect-timeout) CONNECT_TIMEOUT="$2";   shift 2 ;;
    --socket-timeout)  SOCKET_TIMEOUT="$2";    shift 2 ;;
    --no-compression)  ENABLE_COMPRESSION=false; shift ;;
    --fetch-block)     FETCH_BLOCK="$2";       shift 2 ;;
    --exclude-dbs)     EXCLUDE_DBS="$2";       shift 2 ;;
    --jdbc-props)      JDBC_PROPS="$2";        shift 2 ;;
    --force)           FORCE=true;             shift ;;
    --dry-run)         DRY_RUN=true;           shift ;;
    -h|--help)         usage; exit 0 ;;
    *) err "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ -z "$SOURCE_NAME" ]] && { usage; err "--name is required"; }
[[ -z "$CH_HOST"     ]] && { usage; err "--host is required"; }

# ClickHouse Cloud implies SSL and port 8443
if [[ "$CH_CLOUD" == "true" ]]; then
  USE_SSL=true
  CH_PORT=8443
fi

# ── Password prompts ────────────────────────────────────────────────────────────
if [[ -z "$CH_PASS" ]] && [[ -z "$(echo "$@" | grep -o '\-\-ch-pass')" || true ]]; then
  # Only prompt if --ch-pass was never given at all (empty string is valid for default user)
  read -rsp "ClickHouse password for user '$CH_USER' (Enter for none): " CH_PASS </dev/tty
  echo >&2
fi

if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS </dev/tty
  echo >&2
fi

# ── Authenticate with Dremio ────────────────────────────────────────────────────
header "Connecting to Dremio at $DREMIO_URL"

AUTH_JSON=$(curl -sf -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" 2>/dev/null) \
  || err "Could not reach Dremio at $DREMIO_URL — is it running?"

TOKEN=$(echo "$AUTH_JSON" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)
[[ -z "$TOKEN" ]] && err "Authentication failed — check Dremio username/password"
ok "Authenticated as '$DREMIO_USER'"

# ── Check if source already exists ─────────────────────────────────────────────
header "Checking source '$SOURCE_NAME'"

EXISTING=$(curl -sf \
  -H "Authorization: _dremio${TOKEN}" \
  "$DREMIO_URL/apiv2/source/$SOURCE_NAME" 2>/dev/null) || true

EXISTING_ID=""
if [[ -n "$EXISTING" ]]; then
  EXISTING_ID=$(echo "$EXISTING" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('id',''))
except:
    print('')
" 2>/dev/null)
fi

if [[ -n "$EXISTING_ID" ]]; then
  if [[ "$FORCE" == "false" ]]; then
    echo ""
    ok "Source '$SOURCE_NAME' already exists (id: ${EXISTING_ID:0:8}…)"
    info "To update settings, edit it in the Dremio UI or re-run with --force."
    echo ""
    echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is already configured and ready."
    exit 0
  fi

  warn "Source '$SOURCE_NAME' exists — --force specified, deleting and recreating..."

  CATALOG_RESP=$(curl -sf \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/api/v3/catalog/${EXISTING_ID}" 2>/dev/null) || true

  VERSION_TAG=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('tag',''))
except:
    print('')
" 2>/dev/null)

  [[ -z "$VERSION_TAG" ]] && err "Could not retrieve version tag for '$SOURCE_NAME'. Delete it manually in the Dremio UI first."

  ENCODED_TAG=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$VERSION_TAG'))" 2>/dev/null)

  DEL_HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/apiv2/source/$SOURCE_NAME?version=${ENCODED_TAG}" 2>/dev/null)

  [[ "$DEL_HTTP" != "200" && "$DEL_HTTP" != "204" ]] && \
    err "Delete failed (HTTP $DEL_HTTP). Source may be locked by an active query — wait and retry."
  ok "Deleted existing source (HTTP $DEL_HTTP)"

elif [[ -n "$EXISTING" ]]; then
  if [[ "$FORCE" == "false" ]]; then
    warn "Source '$SOURCE_NAME' exists but appears unavailable."
    info "Re-run with --force to delete and recreate it."
    exit 1
  fi

  CATALOG_RESP=$(curl -sf \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/api/v3/catalog/by-path/$SOURCE_NAME" 2>/dev/null) || true

  EXISTING_ID=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('id',''))
except:
    print('')
" 2>/dev/null)

  [[ -z "$EXISTING_ID" ]] && err "Cannot find '$SOURCE_NAME' in the catalog."

  VERSION_TAG=$(echo "$CATALOG_RESP" | python3 -c \
    "import sys,json
try:
    print(json.load(sys.stdin).get('tag',''))
except:
    print('')
" 2>/dev/null)

  [[ -z "$VERSION_TAG" ]] && err "Could not retrieve version tag. Delete '$SOURCE_NAME' manually in the Dremio UI."

  ENCODED_TAG=$(python3 -c "import urllib.parse; print(urllib.parse.quote('$VERSION_TAG'))" 2>/dev/null)

  DEL_HTTP=$(curl -s -o /dev/null -w "%{http_code}" \
    -X DELETE \
    -H "Authorization: _dremio${TOKEN}" \
    "$DREMIO_URL/apiv2/source/$SOURCE_NAME?version=${ENCODED_TAG}" 2>/dev/null)

  [[ "$DEL_HTTP" != "200" && "$DEL_HTTP" != "204" ]] && \
    err "Delete failed (HTTP $DEL_HTTP)."
  ok "Deleted unavailable source (HTTP $DEL_HTTP)"
fi

# ── Build payload ───────────────────────────────────────────────────────────────
header "Creating source '$SOURCE_NAME'"

PAYLOAD=$(python3 << PYEOF
import json

config = {
    # Connection (Tags 1-5)
    'host':     '$CH_HOST',
    'port':     $CH_PORT,
    'database': '$CH_DATABASE',
    'username': '$CH_USER',
    'password': '$CH_PASS',

    # SSL / TLS (Tags 6-8)
    'useSsl': $( [[ "$USE_SSL" == "true" ]] && echo "True" || echo "False" ),

    # Connection pool / timeouts (Tags 9-11)
    'maxIdleConnections':      $MAX_IDLE,
    'connectionTimeoutSeconds': $CONNECT_TIMEOUT,
    'socketTimeoutSeconds':    $SOCKET_TIMEOUT,

    # Performance (Tags 12-13)
    'enableCompression': $( [[ "$ENABLE_COMPRESSION" == "true" ]] && echo "True" || echo "False" ),
    'fetchBlockSize':    $FETCH_BLOCK,

    # ClickHouse Cloud (Tag 15)
    'clickHouseCloud': $( [[ "$CH_CLOUD" == "true" ]] && echo "True" || echo "False" ),
}

# SSL truststore (Tags 7-8) — only include if a path was given
if '$SSL_TRUSTSTORE_PATH':
    config['sslTrustStorePath']     = '$SSL_TRUSTSTORE_PATH'
    config['sslTrustStorePassword'] = '$SSL_TRUSTSTORE_PASS'

# Excluded databases (Tag 14) — only include if non-empty
if '$EXCLUDE_DBS':
    config['excludedDatabases'] = '$EXCLUDE_DBS'

# Additional JDBC properties (Tag 16) — only include if non-empty
if '$JDBC_PROPS':
    config['additionalJdbcProperties'] = '$JDBC_PROPS'

source = {
    'name':   '$SOURCE_NAME',
    'type':   'CLICKHOUSE',
    'config': config,
    'metadataPolicy': {
        'updateMode':                          'PREFETCH_QUERIED',
        'namesRefreshMillis':                  3600000,
        'authTTLMillis':                       86400000,
        'datasetDefinitionRefreshAfterMillis': 3600000,
        'datasetDefinitionExpireAfterMillis':  10800000,
        'deleteUnavailableDatasets':           True,
        'autoPromoteDatasets':                 False,
    }
}

print(json.dumps(source, indent=2))
PYEOF
)

if [[ "$DRY_RUN" == "true" ]]; then
  echo ""
  info "Dry-run: would submit to PUT $DREMIO_URL/apiv2/source/$SOURCE_NAME:"
  echo "$PAYLOAD"
  exit 0
fi

info "Submitting source configuration..."

HTTP=$(curl -s -o /tmp/_dremio_ch_resp.json -w "%{http_code}" \
  -X PUT "$DREMIO_URL/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" 2>/dev/null)

if [[ "$HTTP" != "200" ]]; then
  ERR_MSG=$(python3 -c \
    "import sys,json; print(json.load(open('/tmp/_dremio_ch_resp.json')).get('errorMessage','unknown'))" \
    2>/dev/null || cat /tmp/_dremio_ch_resp.json)
  err "Source creation failed (HTTP $HTTP): $ERR_MSG
       Hints:
         • Verify ClickHouse is reachable from Dremio on port $CH_PORT
         • Confirm the connector plugin JAR is installed: ./install.sh --help
         • Check Dremio logs: docker logs try-dremio | grep -i clickhouse"
fi

RESPONSE=$(cat /tmp/_dremio_ch_resp.json)

# ── Verify ──────────────────────────────────────────────────────────────────────
SOURCE_TYPE=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('type',''))" 2>/dev/null)
SOURCE_ID=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
NUM_DATASETS=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('numberOfDatasets','?'))" 2>/dev/null)

[[ "$SOURCE_TYPE" != "CLICKHOUSE" ]] && warn "Unexpected source type in response: '$SOURCE_TYPE'"

EFFECTIVE_PORT=$( [[ "$CH_CLOUD" == "true" ]] && echo "8443 (Cloud/SSL)" || echo "$CH_PORT" )

echo ""
ok "Source '$SOURCE_NAME' created (id: ${SOURCE_ID:0:8}…)"
info "Host         : ${CH_HOST}:${EFFECTIVE_PORT}"
info "Database     : $CH_DATABASE"
info "SSL          : $( [[ "$USE_SSL" == "true" ]] && echo "enabled" || echo "disabled" )"
[[ "$CH_CLOUD" == "true" ]] && info "Mode         : ClickHouse Cloud"
info "Datasets     : $NUM_DATASETS"
info "Query with   : SELECT * FROM \"$SOURCE_NAME\".\"<database>\".\"<table>\" LIMIT 10;"
echo ""
echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is ready in Dremio."
