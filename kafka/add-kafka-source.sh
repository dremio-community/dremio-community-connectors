#!/usr/bin/env bash
# add-kafka-source.sh — Register an Apache Kafka source in Dremio via REST API
#
# PURPOSE
#   Creates (or recreates) a Kafka source in Dremio without touching the UI.
#   After running, every Kafka topic appears as a table under the source name.
#
# WHAT IT DOES
#   Calls PUT /apiv2/source/{name} to create or update the source with the
#   configuration you supply. On success, Dremio immediately connects and
#   discovers all non-excluded topics.
#
# USAGE
#   ./add-kafka-source.sh --name <source_name> --brokers <host:port>[,...] [options]
#
# CONNECTION OPTIONS
#   --name         NAME          Dremio source name to create (required)
#   --brokers      HOST:PORT     Bootstrap server(s), comma-separated (required)
#                                Example: broker1:9092,broker2:9092
#   --dremio       URL           Dremio base URL (default: http://localhost:9047)
#   --user         USERNAME      Dremio username (default: dremio)
#   --password     PASSWORD      Dremio password (prompted interactively if omitted)
#
# SCHEMA OPTIONS
#   --schema-mode  MODE          JSON (default), RAW, or AVRO
#   --sample       N             Messages to sample per partition for JSON inference (default: 100)
#   --max-records  N             Default max records per partition per scan (default: 10000, 0=all)
#
# TOPIC FILTERING
#   --exclude      REGEX         Regex for topics to exclude (default: "^__")
#   --include      REGEX         Regex for topics to include (default: all)
#   --cache-ttl    SECONDS       Metadata cache TTL in seconds (default: 60)
#
# SECURITY
#   --security     PROTOCOL      PLAINTEXT (default), SSL, SASL_PLAINTEXT, SASL_SSL
#   --sasl-mech    MECHANISM     PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 (default: PLAIN)
#   --sasl-user    USERNAME      SASL username
#   --sasl-pass    PASSWORD      SASL password
#
# SSL / TLS (truststore — verifies broker certificate)
#   --ssl-truststore PATH        Path to SSL truststore file (JKS or PKCS12)
#   --ssl-ts-pass  PASSWORD      Truststore password
#   --ssl-ts-type  TYPE          JKS (default) or PKCS12
#   --disable-hostname-verify    Skip TLS hostname verification (dev/self-signed only)
#
# mTLS (keystore — client certificate sent to broker)
#   --keystore     PATH          Path to client keystore file (enables mTLS)
#   --keystore-pass PASSWORD     Keystore password
#   --keystore-type TYPE         JKS (default) or PKCS12
#
# SCHEMA REGISTRY (required for --schema-mode AVRO)
#   --schema-registry URL        Confluent Schema Registry URL
#                                Example: http://schema-registry:8081
#                                Example: https://psrc-xyz.us-east-1.aws.confluent.cloud
#   --schema-registry-user USER  Schema Registry username (Confluent Cloud: API key)
#   --schema-registry-pass PASS  Schema Registry password (Confluent Cloud: API secret)
#   --schema-registry-no-ssl-verify  Skip SSL verification for Schema Registry
#                                    (dev/self-signed certificates only)
#
# ADVANCED
#   --max-poll     N             Max records per Kafka poll() call (default: 500)
#   --timeout      MS            Request timeout in ms (default: 30000)
#   --force                      Delete and recreate if source already exists
#   --dry-run                    Print the JSON payload without submitting
#   -h, --help                   Show this help
#
# EXAMPLES
#   # Minimal — local Kafka, JSON schema inference
#   ./add-kafka-source.sh --name kafka_local --brokers localhost:9092
#
#   # Multi-broker cluster
#   ./add-kafka-source.sh \
#     --name prod_kafka \
#     --brokers broker1:9092,broker2:9092,broker3:9092
#
#   # SASL_SSL (production with SCRAM)
#   ./add-kafka-source.sh \
#     --name prod_kafka \
#     --brokers kafka.internal:9093 \
#     --security SASL_SSL \
#     --sasl-mech SCRAM-SHA-512 \
#     --sasl-user myuser \
#     --sasl-pass mysecret \
#     --ssl-truststore /etc/ssl/kafka.jks \
#     --ssl-ts-pass changeit
#
#   # mTLS (mutual TLS — client presents certificate to broker)
#   ./add-kafka-source.sh \
#     --name kafka_mtls \
#     --brokers kafka.internal:9093 \
#     --security SSL \
#     --ssl-truststore /etc/ssl/ca.jks \
#     --ssl-ts-pass changeit \
#     --keystore /etc/ssl/client.jks \
#     --keystore-pass clientpass
#
#   # AVRO mode with Confluent Schema Registry
#   ./add-kafka-source.sh \
#     --name kafka_avro \
#     --brokers broker:9092 \
#     --schema-mode AVRO \
#     --schema-registry http://schema-registry:8081
#
#   # AVRO mode with Confluent Cloud
#   ./add-kafka-source.sh \
#     --name confluent_cloud \
#     --brokers pkc-xxx.us-east-1.aws.confluent.cloud:9092 \
#     --security SASL_SSL \
#     --sasl-mech PLAIN \
#     --sasl-user <api-key> \
#     --sasl-pass <api-secret> \
#     --schema-mode AVRO \
#     --schema-registry https://psrc-xxx.us-east-1.aws.confluent.cloud \
#     --schema-registry-user <sr-api-key> \
#     --schema-registry-pass <sr-api-secret>
#
#   # Raw mode — metadata columns only, no JSON parsing
#   ./add-kafka-source.sh --name kafka_raw --brokers localhost:9092 --schema-mode RAW
#
#   # Force recreate (e.g. after broker change)
#   ./add-kafka-source.sh --name kafka_local --brokers localhost:9092 --force
#
#   # Non-interactive (CI/CD)
#   ./add-kafka-source.sh \
#     --name ci_kafka \
#     --brokers kafka:9092 \
#     --user dremio_admin \
#     --password dremio_pass
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
BROKERS=""
SCHEMA_MODE="JSON"
SAMPLE_RECORDS=100
MAX_RECORDS=10000
EXCLUDE_PATTERN="^__"
INCLUDE_PATTERN=""
CACHE_TTL=60
# Security
SECURITY_PROTOCOL="PLAINTEXT"
SASL_MECHANISM="PLAIN"
SASL_USER=""
SASL_PASS=""
# SSL truststore
SSL_TRUSTSTORE_PATH=""
SSL_TRUSTSTORE_PASS=""
SSL_TRUSTSTORE_TYPE="JKS"
DISABLE_HOSTNAME_VERIFY=false
# mTLS keystore
KEYSTORE_PATH=""
KEYSTORE_PASS=""
KEYSTORE_TYPE="JKS"
# Schema Registry
SCHEMA_REGISTRY_URL=""
SCHEMA_REGISTRY_USER=""
SCHEMA_REGISTRY_PASS=""
SCHEMA_REGISTRY_NO_SSL_VERIFY=false
# Advanced
MAX_POLL=500
TIMEOUT_MS=30000
DRY_RUN=false
FORCE=false

# ── Argument parsing ────────────────────────────────────────────────────────────
usage() { grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'; }

while [[ $# -gt 0 ]]; do
  case $1 in
    --name)          SOURCE_NAME="$2";              shift 2 ;;
    --brokers)       BROKERS="$2";                  shift 2 ;;
    --dremio)        DREMIO_URL="${2%/}";           shift 2 ;;
    --user)          DREMIO_USER="$2";              shift 2 ;;
    --password)      DREMIO_PASS="$2";              shift 2 ;;
    --schema-mode)   SCHEMA_MODE="$2";              shift 2 ;;
    --sample)        SAMPLE_RECORDS="$2";           shift 2 ;;
    --max-records)   MAX_RECORDS="$2";              shift 2 ;;
    --exclude)       EXCLUDE_PATTERN="$2";          shift 2 ;;
    --include)       INCLUDE_PATTERN="$2";          shift 2 ;;
    --cache-ttl)     CACHE_TTL="$2";                shift 2 ;;
    --security)      SECURITY_PROTOCOL="$2";        shift 2 ;;
    --sasl-mech)     SASL_MECHANISM="$2";           shift 2 ;;
    --sasl-user)     SASL_USER="$2";               shift 2 ;;
    --sasl-pass)     SASL_PASS="$2";               shift 2 ;;
    --ssl-truststore) SSL_TRUSTSTORE_PATH="$2";     shift 2 ;;
    --ssl-ts-pass)   SSL_TRUSTSTORE_PASS="$2";      shift 2 ;;
    --ssl-ts-type)   SSL_TRUSTSTORE_TYPE="$2";      shift 2 ;;
    --disable-hostname-verify) DISABLE_HOSTNAME_VERIFY=true; shift ;;
    --keystore)      KEYSTORE_PATH="$2";            shift 2 ;;
    --keystore-pass) KEYSTORE_PASS="$2";            shift 2 ;;
    --keystore-type) KEYSTORE_TYPE="$2";            shift 2 ;;
    --schema-registry) SCHEMA_REGISTRY_URL="$2";   shift 2 ;;
    --schema-registry-user) SCHEMA_REGISTRY_USER="$2"; shift 2 ;;
    --schema-registry-pass) SCHEMA_REGISTRY_PASS="$2"; shift 2 ;;
    --schema-registry-no-ssl-verify) SCHEMA_REGISTRY_NO_SSL_VERIFY=true; shift ;;
    --max-poll)      MAX_POLL="$2";                 shift 2 ;;
    --timeout)       TIMEOUT_MS="$2";               shift 2 ;;
    --force)         FORCE=true;                    shift   ;;
    --dry-run)       DRY_RUN=true;                 shift   ;;
    -h|--help)       usage; exit 0 ;;
    *) err "Unknown argument: $1. Use --help for usage." ;;
  esac
done

[[ -z "$SOURCE_NAME" ]] && { usage; err "--name is required"; }
[[ -z "$BROKERS"     ]] && { usage; err "--brokers is required"; }

if [[ "$SCHEMA_MODE" == "AVRO" && -z "$SCHEMA_REGISTRY_URL" ]]; then
  err "--schema-mode AVRO requires --schema-registry <url>"
fi

# ── Dremio password prompt ──────────────────────────────────────────────────────
if [[ -z "$DREMIO_PASS" ]]; then
  read -rsp "Dremio password for '$DREMIO_USER': " DREMIO_PASS </dev/tty
  echo >&2
fi

# ── Authenticate ────────────────────────────────────────────────────────────────
header "Connecting to Dremio at $DREMIO_URL"

AUTH_JSON=$(curl -sf -X POST "$DREMIO_URL/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"$DREMIO_USER\",\"password\":\"$DREMIO_PASS\"}" 2>/dev/null) \
  || err "Could not reach Dremio at $DREMIO_URL — is it running?"

TOKEN=$(echo "$AUTH_JSON" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('token',''))" 2>/dev/null)
[[ -z "$TOKEN" ]] && err "Authentication failed — check username/password"
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

  if [[ -z "$VERSION_TAG" ]]; then
    FULL_RESP=$(curl -sf \
      -H "Authorization: _dremio${TOKEN}" \
      "$DREMIO_URL/api/v3/catalog/${EXISTING_ID}" 2>/dev/null) || true
    VERSION_TAG=$(echo "$FULL_RESP" | python3 -c \
      "import sys,json
try:
    print(json.load(sys.stdin).get('tag',''))
except:
    print('')
" 2>/dev/null)
  fi

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

PAYLOAD=$(python3 -c "
import json

config = {
    'bootstrapServers':              '$BROKERS',
    'schemaMode':                    '$SCHEMA_MODE',
    'sampleRecordsForSchema':        $SAMPLE_RECORDS,
    'defaultMaxRecordsPerPartition': $MAX_RECORDS,
    'topicExcludePattern':           '$EXCLUDE_PATTERN',
    'topicIncludePattern':           '$INCLUDE_PATTERN',
    'metadataCacheTtlSeconds':       $CACHE_TTL,
    'securityProtocol':              '$SECURITY_PROTOCOL',
    'maxPollRecords':                $MAX_POLL,
    'requestTimeoutMs':              $TIMEOUT_MS,
}

# SASL — only when protocol requires it
if '$SECURITY_PROTOCOL' in ('SASL_PLAINTEXT', 'SASL_SSL'):
    config['saslMechanism'] = '$SASL_MECHANISM'
    if '$SASL_USER':
        config['saslUsername'] = '$SASL_USER'
    if '$SASL_PASS':
        config['saslPassword'] = '$SASL_PASS'

# SSL truststore — verifies broker certificate
if '$SSL_TRUSTSTORE_PATH':
    config['sslTruststorePath']     = '$SSL_TRUSTSTORE_PATH'
    config['sslTruststorePassword'] = '$SSL_TRUSTSTORE_PASS'
    config['sslTruststoreType']     = '$SSL_TRUSTSTORE_TYPE'

# TLS hostname verification bypass (dev/self-signed certs only)
if $DISABLE_HOSTNAME_VERIFY:
    config['sslDisableHostnameVerification'] = True

# mTLS keystore — client certificate sent to broker
if '$KEYSTORE_PATH':
    config['sslKeystorePath']     = '$KEYSTORE_PATH'
    config['sslKeystorePassword'] = '$KEYSTORE_PASS'
    config['sslKeystoreType']     = '$KEYSTORE_TYPE'

# Schema Registry — required for AVRO mode
if '$SCHEMA_REGISTRY_URL':
    config['schemaRegistryUrl'] = '$SCHEMA_REGISTRY_URL'
if '$SCHEMA_REGISTRY_USER':
    config['schemaRegistryUsername'] = '$SCHEMA_REGISTRY_USER'
if '$SCHEMA_REGISTRY_PASS':
    config['schemaRegistryPassword'] = '$SCHEMA_REGISTRY_PASS'
if $SCHEMA_REGISTRY_NO_SSL_VERIFY:
    config['schemaRegistryDisableSslVerification'] = True

source = {
    'name':   '$SOURCE_NAME',
    'type':   'APACHE_KAFKA',
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
")

if [[ "$DRY_RUN" == "true" ]]; then
  echo ""
  info "Dry-run: would submit to PUT $DREMIO_URL/apiv2/source/$SOURCE_NAME:"
  echo "$PAYLOAD"
  exit 0
fi

info "Submitting source configuration..."

HTTP=$(curl -s -o /tmp/_dremio_kafka_resp.json -w "%{http_code}" \
  -X PUT "$DREMIO_URL/apiv2/source/$SOURCE_NAME" \
  -H "Authorization: _dremio${TOKEN}" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD" 2>/dev/null)

if [[ "$HTTP" != "200" ]]; then
  ERR_MSG=$(python3 -c \
    "import sys,json; print(json.load(open('/tmp/_dremio_kafka_resp.json')).get('errorMessage','unknown'))" \
    2>/dev/null || cat /tmp/_dremio_kafka_resp.json)
  err "Source creation failed (HTTP $HTTP): $ERR_MSG
       Hint: verify Kafka is reachable from Dremio:
         curl http://<dremio-host>:9047/apiv2/server_status"
fi

RESPONSE=$(cat /tmp/_dremio_kafka_resp.json)

# ── Verify ──────────────────────────────────────────────────────────────────────
SOURCE_TYPE=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('type',''))" 2>/dev/null)
SOURCE_ID=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('id',''))" 2>/dev/null)
NUM_DATASETS=$(echo "$RESPONSE" | python3 -c \
  "import sys,json; print(json.load(sys.stdin).get('numberOfDatasets','?'))" 2>/dev/null)

[[ "$SOURCE_TYPE" != "APACHE_KAFKA" ]] && warn "Unexpected source type in response: '$SOURCE_TYPE'"

echo ""
ok "Source '$SOURCE_NAME' created (id: ${SOURCE_ID:0:8}…)"
info "Topics discovered: $NUM_DATASETS"
info "Schema mode: $SCHEMA_MODE"
[[ -n "$SCHEMA_REGISTRY_URL" ]] && info "Schema Registry: $SCHEMA_REGISTRY_URL"
info "Query with: SELECT * FROM \"$SOURCE_NAME\".\"<topic>\" LIMIT 10;"
echo ""
echo -e "${GREEN}Done.${RESET} Source '$SOURCE_NAME' is ready in Dremio."
