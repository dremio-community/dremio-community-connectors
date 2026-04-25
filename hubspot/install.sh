#!/usr/bin/env bash
# install.sh — Deploy the Dremio HubSpot connector to a running Dremio instance
#
# Usage:
#   ./install.sh [OPTIONS]
#
# Options:
#   --docker    NAME    Dremio Docker container name (default: try-dremio)
#   --local     PATH    Bare-metal Dremio install dir (e.g. /opt/dremio)
#   --k8s       POD     Kubernetes pod name (e.g. dremio-0)
#   --prebuilt          Use JAR from jars/ instead of building from source
#   --no-restart        Don't restart Dremio after installing
#   --help              Show this help

set -euo pipefail

GREEN='\033[0;32m'; CYAN='\033[0;36m'; RESET='\033[0m'
ok()   { echo -e "  ${GREEN}✓${RESET} $*"; }
info() { echo -e "  ${CYAN}→${RESET} $*"; }

CONTAINER=""
LOCAL_DIR=""
K8S_POD=""
PREBUILT=false
RESTART=true
DREMIO_JAR_DIR="/opt/dremio/jars/3rdparty"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

while [[ $# -gt 0 ]]; do
  case $1 in
    --docker)      CONTAINER="$2";  shift 2 ;;
    --local)       LOCAL_DIR="$2";  shift 2 ;;
    --k8s)         K8S_POD="$2";    shift 2 ;;
    --prebuilt)    PREBUILT=true;   shift ;;
    --no-restart)  RESTART=false;   shift ;;
    --help|-h)     sed -n '3,12p' "$0" | sed 's/^# *//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

[[ -z "$CONTAINER" && -z "$LOCAL_DIR" && -z "$K8S_POD" ]] && CONTAINER="try-dremio"

if $PREBUILT; then
  JAR_PATH=$(find "$SCRIPT_DIR/jars" -name "dremio-hubspot-connector-*.jar" | head -1)
  [[ -z "$JAR_PATH" ]] && { echo "ERROR: No prebuilt JAR found in jars/"; exit 1; }
  info "Using prebuilt JAR: $(basename "$JAR_PATH")"
else
  info "Building from source (requires Maven inside the Dremio container)..."
  if [[ -n "$CONTAINER" ]]; then
    DOCKER=$(command -v docker 2>/dev/null || echo "/Applications/Docker.app/Contents/Resources/bin/docker")
    $DOCKER exec -u root "$CONTAINER" bash -c "rm -rf /tmp/hubspot-build" 2>/dev/null || true
    $DOCKER cp "$SCRIPT_DIR/." "${CONTAINER}:/tmp/hubspot-build"
    $DOCKER exec -u root "$CONTAINER" bash -c "cd /tmp/hubspot-build && mvn package -q -DskipTests"
    $DOCKER cp "${CONTAINER}:/tmp/hubspot-build/jars/dremio-hubspot-connector-1.0.0.jar" \
               "${SCRIPT_DIR}/jars/dremio-hubspot-connector-1.0.0.jar" 2>/dev/null || true
    JAR_PATH="${SCRIPT_DIR}/jars/dremio-hubspot-connector-1.0.0.jar"
  else
    cd "$SCRIPT_DIR" && mvn package -DskipTests -q
    JAR_PATH=$(find "$SCRIPT_DIR/jars" -name "dremio-hubspot-connector-*.jar" | head -1)
  fi
  [[ -z "$JAR_PATH" || ! -f "$JAR_PATH" ]] && { echo "ERROR: Build failed — JAR not found"; exit 1; }
  ok "Build complete: $(basename "$JAR_PATH")"
fi

JAR_NAME=$(basename "$JAR_PATH")

if [[ -n "$CONTAINER" ]]; then
  DOCKER=$(command -v docker 2>/dev/null || echo "/Applications/Docker.app/Contents/Resources/bin/docker")
  [[ ! -x "$DOCKER" ]] && { echo "ERROR: docker not found"; exit 1; }
  $DOCKER ps --format '{{.Names}}' | grep -q "^${CONTAINER}$" || { echo "ERROR: Container '$CONTAINER' is not running"; exit 1; }
  info "Deploying to Docker container '$CONTAINER'..."
  $DOCKER cp "$JAR_PATH" "${CONTAINER}:${DREMIO_JAR_DIR}/${JAR_NAME}"
  ok "Copied to ${DREMIO_JAR_DIR}/${JAR_NAME}"
  if $RESTART; then
    info "Restarting Dremio..."
    $DOCKER restart "$CONTAINER"
    info "Waiting for Dremio to come up (up to 90s)..."
    for i in $(seq 1 18); do
      STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9047/ 2>/dev/null || echo "000")
      [[ "$STATUS" == "200" ]] && { ok "Dremio is up"; break; }
      sleep 5
    done
  fi

elif [[ -n "$LOCAL_DIR" ]]; then
  cp "$JAR_PATH" "${LOCAL_DIR}/jars/3rdparty/${JAR_NAME}"
  ok "Copied to ${LOCAL_DIR}/jars/3rdparty/${JAR_NAME}"
  $RESTART && "${LOCAL_DIR}/bin/dremio" restart

elif [[ -n "$K8S_POD" ]]; then
  kubectl cp "$JAR_PATH" "${K8S_POD}:${DREMIO_JAR_DIR}/${JAR_NAME}"
  ok "Copied to ${K8S_POD}:${DREMIO_JAR_DIR}/${JAR_NAME}"
  $RESTART && kubectl exec "$K8S_POD" -- /opt/dremio/bin/dremio restart
fi

echo ""
ok "HubSpot connector installed successfully"
echo ""
echo "  Test with:"
echo "    SELECT id, firstname, lastname, email FROM hubspot.contacts LIMIT 10;"
echo "    SELECT id, name, closedate, amount FROM hubspot.deals LIMIT 10;"
echo "    SELECT * FROM hubspot.owners;"
echo ""
