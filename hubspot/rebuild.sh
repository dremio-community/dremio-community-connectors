#!/usr/bin/env bash
# rebuild.sh — Detect running Dremio version, update pom.xml, rebuild JAR, and redeploy
#
# Usage:
#   ./rebuild.sh [OPTIONS]
#
# Options:
#   --docker    NAME    Docker container name (default: try-dremio)
#   --local     PATH    Bare-metal Dremio dir (e.g. /opt/dremio)
#   --k8s       POD     Kubernetes pod name
#   --force             Rebuild even if version already matches
#   --dry-run           Show detected version without building
#   --help              Show this help

set -euo pipefail

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; RESET='\033[0m'
ok()   { echo -e "  ${GREEN}✓${RESET} $*"; }
warn() { echo -e "  ${YELLOW}⚠${RESET} $*"; }
info() { echo -e "  ${CYAN}→${RESET} $*"; }

CONTAINER=""
LOCAL_DIR=""
K8S_POD=""
FORCE=false
DRY_RUN=false
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM="$SCRIPT_DIR/pom.xml"

while [[ $# -gt 0 ]]; do
  case $1 in
    --docker)   CONTAINER="$2";  shift 2 ;;
    --local)    LOCAL_DIR="$2";  shift 2 ;;
    --k8s)      K8S_POD="$2";    shift 2 ;;
    --force)    FORCE=true;      shift ;;
    --dry-run)  DRY_RUN=true;    shift ;;
    --help|-h)  sed -n '3,13p' "$0" | sed 's/^# *//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

[[ -z "$CONTAINER" && -z "$LOCAL_DIR" && -z "$K8S_POD" ]] && CONTAINER="try-dremio"

info "Detecting Dremio version..."
if [[ -n "$CONTAINER" ]]; then
  DOCKER=$(command -v docker 2>/dev/null || echo "/Applications/Docker.app/Contents/Resources/bin/docker")
  VERSION=$($DOCKER exec "$CONTAINER" find /opt/dremio/jars -name "dremio-common-*.jar" ! -name "*tests*" 2>/dev/null \
    | head -1 | sed 's/.*dremio-common-//;s/\.jar//')
elif [[ -n "$LOCAL_DIR" ]]; then
  VERSION=$(find "${LOCAL_DIR}/jars" -name "dremio-common-*.jar" ! -name "*tests*" 2>/dev/null \
    | head -1 | sed 's/.*dremio-common-//;s/\.jar//')
elif [[ -n "$K8S_POD" ]]; then
  VERSION=$(kubectl exec "$K8S_POD" -- find /opt/dremio/jars -name "dremio-common-*.jar" ! -name "*tests*" 2>/dev/null \
    | head -1 | sed 's/.*dremio-common-//;s/\.jar//')
fi

[[ -z "$VERSION" ]] && { echo "ERROR: Could not detect Dremio version"; exit 1; }
ok "Detected Dremio version: $VERSION"

CURRENT=$(grep -m1 '<dremio.version>' "$POM" | sed 's/.*<dremio.version>//;s/<.*//')
info "pom.xml version: $CURRENT"

if $DRY_RUN; then
  info "Dry-run: would update to $VERSION"
  exit 0
fi

if [[ "$CURRENT" == "$VERSION" && "$FORCE" == "false" ]]; then
  ok "Version already matches — skipping rebuild. Use --force to rebuild anyway."
  exit 0
fi

sed -i.bak "s|<dremio.version>.*</dremio.version>|<dremio.version>${VERSION}</dremio.version>|" "$POM"
rm -f "${POM}.bak"
ok "Updated pom.xml: $CURRENT → $VERSION"

info "Rebuilding JAR inside container..."
if [[ -n "$CONTAINER" ]]; then
  DOCKER=$(command -v docker 2>/dev/null || echo "/Applications/Docker.app/Contents/Resources/bin/docker")
  $DOCKER exec -u root "$CONTAINER" bash -c "rm -rf /tmp/hubspot-build" 2>/dev/null || true
  $DOCKER cp "$SCRIPT_DIR/." "${CONTAINER}:/tmp/hubspot-build"
  $DOCKER exec -u root "$CONTAINER" bash -c "cd /tmp/hubspot-build && mvn package -q -DskipTests"
  $DOCKER cp "${CONTAINER}:/tmp/hubspot-build/jars/dremio-hubspot-connector-1.0.0.jar" \
             "${SCRIPT_DIR}/jars/dremio-hubspot-connector-1.0.0.jar"
else
  cd "$SCRIPT_DIR" && mvn package -DskipTests -q
fi

JAR_PATH=$(find "$SCRIPT_DIR/jars" -name "dremio-hubspot-connector-*.jar" | head -1)
ok "Built: $(basename "$JAR_PATH")"

"$SCRIPT_DIR/install.sh" \
  ${CONTAINER:+--docker "$CONTAINER"} \
  ${LOCAL_DIR:+--local "$LOCAL_DIR"} \
  ${K8S_POD:+--k8s "$K8S_POD"} \
  --prebuilt
