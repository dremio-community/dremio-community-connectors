#!/usr/bin/env bash
# ============================================================================
# Dremio Splunk Connector — Installer
#
# Usage examples:
#   ./install.sh                                               # Docker, interactive
#   ./install.sh --docker try-dremio --prebuilt               # Docker, pre-built JAR
#   ./install.sh --docker try-dremio --build                  # Docker, build from source
#   ./install.sh --local /opt/dremio --prebuilt               # Bare-metal
#   ./install.sh --k8s dremio-0 --namespace dremio --prebuilt # Kubernetes
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PLUGIN_JAR_NAME="dremio-splunk-connector-1.0.0-SNAPSHOT-plugin.jar"
PREBUILT_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
DREMIO_JAR_SUBDIR="jars/3rdparty"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

banner() {
  echo -e "${BOLD}${CYAN}"
  echo "  ╔══════════════════════════════════════════╗"
  echo "  ║   Dremio Splunk Connector Installer      ║"
  echo "  ╚══════════════════════════════════════════╝"
  echo -e "${RESET}"
}

step()  { echo -e "\n${BOLD}[${1}]${RESET} ${2}"; }
ok()    { echo -e "    ${GREEN}✓${RESET} ${1}"; }
warn()  { echo -e "    ${YELLOW}⚠${RESET}  ${1}"; }
err()   { echo -e "    ${RED}✗${RESET} ${1}"; }
info()  { echo -e "    ${CYAN}→${RESET} ${1}"; }

# ── Parse arguments ───────────────────────────────────────────────────────────
MODE=""
TARGET=""
BUILD_MODE="prebuilt"
NAMESPACE=""

while [[ $# -gt 0 ]]; do
  case $1 in
    --docker)   MODE="docker";  TARGET="${2:-}"; [[ -n "$TARGET" ]] && shift; shift ;;
    --local)    MODE="local";   TARGET="${2:-}"; [[ -n "$TARGET" ]] && shift; shift ;;
    --k8s)      MODE="k8s";     TARGET="${2:-}"; [[ -n "$TARGET" ]] && shift; shift ;;
    --prebuilt) BUILD_MODE="prebuilt"; shift ;;
    --build)    BUILD_MODE="build";    shift ;;
    --namespace) NAMESPACE="$2"; shift 2 ;;
    --help|-h)
      echo "Usage: $0 [--docker CONTAINER | --local DREMIO_HOME | --k8s POD] [--prebuilt | --build] [--namespace NS]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

banner

# ── Interactive mode selection ────────────────────────────────────────────────
if [[ -z "$MODE" ]]; then
  echo "  Select deployment mode:"
  echo "    1) Docker container"
  echo "    2) Local bare-metal"
  echo "    3) Kubernetes pod"
  read -rp "  Choice [1]: " choice
  case "${choice:-1}" in
    1) MODE="docker" ;;
    2) MODE="local"  ;;
    3) MODE="k8s"    ;;
    *) err "Invalid choice"; exit 1 ;;
  esac
fi

if [[ -z "$TARGET" ]]; then
  case "$MODE" in
    docker)
      CONTAINERS=$(docker ps --format '{{.Names}}' 2>/dev/null | grep -i dremio | head -5 || true)
      if [[ -n "$CONTAINERS" ]]; then
        echo "  Detected Dremio containers: $CONTAINERS"
        read -rp "  Container name [$(echo "$CONTAINERS" | head -1)]: " TARGET
        TARGET="${TARGET:-$(echo "$CONTAINERS" | head -1)}"
      else
        read -rp "  Docker container name: " TARGET
      fi ;;
    local) read -rp "  Dremio home directory [/opt/dremio]: " TARGET; TARGET="${TARGET:-/opt/dremio}" ;;
    k8s)   read -rp "  Kubernetes pod name: " TARGET ;;
  esac
fi

if [[ -z "$BUILD_MODE" ]] || [[ "$BUILD_MODE" != "build" ]]; then
  echo ""
  echo "  Install method:"
  echo "    1) Use pre-built JAR (no Maven required)"
  echo "    2) Build from source (requires Maven)"
  read -rp "  Choice [1]: " bm
  case "${bm:-1}" in
    1) BUILD_MODE="prebuilt" ;;
    2) BUILD_MODE="build"    ;;
  esac
fi

# ── Obtain JAR ────────────────────────────────────────────────────────────────
step "1" "Obtaining plugin JAR"

if [[ "$BUILD_MODE" == "prebuilt" ]]; then
  if [[ ! -f "$PREBUILT_JAR" ]]; then
    err "Pre-built JAR not found at: $PREBUILT_JAR"
    echo "    Run with --build to compile from source."
    exit 1
  fi
  ok "Using pre-built JAR: $PREBUILT_JAR"
  DEPLOY_JAR="$PREBUILT_JAR"
else
  info "Building from source with Maven..."
  case "$MODE" in
    docker)
      docker cp "$SCRIPT_DIR/." "${TARGET}:/tmp/splunk-build/"
      docker exec "$TARGET" bash -c "cd /tmp/splunk-build && mvn package -q -DskipTests"
      docker cp "${TARGET}:/tmp/splunk-build/target/${PLUGIN_JAR_NAME}" "${SCRIPT_DIR}/jars/"
      ;;
    local)
      (cd "$SCRIPT_DIR" && mvn package -q -DskipTests)
      ;;
    k8s)
      NS_ARG="${NAMESPACE:+--namespace $NAMESPACE}"
      kubectl cp "$SCRIPT_DIR/." "${TARGET}:/tmp/splunk-build/" $NS_ARG
      kubectl exec "$TARGET" $NS_ARG -- bash -c "cd /tmp/splunk-build && mvn package -q -DskipTests"
      kubectl cp "${TARGET}:/tmp/splunk-build/target/${PLUGIN_JAR_NAME}" "${SCRIPT_DIR}/jars/" $NS_ARG
      ;;
  esac
  DEPLOY_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
  ok "Build complete: $DEPLOY_JAR"
fi

# ── Deploy JAR ────────────────────────────────────────────────────────────────
step "2" "Deploying plugin JAR"

case "$MODE" in
  docker)
    DREMIO_HOME=$(docker exec "$TARGET" sh -c 'echo ${DREMIO_HOME:-/opt/dremio}' 2>/dev/null || echo "/opt/dremio")
    DEST="${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    docker exec "$TARGET" mkdir -p "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}"
    docker cp "$DEPLOY_JAR" "${TARGET}:${DEST}"
    ok "Deployed to ${TARGET}:${DEST}"
    ;;
  local)
    DEST="${TARGET}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    mkdir -p "${TARGET}/${DREMIO_JAR_SUBDIR}"
    cp "$DEPLOY_JAR" "$DEST"
    ok "Deployed to $DEST"
    ;;
  k8s)
    NS_ARG="${NAMESPACE:+--namespace $NAMESPACE}"
    DREMIO_HOME=$(kubectl exec "$TARGET" $NS_ARG -- sh -c 'echo ${DREMIO_HOME:-/opt/dremio}' 2>/dev/null || echo "/opt/dremio")
    DEST="${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    kubectl exec "$TARGET" $NS_ARG -- mkdir -p "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}"
    kubectl cp "$DEPLOY_JAR" "${TARGET}:${DEST}" $NS_ARG
    ok "Deployed to ${TARGET}:${DEST}"
    ;;
esac

# ── Restart Dremio ────────────────────────────────────────────────────────────
step "3" "Restarting Dremio"

case "$MODE" in
  docker)
    info "Restarting container $TARGET..."
    docker restart "$TARGET"
    info "Waiting for Dremio to be ready..."
    for i in $(seq 1 30); do
      if docker exec "$TARGET" curl -sf http://localhost:9047/apiv2/server_status &>/dev/null; then
        ok "Dremio is ready"
        break
      fi
      sleep 2
      if [[ $i -eq 30 ]]; then warn "Dremio did not respond after 60s — check logs"; fi
    done ;;
  local)
    warn "Restart Dremio manually to load the new plugin."
    info "Example: sudo systemctl restart dremio" ;;
  k8s)
    NS_ARG="${NAMESPACE:+--namespace $NAMESPACE}"
    warn "Restart the Dremio pod to load the new plugin."
    info "Example: kubectl rollout restart statefulset/dremio $NS_ARG" ;;
esac

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${BOLD}${GREEN}Installation complete!${RESET}"
echo ""
echo "  Next step: add a Splunk source in Dremio:"
echo "    ./add-splunk-source.sh --name splunk --host your-splunk-host"
echo ""
echo "  Or open the Dremio UI → Sources → + → Splunk"
echo ""
