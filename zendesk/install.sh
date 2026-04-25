#!/usr/bin/env bash
# ============================================================================
# Dremio Zendesk Connector — Installer
#
# Supports three deployment modes:
#   --docker   [container]   Deploy into a running Docker container (default)
#   --local    [dremio-home] Deploy to a local bare-metal Dremio installation
#   --k8s      [pod]         Deploy into a running Kubernetes pod
#
# For each mode you can choose how to obtain the plugin JAR:
#   --prebuilt    Use the JAR included in this package (no Maven required)
#   --build       Build from source (requires rebuild.sh)
#
# Usage examples:
#   ./install.sh                                 # Docker, interactive mode picker
#   ./install.sh --docker try-dremio --prebuilt  # Docker, use pre-built JAR
#   ./install.sh --docker try-dremio --build     # Docker, build from source
#   ./install.sh --local /opt/dremio --prebuilt  # Bare-metal, pre-built JAR
#   ./install.sh --k8s dremio-0 --prebuilt       # Kubernetes pod, pre-built JAR
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PLUGIN_JAR_NAME="dremio-zendesk-connector-1.0.0.jar"
PREBUILT_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
DREMIO_JAR_SUBDIR="jars/3rdparty"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

banner() {
  echo ""
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}${CYAN}   Dremio Zendesk Connector Installer${RESET}"
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════${RESET}"
  echo ""
}

step()  { echo -e "\n${BOLD}[${1}]${RESET} ${2}"; }
ok()    { echo -e "    ${GREEN}✓${RESET}  ${1}"; }
warn()  { echo -e "    ${YELLOW}⚠${RESET}  ${1}"; }
err()   { echo -e "    ${RED}✗${RESET}  ${1}" >&2; exit 1; }
info()  { echo -e "    ${CYAN}→${RESET}  ${1}"; }

MODE=""; TARGET=""; JAR_MODE=""; NAMESPACE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)     MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)      MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --k8s)        MODE="k8s";    TARGET="${2:-}"; shift 2 || shift ;;
    --prebuilt)   JAR_MODE="prebuilt"; shift ;;
    --build)      JAR_MODE="build"; shift ;;
    --namespace|-n) NAMESPACE="${2:-}"; shift 2 ;;
    -h|--help)
      grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) err "Unknown argument: $1" ;;
  esac
done

banner

if [[ -z "$MODE" ]]; then
  echo -e "  Deployment mode:"
  echo -e "    ${BOLD}1${RESET}) Docker container  (default: try-dremio)"
  echo -e "    ${BOLD}2${RESET}) Bare-metal / local Dremio"
  echo -e "    ${BOLD}3${RESET}) Kubernetes pod"
  read -rp "  Select [1]: " MODE_CHOICE </dev/tty
  case "${MODE_CHOICE:-1}" in
    1) MODE="docker" ;; 2) MODE="local" ;; 3) MODE="k8s" ;;
    *) err "Invalid selection" ;;
  esac
fi

if [[ -z "$TARGET" ]]; then
  case "$MODE" in
    docker) read -rp "  Container name [try-dremio]: " TARGET </dev/tty; TARGET="${TARGET:-try-dremio}" ;;
    local)  read -rp "  Dremio home [/opt/dremio]: "  TARGET </dev/tty; TARGET="${TARGET:-/opt/dremio}" ;;
    k8s)    read -rp "  Pod name: " TARGET </dev/tty ;;
  esac
fi

if [[ -z "$JAR_MODE" ]]; then
  echo ""
  echo -e "  JAR source:"
  echo -e "    ${BOLD}1${RESET}) Pre-built JAR (included — no Maven needed)  [default]"
  echo -e "    ${BOLD}2${RESET}) Build from source (runs rebuild.sh)"
  read -rp "  Select [1]: " JAR_CHOICE </dev/tty
  case "${JAR_CHOICE:-1}" in
    1) JAR_MODE="prebuilt" ;; 2) JAR_MODE="build" ;;
    *) err "Invalid selection" ;;
  esac
fi

step 1 "Preparing JAR"

if [[ "$JAR_MODE" == "prebuilt" ]]; then
  [[ ! -f "$PREBUILT_JAR" ]] && err "Pre-built JAR not found at: $PREBUILT_JAR"
  ok "Using pre-built JAR: $PLUGIN_JAR_NAME"
  SOURCE_JAR="$PREBUILT_JAR"
else
  info "Building from source via rebuild.sh..."
  bash "${SCRIPT_DIR}/rebuild.sh" --"${MODE}" "$TARGET" --force
  ok "Build complete"
  exit 0
fi

step 2 "Deploying JAR to ${MODE} target: ${TARGET}"

case "$MODE" in
  docker)
    docker cp "$SOURCE_JAR" "${TARGET}:/tmp/${PLUGIN_JAR_NAME}"
    docker exec -u root "$TARGET" bash -c "
      cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
      chmod 644 /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
    "
    ok "JAR deployed to /opt/dremio/${DREMIO_JAR_SUBDIR}/"
    ;;
  local)
    install -m 644 "$SOURCE_JAR" "${TARGET}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    ok "JAR deployed to ${TARGET}/${DREMIO_JAR_SUBDIR}/"
    ;;
  k8s)
    KUBECTL_NS="${NAMESPACE:+--namespace $NAMESPACE}"
    kubectl cp "$SOURCE_JAR" ${KUBECTL_NS} "${TARGET}:/opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    ok "JAR deployed to pod ${TARGET}"
    ;;
esac

step 3 "Restarting Dremio"

case "$MODE" in
  docker)
    docker restart "$TARGET" > /dev/null
    info "Waiting for Dremio to start..."
    for i in $(seq 1 20); do
      HTTP=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9047/apiv2/info" 2>/dev/null || true)
      if [[ "$HTTP" == "200" || "$HTTP" == "404" ]]; then
        echo ""; ok "Dremio is up"; break
      fi
      printf "."; sleep 3
    done
    echo ""
    ;;
  local)
    "${TARGET}/bin/dremio" restart 2>/dev/null || warn "Restart Dremio manually to load the connector."
    ;;
  k8s)
    kubectl exec ${NAMESPACE:+--namespace $NAMESPACE} "$TARGET" -- \
      /opt/dremio/bin/dremio restart 2>/dev/null || warn "Restart the pod manually to load the connector."
    ;;
esac

echo ""
echo -e "${GREEN}${BOLD}Installation complete.${RESET}"
echo ""
echo -e "  Next: open Dremio → ${BOLD}Add Source${RESET} → ${BOLD}Zendesk${RESET}"
echo ""
echo -e "  Or create a source from the command line:"
echo -e "    ${CYAN}./add-zendesk-source.sh --name zendesk \\${RESET}"
echo -e "    ${CYAN}    --subdomain acme --email agent@example.com \\${RESET}"
echo -e "    ${CYAN}    --api-token your_token${RESET}"
echo ""
