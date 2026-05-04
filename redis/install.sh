#!/bin/bash
# ============================================================================
# Dremio Redis Connector — Installer
#
# Supports three deployment modes:
#   --docker   [container]   Deploy into a running Docker container (default)
#   --local    [dremio-home] Deploy to a local bare-metal Dremio installation
#   --k8s      [pod]         Deploy into a running Kubernetes pod
#
# Usage examples:
#   ./install.sh                                              # fully interactive
#   ./install.sh --docker try-dremio --prebuilt              # Docker, pre-built plugin
#   ./install.sh --local /opt/dremio --prebuilt              # bare-metal, pre-built
#   ./install.sh --k8s dremio-0 --prebuilt                   # Kubernetes, pre-built
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PLUGIN_JAR_NAME="dremio-redis-connector-1.0.0-SNAPSHOT-plugin.jar"
PREBUILT_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
DREMIO_JAR_SUBDIR="jars/3rdparty"

# Redis Connector/J — single JAR, no separate driver deployment needed.
# The driver is bundled inside the plugin fat JAR by maven-shade-plugin.

# ANSI colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

banner() {
  echo ""
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}${CYAN}   Dremio Redis Connector Installer${RESET}"
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════${RESET}"
  echo ""
}

step()  { echo -e "${BOLD}[${1}]${RESET} ${2}"; }
ok()    { echo -e "    ${GREEN}✓${RESET} ${1}"; }
warn()  { echo -e "    ${YELLOW}⚠${RESET}  ${1}"; }
err()   { echo -e "    ${RED}✗${RESET} ${1}"; }
info()  { echo -e "    ${CYAN}→${RESET} ${1}"; }

# ============================================================================
# Parse arguments
# ============================================================================
MODE=""
TARGET=""
JAR_MODE=""
NAMESPACE=""
POD_SELECTOR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)          MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)           MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --k8s)             MODE="k8s";    TARGET="${2:-}"; shift 2 || shift ;;
    --prebuilt)        JAR_MODE="prebuilt"; shift ;;
    --build)           JAR_MODE="build"; shift ;;
    --namespace|-n)    NAMESPACE="${2:-}"; shift 2 ;;
    --pod-selector|-l) POD_SELECTOR="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--docker CONTAINER | --local DREMIO_HOME | --k8s POD]"
      echo "          [--prebuilt | --build]"
      echo "          K8s options: [--namespace NS] [--pod-selector SELECTOR]"
      exit 0 ;;
    *) echo "Unknown argument: $1"; exit 1 ;;
  esac
done

# ============================================================================
# Interactive prompts
# ============================================================================
banner

if [[ -z "$MODE" ]]; then
  echo -e "${BOLD}Select deployment mode:${RESET}"
  echo "  1) Docker container  (most common for dev/test)"
  echo "  2) Local / bare-metal Dremio installation"
  echo "  3) Kubernetes pod"
  echo ""
  read -rp "Choice [1]: " MODE_CHOICE
  case "${MODE_CHOICE:-1}" in
    1) MODE="docker" ;;
    2) MODE="local"  ;;
    3) MODE="k8s"    ;;
    *) err "Invalid choice"; exit 1 ;;
  esac
fi

if [[ -z "$TARGET" ]]; then
  case "$MODE" in
    docker) read -rp "Container name [try-dremio]: " TARGET; TARGET="${TARGET:-try-dremio}" ;;
    local)  read -rp "Dremio home directory [/opt/dremio]: " TARGET; TARGET="${TARGET:-/opt/dremio}" ;;
    k8s)
      read -rp "Coordinator pod name (e.g. dremio-0): " TARGET
      if [[ -z "$TARGET" ]]; then err "Pod name is required for --k8s mode"; exit 1; fi
      if [[ -z "$NAMESPACE" ]]; then
        read -rp "Kubernetes namespace [leave blank for current context]: " NAMESPACE
      fi
      ;;
  esac
fi

if [[ -z "$JAR_MODE" ]]; then
  echo ""
  echo -e "${BOLD}How would you like to obtain the connector plugin JAR?${RESET}"
  echo ""
  echo "  1) Use pre-built JAR  — fast, no Maven required"
  if [[ -f "$PREBUILT_JAR" ]]; then
    echo -e "         ${GREEN}(JAR found — $(du -sh "$PREBUILT_JAR" | cut -f1))${RESET}"
  else
    echo -e "         ${RED}(JAR not found in jars/ — choose option 2)${RESET}"
  fi
  echo ""
  echo "  2) Build from source  — compiles inside the target environment (~2-3 min)"
  echo "         Requires: Maven (installed automatically in Docker/k8s)"
  echo ""
  read -rp "Choice [1]: " JAR_CHOICE
  case "${JAR_CHOICE:-1}" in
    1) JAR_MODE="prebuilt" ;;
    2) JAR_MODE="build"    ;;
    *) err "Invalid choice"; exit 1 ;;
  esac
fi

if [[ "$JAR_MODE" == "prebuilt" && ! -f "$PREBUILT_JAR" ]]; then
  err "Pre-built plugin JAR not found at: $PREBUILT_JAR"
  err "Re-run with --build to compile from source, or add the JAR to jars/"
  exit 1
fi

echo ""
echo -e "${BOLD}Installation plan:${RESET}"
info "Mode          : $MODE"
info "Target        : $TARGET"
info "Plugin JAR    : $JAR_MODE"
[[ "$MODE" == "k8s" && -n "$NAMESPACE" ]] && info "Namespace     : $NAMESPACE"
[[ "$MODE" == "k8s" && -n "$POD_SELECTOR" ]] && info "Pod selector  : $POD_SELECTOR"
echo ""
read -rp "Proceed? [Y/n]: " CONFIRM || CONFIRM="Y"
[[ "${CONFIRM:-Y}" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# ============================================================================
# Helper: build plugin from source
# ============================================================================
install_dremio_jars() {
  # Install Dremio JARs into Maven local repo inside the target environment
  local exec_cmd="$1"
  $exec_cmd bash -c '
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
JARS=/opt/dremio/jars
TP=$JARS/3rdparty

install_jar() {
  mvn install:install-file -Dfile="$1" -DgroupId="$2" -DartifactId="$3" \
    -Dversion="$4" -Dpackaging=jar -q 2>/dev/null && echo "  installed $3" || true
}

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar         com.dremio dremio-sabot-kernel         $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar               com.dremio dremio-common               $DREMIO_VER
install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar        com.dremio.plugin dremio-plugin-common $DREMIO_VER
install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar   com.dremio dremio-sabot-kernel-proto   $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar   com.dremio dremio-sabot-vector-tools   $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar   com.dremio dremio-services-namespace   $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar            com.dremio dremio-connector            $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar        com.dremio dremio-sabot-logical        $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar          com.dremio dremio-common-core          $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                   org.apache.arrow arrow-vector          $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar              org.apache.arrow arrow-memory-core     $ARROW_VER
install_jar $TP/arrow-format-${ARROW_VER}.jar                   org.apache.arrow arrow-format          $ARROW_VER

install_jar $JARS/dremio-ce-jdbc-plugin-${DREMIO_VER}.jar       com.dremio.plugins dremio-ce-jdbc-plugin      $DREMIO_VER
install_jar $JARS/dremio-ce-jdbc-fetcher-api-${DREMIO_VER}.jar  com.dremio.plugins dremio-ce-jdbc-fetcher-api $DREMIO_VER

CALCITE_VER=$(ls $TP/calcite-core-*.jar 2>/dev/null | head -1 | sed "s/.*calcite-core-//;s/\.jar//")
if [ -n "$CALCITE_VER" ]; then
  install_jar $TP/calcite-core-${CALCITE_VER}.jar   org.apache.calcite calcite-core   $CALCITE_VER
  install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar org.apache.calcite calcite-linq4j $CALCITE_VER
fi
'
}

# ============================================================================
# DOCKER mode
# ============================================================================
if [[ "$MODE" == "docker" ]]; then

  step "1" "Verifying container '$TARGET' is running..."
  if ! docker ps --format '{{.Names}}' | grep -q "^${TARGET}$"; then
    err "Container '$TARGET' is not running."
    info "Start it with: docker start $TARGET"
    exit 1
  fi
  ok "Container is running"

  DEPLOY_FROM=""

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "2" "Copying pre-built plugin JAR into container..."
    docker cp "$PREBUILT_JAR" "${TARGET}:/tmp/${PLUGIN_JAR_NAME}"
    ok "Plugin JAR copied"
    DEPLOY_FROM="/tmp/${PLUGIN_JAR_NAME}"
  else
    step "2" "Building plugin from source..."
    if ! docker exec "$TARGET" which mvn > /dev/null 2>&1; then
      info "Installing Maven in container (one-time)..."
      docker exec -u root "$TARGET" bash -c \
        "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -3"
    fi
    step "2a" "Installing Dremio JARs into Maven local repo..."
    install_dremio_jars "docker exec $TARGET"
    ok "Dremio JARs installed"

    step "2b" "Copying source and building..."
    docker exec -u root "$TARGET" bash -c "rm -rf /tmp/redis-build && mkdir -p /tmp/redis-build"
    docker cp "${SCRIPT_DIR}/." "${TARGET}:/tmp/redis-build/"
    docker exec -u root "$TARGET" bash -c "rm -rf /tmp/redis-build/target && chown -R dremio:dremio /tmp/redis-build"
    docker exec "$TARGET" bash -c "cd /tmp/redis-build && mvn package -DskipTests -q --batch-mode"
    ok "Build successful"
    docker cp "${TARGET}:/tmp/redis-build/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null && \
      ok "Built JAR saved to jars/ for future --prebuilt installs"
    DEPLOY_FROM="/tmp/redis-build/target/${PLUGIN_JAR_NAME}"
  fi

  step "3" "Deploying JAR to ${DREMIO_JAR_SUBDIR}..."
  docker exec -u root "$TARGET" bash -c "
    cp ${DEPLOY_FROM} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
    chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
  "
  ok "Deployed plugin → /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"

  step "4" "Restarting Dremio container..."
  docker restart "$TARGET"
  echo -n "    Waiting for Dremio to start"
  for i in $(seq 1 24); do
    sleep 5
    if docker exec "$TARGET" bash -c "curl -sf http://localhost:9047/apiv2/server_status" > /dev/null 2>&1; then
      echo ""
      ok "Dremio is up (took ~$((i*5))s)"
      break
    fi
    echo -n "."
  done

# ============================================================================
# LOCAL / bare-metal mode
# ============================================================================
elif [[ "$MODE" == "local" ]]; then

  DREMIO_HOME="$TARGET"

  step "1" "Verifying Dremio installation at '$DREMIO_HOME'..."
  if [[ ! -d "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}" ]]; then
    err "Directory not found: ${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}"
    exit 1
  fi
  ok "Dremio installation found"

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "2" "Copying pre-built plugin JAR..."
    cp "$PREBUILT_JAR" "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    ok "Deployed plugin → ${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
  else
    step "2" "Building plugin from source..."
    if ! command -v mvn > /dev/null 2>&1; then
      err "Maven not found on PATH. Install Maven or use --prebuilt."
      exit 1
    fi
    cd "$SCRIPT_DIR"
    mvn package -DskipTests -q
    cp "target/${PLUGIN_JAR_NAME}" "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    cp "target/${PLUGIN_JAR_NAME}" "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null || true
    ok "Built and deployed plugin → ${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
  fi

  step "3" "Restart Dremio to load the plugin."
  warn "Automatic restart is not performed in local mode."
  info "Restart command:  \$DREMIO_HOME/bin/dremio restart"
  info "  or (systemd):  sudo systemctl restart dremio"

# ============================================================================
# KUBERNETES mode
# ============================================================================
elif [[ "$MODE" == "k8s" ]]; then

  NS_FLAG=()
  [[ -n "$NAMESPACE" ]] && NS_FLAG=(-n "$NAMESPACE")

  step "1" "Verifying pod '$TARGET'..."
  if ! kubectl get pod "${NS_FLAG[@]}" "$TARGET" > /dev/null 2>&1; then
    err "Pod '$TARGET' not found."
    exit 1
  fi
  ok "Pod found"

  step "2" "Discovering Dremio pods..."
  if [[ -n "$POD_SELECTOR" ]]; then
    SELECTOR="$POD_SELECTOR"
  else
    APP_LABEL=$(kubectl get pod "${NS_FLAG[@]}" "$TARGET" \
      -o jsonpath='{.metadata.labels.app}' 2>/dev/null || true)
    SELECTOR="${APP_LABEL:+app=$APP_LABEL}"
  fi

  if [[ -n "$SELECTOR" ]]; then
    DEPLOY_PODS=$(kubectl get pods "${NS_FLAG[@]}" -l "$SELECTOR" \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null | grep -v '^$' || true)
  fi
  [[ -z "${DEPLOY_PODS:-}" ]] && DEPLOY_PODS="$TARGET"

  LOCAL_JAR=""
  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "3" "Using pre-built plugin JAR..."
    LOCAL_JAR="$PREBUILT_JAR"
    ok "Plugin JAR ready"
  else
    step "3" "Building from source in pod '$TARGET'..."
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
      "rm -rf /tmp/redis-build && mkdir -p /tmp/redis-build"
    tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
      kubectl exec "${NS_FLAG[@]}" -i "$TARGET" -- tar -C /tmp/redis-build -xf -
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
      "cd /tmp/redis-build && mvn package -DskipTests -q --batch-mode"
    mkdir -p "${SCRIPT_DIR}/jars"
    kubectl cp "${NS_FLAG[@]}" \
      "${TARGET}:/tmp/redis-build/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
    LOCAL_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
    ok "Build successful"
  fi

  step "4" "Deploying JAR to all pods..."
  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    echo -n "    Copying to '$pod'... "
    if kubectl cp "${NS_FLAG[@]}" "$LOCAL_JAR" "${pod}:/tmp/${PLUGIN_JAR_NAME}" 2>/dev/null && \
       kubectl exec "${NS_FLAG[@]}" "$pod" -- bash -c \
         "cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} && \
          chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} 2>/dev/null || true"; then
      echo -e "${GREEN}done${RESET}"
    else
      echo -e "${RED}FAILED${RESET}"
    fi
  done <<< "$DEPLOY_PODS"

  step "5" "Restart Dremio pods to load the plugin."
  warn "Automatic restart is not performed in k8s mode."
  info "  kubectl rollout restart statefulset/<name>${NAMESPACE:+ -n $NAMESPACE}"

fi

# ============================================================================
# Done
# ============================================================================
echo ""
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${GREEN}   Installation complete!${RESET}"
echo -e "${BOLD}${GREEN}════════════════════════════════════════════════${RESET}"
echo ""
echo -e "${BOLD}Next steps:${RESET}"
echo "  1. Open http://localhost:9047  (or your Dremio URL)"
echo "  2. Go to Sources → Add Source → Redis"
echo "  3. Enter your host, port (3306), database, username, and password"
echo "  4. Click Save — your Redis tables will appear in the catalog"
echo ""
echo -e "${BOLD}Example queries:${RESET}"
echo "  SELECT * FROM redis_source.my_database.my_table LIMIT 10;"
echo "  SELECT region, COUNT(*), AVG(amount) FROM redis_source.sales.orders"
echo "    GROUP BY region ORDER BY COUNT(*) DESC;"
echo ""
