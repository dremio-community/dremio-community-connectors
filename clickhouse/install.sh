#!/bin/bash
# ============================================================================
# Dremio ClickHouse Connector — Installer
#
# Supports three deployment modes:
#   --docker   [container]   Deploy into a running Docker container (default)
#   --local    [dremio-home] Deploy to a local bare-metal Dremio installation
#   --k8s      [pod]         Deploy into a running Kubernetes pod
#
# For each mode you can choose how to obtain the plugin JAR:
#   --prebuilt    Use the JAR included in this package (no Maven required)
#   --build       Build from source inside the target environment
#
# For the ClickHouse JDBC driver JAR you can choose:
#   --driver-included    Use the pre-patched driver JAR from jars/ (default)
#   --driver-fetch       Download from Maven Central and patch at install time
#
# Usage examples:
#   ./install.sh                                              # fully interactive
#   ./install.sh --docker try-dremio --prebuilt              # Docker, pre-built plugin, included driver
#   ./install.sh --docker try-dremio --prebuilt --driver-fetch  # Docker, download driver fresh
#   ./install.sh --local /opt/dremio --prebuilt              # bare-metal, pre-built
#   ./install.sh --k8s dremio-0 --prebuilt                   # Kubernetes, pre-built
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Ensure common tool locations are on PATH (handles macOS with Docker Desktop)
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PLUGIN_JAR_NAME="dremio-clickhouse-connector-1.0.0-SNAPSHOT-plugin.jar"
PREBUILT_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
DREMIO_JAR_SUBDIR="jars/3rdparty"

DRIVER_JAR_NAME="clickhouse-jdbc-0.4.6-all.jar"
DRIVER_MAVEN_URL="https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar"
PREBUILT_DRIVER_JAR="${SCRIPT_DIR}/jars/${DRIVER_JAR_NAME}"

# ANSI colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

banner() {
  echo ""
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}${CYAN}   Dremio ClickHouse Connector Installer${RESET}"
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
MODE=""          # docker | local | k8s
TARGET=""        # container name | dremio home | pod name
JAR_MODE=""      # prebuilt | build
DRIVER_MODE=""   # included | fetch
NAMESPACE=""     # K8s namespace (empty = use current context namespace)
POD_SELECTOR=""  # K8s label selector to find all Dremio pods

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)          MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)           MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --k8s)             MODE="k8s";    TARGET="${2:-}"; shift 2 || shift ;;
    --prebuilt)        JAR_MODE="prebuilt"; shift ;;
    --build)           JAR_MODE="build"; shift ;;
    --driver-included) DRIVER_MODE="included"; shift ;;
    --driver-fetch)    DRIVER_MODE="fetch"; shift ;;
    --namespace|-n)    NAMESPACE="${2:-}"; shift 2 ;;
    --pod-selector|-l) POD_SELECTOR="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--docker CONTAINER | --local DREMIO_HOME | --k8s POD]"
      echo "          [--prebuilt | --build] [--driver-included | --driver-fetch]"
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
  echo "  1) Use pre-built JAR  — fast, no Maven required (~5 seconds)"
  echo "         Included: jars/${PLUGIN_JAR_NAME}"
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

if [[ -z "$DRIVER_MODE" ]]; then
  echo ""
  echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  echo -e "${BOLD}ClickHouse JDBC Driver${RESET}"
  echo ""
  echo -e "  This connector requires the official ClickHouse JDBC driver"
  echo -e "  (${DRIVER_JAR_NAME}, Apache 2.0 license)."
  echo ""
  echo -e "  One small patch is applied before deployment: the file"
  echo -e "  ${CYAN}META-INF/services/java.sql.Driver${RESET} is removed from the JAR."
  echo -e "  Without this, Java's ServiceLoader auto-registers the driver"
  echo -e "  during JVM startup on a background thread, causing a class-"
  echo -e "  initialization race condition inside Dremio's DBCP2 connection"
  echo -e "  pool that permanently blocks all future connection attempts."
  echo -e "  The patch has no effect outside of Dremio."
  echo ""
  echo -e "  How would you like to obtain the driver?"
  echo ""
  echo "  1) Use the pre-patched JAR included in jars/  [recommended]"
  echo "         Fast (~2 seconds), no internet access required."
  echo "         The JAR in this repo is the official Maven Central artifact"
  echo "         with only that one SPI entry removed — nothing else changed."
  if [[ -f "$PREBUILT_DRIVER_JAR" ]]; then
    echo -e "         ${GREEN}(Found: jars/${DRIVER_JAR_NAME})${RESET}"
  else
    echo -e "         ${YELLOW}(Not found in jars/ — choose option 2)${RESET}"
  fi
  echo ""
  echo "  2) Download from Maven Central and patch at install time"
  echo "         Fetches the official JAR directly from:"
  echo "         ${DRIVER_MAVEN_URL}"
  echo "         Then removes the SPI entry here on your machine before"
  echo "         deploying. Requires: curl, zip."
  echo ""
  echo -e "${BOLD}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${RESET}"
  read -rp "Choice [1]: " DRIVER_CHOICE
  case "${DRIVER_CHOICE:-1}" in
    1) DRIVER_MODE="included" ;;
    2) DRIVER_MODE="fetch"    ;;
    *) err "Invalid choice"; exit 1 ;;
  esac
fi

# Validate plugin JAR
if [[ "$JAR_MODE" == "prebuilt" && ! -f "$PREBUILT_JAR" ]]; then
  err "Pre-built plugin JAR not found at: $PREBUILT_JAR"
  err "Re-run with --build to compile from source, or add the JAR to jars/"
  exit 1
fi

# Validate / fetch driver JAR
if [[ "$DRIVER_MODE" == "included" ]]; then
  if [[ ! -f "$PREBUILT_DRIVER_JAR" ]]; then
    err "Pre-patched driver JAR not found at: $PREBUILT_DRIVER_JAR"
    err "Re-run with --driver-fetch to download and patch from Maven Central."
    exit 1
  fi
  DRIVER_JAR_PATH="$PREBUILT_DRIVER_JAR"
else
  # fetch mode — download and patch on the host before deploying
  step "D1" "Checking prerequisites for driver fetch (curl, zip)..."
  if ! command -v curl > /dev/null 2>&1; then
    err "curl is required for --driver-fetch but was not found on PATH."
    exit 1
  fi
  if ! command -v zip > /dev/null 2>&1; then
    err "zip is required for --driver-fetch but was not found on PATH."
    exit 1
  fi
  ok "curl and zip found"

  DRIVER_TMP="/tmp/${DRIVER_JAR_NAME}"
  step "D2" "Downloading ${DRIVER_JAR_NAME} from Maven Central..."
  info "Source: ${DRIVER_MAVEN_URL}"
  curl -fL --progress-bar "$DRIVER_MAVEN_URL" -o "$DRIVER_TMP"
  ok "Downloaded to ${DRIVER_TMP} ($(du -sh "$DRIVER_TMP" | cut -f1))"

  step "D3" "Patching driver JAR (removing SPI auto-registration entry)..."
  info "Removing: META-INF/services/java.sql.Driver"
  zip -d "$DRIVER_TMP" "META-INF/services/java.sql.Driver" > /dev/null 2>&1 && \
    ok "SPI entry removed" || \
    warn "SPI entry not found in JAR (may already be absent — continuing)"

  DRIVER_JAR_PATH="$DRIVER_TMP"
fi

echo ""
echo -e "${BOLD}Installation plan:${RESET}"
info "Mode          : $MODE"
info "Target        : $TARGET"
info "Plugin JAR    : $JAR_MODE"
info "Driver JAR    : $DRIVER_MODE (${DRIVER_JAR_NAME})"
[[ "$MODE" == "k8s" && -n "$NAMESPACE" ]] && info "Namespace     : $NAMESPACE"
[[ "$MODE" == "k8s" && -n "$POD_SELECTOR" ]] && info "Pod selector  : $POD_SELECTOR"
echo ""
read -rp "Proceed? [Y/n]: " CONFIRM || CONFIRM="Y"
[[ "${CONFIRM:-Y}" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# ============================================================================
# Helper: build plugin from source inside the container / pod
# ============================================================================
build_in_container() {
  local exec_cmd="$1"   # e.g. "docker exec CONTAINER" or "kubectl exec POD --"

  step "B1" "Checking for Maven in target environment..."
  if ! $exec_cmd which mvn > /dev/null 2>&1; then
    step "B2" "Installing Maven (one-time setup)..."
    $exec_cmd bash -c "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -3"
    ok "Maven installed"
  else
    ok "Maven already present"
  fi

  step "B3" "Installing Dremio JARs into Maven local repo..."
  $exec_cmd bash -c '
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
HADOOP_VER=3.3.6-dremio-202507241551560856-75923ad5
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
install_jar $TP/hadoop-common-${HADOOP_VER}.jar                 org.apache.hadoop hadoop-common        $HADOOP_VER
install_jar $TP/guava-33.4.0-jre.jar                            com.google.guava guava                 33.4.0-jre
install_jar $TP/javax.inject-1.jar                              javax.inject javax.inject              1
install_jar $TP/protobuf-java-3.25.5.jar                        com.google.protobuf protobuf-java       3.25.5

install_jar $JARS/dremio-ce-jdbc-plugin-${DREMIO_VER}.jar         com.dremio.plugins dremio-ce-jdbc-plugin      $DREMIO_VER
install_jar $JARS/dremio-ce-jdbc-fetcher-api-${DREMIO_VER}.jar    com.dremio.plugins dremio-ce-jdbc-fetcher-api $DREMIO_VER

CALCITE_VER=$(ls $TP/calcite-core-*.jar 2>/dev/null | head -1 | sed "s/.*calcite-core-//;s/\.jar//")
if [ -n "$CALCITE_VER" ]; then
  install_jar $TP/calcite-core-${CALCITE_VER}.jar   org.apache.calcite calcite-core   $CALCITE_VER
  install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar org.apache.calcite calcite-linq4j $CALCITE_VER
fi
'
  ok "Dremio JARs installed in Maven repo"

  step "B4" "Copying source and building connector..."
  if [[ "$exec_cmd" == docker* ]]; then
    local container_name
    container_name=$(echo "$exec_cmd" | awk '{print $3}')
    docker exec -u root "${container_name}" bash -c "rm -rf /tmp/clickhouse-build && mkdir -p /tmp/clickhouse-build"
    docker cp "${SCRIPT_DIR}/." "${container_name}:/tmp/clickhouse-build/"
    docker exec -u root "${container_name}" bash -c "rm -rf /tmp/clickhouse-build/target && chown -R dremio:dremio /tmp/clickhouse-build"
    docker exec "${container_name}" bash -c "
      cd /tmp/clickhouse-build && mvn package -DskipTests -q
      echo 'Build complete.'
      ls -lh target/${PLUGIN_JAR_NAME}
    "
    docker cp "${container_name}:/tmp/clickhouse-build/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null && \
      ok "Built JAR saved to jars/ for future --prebuilt installs"
    DEPLOY_FROM="/tmp/clickhouse-build/target/${PLUGIN_JAR_NAME}"
  else
    tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
      kubectl exec -i "$TARGET" -- tar -C /tmp/clickhouse-build -xf -
    $exec_cmd bash -c "cd /tmp/clickhouse-build && mvn package -DskipTests -q"
    DEPLOY_FROM="/tmp/clickhouse-build/target/${PLUGIN_JAR_NAME}"
  fi
  ok "Build successful"
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
    build_in_container "docker exec $TARGET"
  fi

  step "3" "Copying driver JAR into container..."
  docker cp "$DRIVER_JAR_PATH" "${TARGET}:/tmp/${DRIVER_JAR_NAME}"
  ok "Driver JAR copied"

  step "4" "Deploying JARs to ${DREMIO_JAR_SUBDIR}..."
  docker exec -u root "$TARGET" bash -c "
    cp ${DEPLOY_FROM} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
    cp /tmp/${DRIVER_JAR_NAME} /opt/dremio/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME}
    chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
    chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME}
  "
  ok "Deployed plugin  → /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
  ok "Deployed driver  → /opt/dremio/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME}"

  step "5" "Restarting Dremio container..."
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
    err "Is DREMIO_HOME correct? Expected layout: \$DREMIO_HOME/jars/3rdparty/"
    exit 1
  fi
  ok "Dremio installation found"

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "2" "Copying pre-built plugin JAR..."
    cp "$PREBUILT_JAR" "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    ok "Deployed plugin → ${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
  else
    step "2" "Building plugin from source (requires Maven on PATH)..."
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

  step "3" "Deploying driver JAR..."
  cp "$DRIVER_JAR_PATH" "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME}"
  ok "Deployed driver → ${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME}"

  step "4" "Restart Dremio to load the plugin."
  warn "Automatic restart is not performed in local mode."
  info "Restart command:  \$DREMIO_HOME/bin/dremio restart"
  info "  or (systemd):  sudo systemctl restart dremio"

# ============================================================================
# KUBERNETES mode
# ============================================================================
elif [[ "$MODE" == "k8s" ]]; then

  NS_FLAG=()
  [[ -n "$NAMESPACE" ]] && NS_FLAG=(-n "$NAMESPACE")
  NS_LABEL="${NAMESPACE:+ in namespace '$NAMESPACE'}"

  step "1" "Verifying pod '$TARGET'${NS_LABEL}..."
  if ! kubectl get pod "${NS_FLAG[@]}" "$TARGET" > /dev/null 2>&1; then
    err "Pod '$TARGET' not found${NS_LABEL}."
    info "List pods: kubectl get pods${NAMESPACE:+ -n $NAMESPACE}"
    exit 1
  fi
  ok "Pod found"

  step "2" "Discovering Dremio pods to deploy to..."
  if [[ -n "$POD_SELECTOR" ]]; then
    SELECTOR="$POD_SELECTOR"
    info "Using provided pod selector: $SELECTOR"
  else
    APP_LABEL=$(kubectl get pod "${NS_FLAG[@]}" "$TARGET" \
      -o jsonpath='{.metadata.labels.app}' 2>/dev/null || true)
    if [[ -n "$APP_LABEL" ]]; then
      SELECTOR="app=${APP_LABEL}"
      info "Auto-detected pod selector: $SELECTOR"
    else
      SELECTOR=""
      warn "Could not auto-detect pod selector. Deploying to '$TARGET' only."
      warn "Use --pod-selector to target all pods (coordinator + executors)."
    fi
  fi

  if [[ -n "$SELECTOR" ]]; then
    DEPLOY_PODS=$(kubectl get pods "${NS_FLAG[@]}" -l "$SELECTOR" \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
      | grep -v '^$' || true)
  fi
  [[ -z "${DEPLOY_PODS:-}" ]] && DEPLOY_PODS="$TARGET"

  POD_COUNT=$(echo "$DEPLOY_PODS" | grep -c '[^[:space:]]' || true)
  ok "Will deploy to $POD_COUNT pod(s):"
  while IFS= read -r p; do [[ -n "$p" ]] && info "$p"; done <<< "$DEPLOY_PODS"
  echo ""

  LOCAL_JAR=""

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "3" "Using pre-built plugin JAR ($(du -sh "$PREBUILT_JAR" | cut -f1))..."
    LOCAL_JAR="$PREBUILT_JAR"
    ok "Plugin JAR ready"
  else
    step "3" "Building plugin from source in pod '$TARGET'..."
    if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- which mvn > /dev/null 2>&1; then
      step "3a" "Maven not found — attempting install (requires root in pod)..."
      if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
           "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -3" 2>/dev/null; then
        err "Maven not found and install failed."
        info "Recommended fix: build locally and use --prebuilt:"
        info "  mvn package -DskipTests  (on your local machine with Maven)"
        info "  ./install.sh --k8s $TARGET ${NAMESPACE:+--namespace $NAMESPACE }--prebuilt"
        exit 1
      fi
      ok "Maven installed"
    else
      ok "Maven available in pod"
    fi

    step "3b" "Installing Dremio JARs into Maven local repo..."
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c '
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
HADOOP_VER=3.3.6-dremio-202507241551560856-75923ad5
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
install_jar $TP/hadoop-common-${HADOOP_VER}.jar                 org.apache.hadoop hadoop-common        $HADOOP_VER
install_jar $TP/guava-33.4.0-jre.jar                            com.google.guava guava                 33.4.0-jre
install_jar $TP/javax.inject-1.jar                              javax.inject javax.inject              1
install_jar $TP/protobuf-java-3.25.5.jar                        com.google.protobuf protobuf-java       3.25.5
install_jar $JARS/dremio-ce-jdbc-plugin-${DREMIO_VER}.jar       com.dremio.plugins dremio-ce-jdbc-plugin      $DREMIO_VER
install_jar $JARS/dremio-ce-jdbc-fetcher-api-${DREMIO_VER}.jar  com.dremio.plugins dremio-ce-jdbc-fetcher-api $DREMIO_VER

CALCITE_VER=$(ls $TP/calcite-core-*.jar 2>/dev/null | head -1 | sed "s/.*calcite-core-//;s/\.jar//")
if [ -n "$CALCITE_VER" ]; then
  install_jar $TP/calcite-core-${CALCITE_VER}.jar   org.apache.calcite calcite-core   $CALCITE_VER
  install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar org.apache.calcite calcite-linq4j $CALCITE_VER
fi
'
    ok "Dremio JARs installed"

    step "3c" "Copying source into pod and building..."
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
      "rm -rf /tmp/clickhouse-build && mkdir -p /tmp/clickhouse-build"
    tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
      kubectl exec "${NS_FLAG[@]}" -i "$TARGET" -- tar -C /tmp/clickhouse-build -xf -
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
      "cd /tmp/clickhouse-build && mvn package -DskipTests -q --batch-mode"
    ok "Build successful"

    step "3d" "Extracting built plugin JAR to local jars/..."
    kubectl cp "${NS_FLAG[@]}" \
      "${TARGET}:/tmp/clickhouse-build/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
    ok "JAR saved to jars/${PLUGIN_JAR_NAME}"
    LOCAL_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
  fi

  step "4" "Deploying JARs to all pods..."
  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    echo -n "    Copying to '$pod'... "
    if kubectl cp "${NS_FLAG[@]}" \
         "$LOCAL_JAR" "${pod}:/tmp/${PLUGIN_JAR_NAME}" 2>/dev/null && \
       kubectl cp "${NS_FLAG[@]}" \
         "$DRIVER_JAR_PATH" "${pod}:/tmp/${DRIVER_JAR_NAME}" 2>/dev/null && \
       kubectl exec "${NS_FLAG[@]}" "$pod" -- bash -c \
         "cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} && \
          cp /tmp/${DRIVER_JAR_NAME} /opt/dremio/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME} && \
          chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} \
                              /opt/dremio/${DREMIO_JAR_SUBDIR}/${DRIVER_JAR_NAME} 2>/dev/null || true"; then
      echo -e "${GREEN}done${RESET}"
    else
      echo -e "${RED}FAILED${RESET}"
      warn "Could not deploy to pod '$pod' — it may not be running. Continuing..."
    fi
  done <<< "$DEPLOY_PODS"

  step "5" "Restart Dremio pods to load the plugin."
  warn "Automatic restart is not performed in k8s mode."
  echo ""
  SS_NAME=""
  if [[ -n "$SELECTOR" ]]; then
    SS_NAME=$(kubectl get statefulset "${NS_FLAG[@]}" -l "$SELECTOR" \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  fi
  info "Option A — rolling restart (zero downtime if you have replicas):"
  if [[ -n "$SS_NAME" ]]; then
    info "  kubectl rollout restart statefulset/${SS_NAME}${NAMESPACE:+ -n $NAMESPACE}"
  else
    info "  kubectl rollout restart statefulset/<your-statefulset>${NAMESPACE:+ -n $NAMESPACE}"
  fi
  echo ""
  info "Option B — immediate restart (brief downtime):"
  if [[ -n "$SELECTOR" ]]; then
    info "  kubectl delete pods${NAMESPACE:+ -n $NAMESPACE} -l $SELECTOR"
  else
    info "  kubectl delete pod${NAMESPACE:+ -n $NAMESPACE} $TARGET"
  fi
  echo ""
  warn "IMPORTANT: The JARs are written to the running pod's filesystem."
  warn "If pods are replaced by their StatefulSet, re-run this installer."

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
echo "  2. Go to Sources → Add Source → ClickHouse"
echo "  3. Enter your host, port (8123), database, username, and password"
echo "  4. Click Save — your ClickHouse tables will appear in the catalog"
echo ""
echo -e "${BOLD}Example queries:${RESET}"
echo "  SELECT * FROM clickhouse_source.my_database.my_table LIMIT 10;"
echo "  SELECT region, COUNT(*), AVG(amount) FROM clickhouse_source.analytics.events"
echo "    GROUP BY region ORDER BY COUNT(*) DESC;"
echo ""
echo -e "${BOLD}Note:${RESET} ClickHouse uses port 8123 (HTTP) by default."
echo "  For HTTPS, use port 8443 and enable SSL in the source settings."
echo ""
