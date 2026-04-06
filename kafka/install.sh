#!/usr/bin/env bash
# ============================================================================
# Dremio Kafka Connector — Installer
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
# Kubernetes-specific options:
#   --namespace NS        Kubernetes namespace (default: current context namespace)
#   --pod-selector SEL    Label selector to find ALL Dremio pods (e.g. app=dremio)
#                         The JAR is deployed to every matching pod.
#                         If omitted, auto-detected from the target pod's 'app' label.
#
# Usage examples:
#   ./install.sh                                               # Docker, interactive
#   ./install.sh --docker try-dremio --prebuilt               # Docker, pre-built JAR
#   ./install.sh --docker try-dremio --build                  # Docker, build from source
#   ./install.sh --local /opt/dremio --prebuilt               # Bare-metal, pre-built JAR
#   ./install.sh --k8s dremio-0 --prebuilt                    # K8s, current namespace
#   ./install.sh --k8s dremio-0 --namespace dremio --prebuilt # K8s, explicit namespace
#   ./install.sh --k8s dremio-0 --namespace dremio \
#                --pod-selector app=dremio --prebuilt          # K8s, all Dremio pods
# ============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="/usr/local/bin:/usr/bin:/bin:$PATH"

PLUGIN_JAR_NAME="dremio-kafka-connector-1.0.0-SNAPSHOT-plugin.jar"
PREBUILT_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
DREMIO_JAR_SUBDIR="jars/3rdparty"

# ANSI colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

banner() {
  echo ""
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}${CYAN}   Dremio Apache Kafka Connector Installer v1${RESET}"
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
    --docker)        MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)         MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --k8s)           MODE="k8s";    TARGET="${2:-}"; shift 2 || shift ;;
    --prebuilt)      JAR_MODE="prebuilt"; shift ;;
    --build)         JAR_MODE="build"; shift ;;
    --namespace|-n)  NAMESPACE="${2:-}"; shift 2 ;;
    --pod-selector|-l) POD_SELECTOR="${2:-}"; shift 2 ;;
    -h|--help)
      echo "Usage: $0 [--docker CONTAINER | --local DREMIO_HOME | --k8s POD] [--prebuilt | --build]"
      echo "       K8s options: [--namespace NS] [--pod-selector SELECTOR]"
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
        read -rp "Kubernetes namespace [default/current context]: " NAMESPACE
      fi
      ;;
  esac
fi

if [[ -z "$JAR_MODE" ]]; then
  echo ""
  echo -e "${BOLD}How would you like to install the plugin JAR?${RESET}"
  echo ""
  echo "  1) Use pre-built JAR  — fast, no Maven required"
  echo "         Included: jars/${PLUGIN_JAR_NAME}"
  if [[ -f "$PREBUILT_JAR" ]]; then
    echo -e "         ${GREEN}(JAR found — $(du -sh "$PREBUILT_JAR" | cut -f1))${RESET}"
  else
    echo -e "         ${RED}(JAR not found in jars/ — choose option 2 to build)${RESET}"
  fi
  echo ""
  echo "  2) Build from source  — compiles inside the target environment"
  echo "         Requires: Maven (installed automatically in Docker; must exist in K8s pod)"
  echo ""
  read -rp "Choice [1]: " JAR_CHOICE
  case "${JAR_CHOICE:-1}" in
    1) JAR_MODE="prebuilt" ;;
    2) JAR_MODE="build"    ;;
    *) err "Invalid choice"; exit 1 ;;
  esac
fi

if [[ "$JAR_MODE" == "prebuilt" && ! -f "$PREBUILT_JAR" ]]; then
  err "Pre-built JAR not found at: $PREBUILT_JAR"
  err "Re-run with --build to compile from source, or add the JAR to jars/"
  exit 1
fi

echo ""
echo -e "${BOLD}Installation plan:${RESET}"
info "Mode       : $MODE"
info "Target     : $TARGET"
info "JAR source : $JAR_MODE"
[[ "$MODE" == "k8s" && -n "$NAMESPACE" ]] && info "Namespace  : $NAMESPACE"
[[ "$MODE" == "k8s" && -n "$POD_SELECTOR" ]] && info "Pod selector: $POD_SELECTOR"
echo ""
read -rp "Proceed? [Y/n]: " CONFIRM || CONFIRM="Y"
[[ "${CONFIRM:-Y}" =~ ^[Yy]$ ]] || { echo "Aborted."; exit 0; }
echo ""

# ============================================================================
# Helper: build from source inside a Docker container (Docker mode only)
# ============================================================================
build_in_docker() {
  local container_name="$1"

  step "B1" "Checking for Maven in container..."
  if ! docker exec "$container_name" which mvn > /dev/null 2>&1; then
    step "B2" "Installing Maven (one-time setup)..."
    docker exec -u root "$container_name" bash -c \
      "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -3"
    ok "Maven installed"
  else
    ok "Maven already present"
  fi

  step "B3" "Installing Dremio JARs into Maven local repo..."
  docker exec "$container_name" bash -c '
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
JARS=/opt/dremio/jars
TP=$JARS/3rdparty

install_jar() {
  mvn install:install-file -Dfile="$1" -DgroupId="$2" -DartifactId="$3" \
    -Dversion="$4" -Dpackaging=jar -q 2>/dev/null && echo "  installed $3" || true
}

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar         com.dremio        dremio-sabot-kernel         $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar               com.dremio        dremio-common               $DREMIO_VER
install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar        com.dremio.plugin dremio-plugin-common        $DREMIO_VER
install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar   com.dremio        dremio-sabot-kernel-proto   $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar   com.dremio        dremio-sabot-vector-tools   $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar   com.dremio        dremio-services-namespace   $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar            com.dremio        dremio-connector            $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar        com.dremio        dremio-sabot-logical        $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar          com.dremio        dremio-common-core          $DREMIO_VER
install_jar $JARS/dremio-services-credentials-${DREMIO_VER}.jar com.dremio.services dremio-services-credentials $DREMIO_VER
install_jar $JARS/dremio-services-datastore-${DREMIO_VER}.jar   com.dremio        dremio-services-datastore   $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                   org.apache.arrow  arrow-vector                $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar              org.apache.arrow  arrow-memory-core           $ARROW_VER
install_jar $TP/arrow-format-${ARROW_VER}.jar                   org.apache.arrow  arrow-format                $ARROW_VER
install_jar $TP/guava-33.4.0-jre.jar                            com.google.guava  guava                       33.4.0-jre
install_jar $TP/javax.inject-1.jar                              javax.inject      javax.inject                1

CALCITE_VER=$(ls $TP/calcite-core-*.jar 2>/dev/null | head -1 | sed "s/.*calcite-core-//;s/\.jar//")
if [ -n "$CALCITE_VER" ]; then
  install_jar $TP/calcite-core-${CALCITE_VER}.jar   org.apache.calcite calcite-core   $CALCITE_VER
  install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar org.apache.calcite calcite-linq4j $CALCITE_VER
fi
'
  ok "Dremio JARs installed in Maven repo"

  step "B4" "Copying source and building connector..."
  docker exec -u root "$container_name" bash -c "rm -rf /tmp/kafka-build && mkdir -p /tmp/kafka-build"
  docker cp "${SCRIPT_DIR}/." "${container_name}:/tmp/kafka-build/"
  docker exec -u root "$container_name" bash -c "
    chmod -R a+rX /tmp/kafka-build
    rm -rf /tmp/kafka-build/target /tmp/kafka-build/jars
  "
  docker exec "$container_name" bash -c "
    cd /tmp/kafka-build && mvn package -DskipTests -q --batch-mode
    echo 'Build complete.'
    ls -lh target/${PLUGIN_JAR_NAME}
  "
  docker cp "${container_name}:/tmp/kafka-build/target/${PLUGIN_JAR_NAME}" \
    "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null && \
    ok "Built JAR saved to jars/ for future --prebuilt installs"
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

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "2" "Copying pre-built JAR into container..."
    docker cp "$PREBUILT_JAR" "${TARGET}:/tmp/${PLUGIN_JAR_NAME}"
    ok "JAR copied"
    DEPLOY_FROM="/tmp/${PLUGIN_JAR_NAME}"
  else
    step "2" "Building from source..."
    build_in_docker "$TARGET"
    DEPLOY_FROM="/tmp/kafka-build/target/${PLUGIN_JAR_NAME}"
  fi

  step "3" "Deploying JAR to ${DREMIO_JAR_SUBDIR}..."
  docker exec -u root "$TARGET" bash -c "
    cp ${DEPLOY_FROM} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}
    chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} 2>/dev/null || true
  "
  ok "Deployed to /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"

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
    step "2" "Copying pre-built JAR..."
    cp "$PREBUILT_JAR" "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    ok "Deployed to ${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
  else
    step "2" "Building from source (requires Maven on PATH)..."
    if ! which mvn > /dev/null 2>&1; then
      err "Maven not found on PATH. Install Maven or use --prebuilt."
      exit 1
    fi
    cd "$SCRIPT_DIR"
    mvn package -DskipTests -q
    cp "target/${PLUGIN_JAR_NAME}" "${DREMIO_HOME}/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME}"
    cp "target/${PLUGIN_JAR_NAME}" "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null || true
    ok "Built and deployed"
  fi

  step "3" "Restart Dremio to load the plugin."
  warn "Automatic restart is not performed in local mode."
  info "Restart command:  \$DREMIO_HOME/bin/dremio restart"

# ============================================================================
# KUBERNETES mode
# ============================================================================
elif [[ "$MODE" == "k8s" ]]; then

  # Build namespace flag array — empty if no namespace specified
  NS_FLAG=()
  [[ -n "$NAMESPACE" ]] && NS_FLAG=(-n "$NAMESPACE")
  NS_LABEL="${NAMESPACE:+ in namespace '$NAMESPACE'}"

  # ── Step 1: Verify target pod ──────────────────────────────────────────────
  step "1" "Verifying pod '$TARGET'${NS_LABEL}..."
  if ! kubectl get pod "${NS_FLAG[@]}" "$TARGET" > /dev/null 2>&1; then
    err "Pod '$TARGET' not found${NS_LABEL}."
    info "List pods: kubectl get pods${NAMESPACE:+ -n $NAMESPACE}"
    exit 1
  fi
  ok "Pod found"

  # ── Step 2: Discover all pods to deploy to ────────────────────────────────
  step "2" "Discovering Dremio pods to deploy to..."

  if [[ -n "$POD_SELECTOR" ]]; then
    SELECTOR="$POD_SELECTOR"
    info "Using provided pod selector: $SELECTOR"
  else
    # Auto-detect from the target pod's 'app' label
    APP_LABEL=$(kubectl get pod "${NS_FLAG[@]}" "$TARGET" \
      -o jsonpath='{.metadata.labels.app}' 2>/dev/null || true)
    if [[ -n "$APP_LABEL" ]]; then
      SELECTOR="app=${APP_LABEL}"
      info "Auto-detected pod selector: $SELECTOR"
    else
      SELECTOR=""
      warn "Could not auto-detect pod selector from pod labels."
      warn "Deploying to '$TARGET' only. Use --pod-selector to target all pods."
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

  # ── Step 3: Obtain JAR ────────────────────────────────────────────────────
  LOCAL_JAR=""

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    step "3" "Using pre-built JAR ($(du -sh "$PREBUILT_JAR" | cut -f1))..."
    LOCAL_JAR="$PREBUILT_JAR"
    ok "JAR ready"

  else
    step "3" "Building from source in pod '$TARGET'..."

    # Check Maven availability
    if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- which mvn > /dev/null 2>&1; then
      step "3a" "Maven not found — attempting install (requires root in pod)..."
      if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
           "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -3" 2>/dev/null; then
        err "Maven not found and install failed."
        info "The Dremio pod may not be running as root."
        info ""
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
JARS=/opt/dremio/jars
TP=$JARS/3rdparty

install_jar() {
  mvn install:install-file -Dfile="$1" -DgroupId="$2" -DartifactId="$3" \
    -Dversion="$4" -Dpackaging=jar -q 2>/dev/null && echo "  installed $3" || true
}

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar         com.dremio        dremio-sabot-kernel         $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar               com.dremio        dremio-common               $DREMIO_VER
install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar        com.dremio.plugin dremio-plugin-common        $DREMIO_VER
install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar   com.dremio        dremio-sabot-kernel-proto   $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar   com.dremio        dremio-sabot-vector-tools   $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar   com.dremio        dremio-services-namespace   $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar            com.dremio        dremio-connector            $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar        com.dremio        dremio-sabot-logical        $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar          com.dremio        dremio-common-core          $DREMIO_VER
install_jar $JARS/dremio-services-credentials-${DREMIO_VER}.jar com.dremio.services dremio-services-credentials $DREMIO_VER
install_jar $JARS/dremio-services-datastore-${DREMIO_VER}.jar   com.dremio        dremio-services-datastore   $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                   org.apache.arrow  arrow-vector                $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar              org.apache.arrow  arrow-memory-core           $ARROW_VER
install_jar $TP/arrow-format-${ARROW_VER}.jar                   org.apache.arrow  arrow-format                $ARROW_VER
install_jar $TP/guava-33.4.0-jre.jar                            com.google.guava  guava                       33.4.0-jre
install_jar $TP/javax.inject-1.jar                              javax.inject      javax.inject                1

CALCITE_VER=$(ls $TP/calcite-core-*.jar 2>/dev/null | head -1 | sed "s/.*calcite-core-//;s/\.jar//")
if [ -n "$CALCITE_VER" ]; then
  install_jar $TP/calcite-core-${CALCITE_VER}.jar   org.apache.calcite calcite-core   $CALCITE_VER
  install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar org.apache.calcite calcite-linq4j $CALCITE_VER
fi
'
    ok "Dremio JARs installed"

    step "3c" "Copying source into pod and building..."
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
      "rm -rf /tmp/kafka-build && mkdir -p /tmp/kafka-build"
    tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
      kubectl exec "${NS_FLAG[@]}" -i "$TARGET" -- tar -C /tmp/kafka-build -xf -
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
      "cd /tmp/kafka-build && mvn package -DskipTests -q --batch-mode"
    ok "Build successful"

    step "3d" "Extracting built JAR to local jars/ (for future --prebuilt installs)..."
    kubectl cp "${NS_FLAG[@]}" \
      "${TARGET}:/tmp/kafka-build/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
    ok "JAR saved to jars/${PLUGIN_JAR_NAME}"
    LOCAL_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
  fi

  # ── Step 4: Deploy JAR to every discovered pod ────────────────────────────
  step "4" "Deploying JAR to all pods..."
  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    echo -n "    Copying to '$pod'... "
    if kubectl cp "${NS_FLAG[@]}" \
         "$LOCAL_JAR" "${pod}:/tmp/${PLUGIN_JAR_NAME}" 2>/dev/null && \
       kubectl exec "${NS_FLAG[@]}" "$pod" -- bash -c \
         "cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} && \
          chown dremio:dremio /opt/dremio/${DREMIO_JAR_SUBDIR}/${PLUGIN_JAR_NAME} 2>/dev/null || true"; then
      echo -e "${GREEN}done${RESET}"
    else
      echo -e "${RED}FAILED${RESET}"
      warn "Could not deploy to pod '$pod' — it may not be running. Continuing..."
    fi
  done <<< "$DEPLOY_PODS"

  # ── Step 5: Restart instructions ──────────────────────────────────────────
  step "5" "Restart Dremio pods to load the plugin."
  warn "Automatic restart is not performed in k8s mode."
  echo ""

  # Try to find the StatefulSet name for a better restart command
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
  info "Option B — immediate restart (brief downtime, all pods restart at once):"
  if [[ -n "$SELECTOR" ]]; then
    info "  kubectl delete pods${NAMESPACE:+ -n $NAMESPACE} -l $SELECTOR"
  else
    info "  kubectl delete pod${NAMESPACE:+ -n $NAMESPACE} $TARGET"
  fi
  echo ""
  warn "IMPORTANT: The JAR is written to the running pod's filesystem."
  warn "If the pod is replaced by its StatefulSet (node drain, OOM kill, etc.),"
  warn "the new pod will start without the JAR. Re-run this installer after"
  warn "any pod replacement, or mount the JAR via a PersistentVolumeClaim."

fi

echo ""
echo -e "${GREEN}${BOLD}Installation complete!${RESET}"
echo ""
echo "  Next: In Dremio UI → Sources → + → Apache Kafka"
echo "  Then run: ./test-connection.sh --source <source-name> --topic <topic>"
echo ""
