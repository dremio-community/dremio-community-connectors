#!/bin/bash
# ============================================================================
# Dremio Amazon DynamoDB Connector — Installer
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

PLUGIN_JAR_NAME="dremio-dynamodb-connector-1.0.0-SNAPSHOT-plugin.jar"
PREBUILT_JAR="${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
DREMIO_JAR_SUBDIR="jars/3rdparty"

# ANSI colours
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; BOLD='\033[1m'; RESET='\033[0m'

banner() {
  echo ""
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════${RESET}"
  echo -e "${BOLD}${CYAN}   Dremio Amazon DynamoDB Connector Installer${RESET}"
  echo -e "${BOLD}${CYAN}════════════════════════════════════════════════${RESET}"
  echo ""
}

ok()   { echo -e "${GREEN}✓${RESET} $*"; }
warn() { echo -e "${YELLOW}⚠${RESET} $*"; }
err()  { echo -e "${RED}✗${RESET} $*"; exit 1; }
info() { echo -e "${CYAN}→${RESET} $*"; }

# ── Argument parsing ─────────────────────────────────────────────────────────
MODE=""       # docker | local | k8s
TARGET=""     # container name | dremio home | pod name
JAR_MODE=""   # prebuilt | build

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)  MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 ;;
    --local)   MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 ;;
    --k8s)     MODE="k8s";    TARGET="${2:-}"; shift 2 ;;
    --prebuilt) JAR_MODE="prebuilt"; shift ;;
    --build)    JAR_MODE="build";    shift ;;
    -h|--help)
      echo "Usage: $0 [--docker CONTAINER | --local DREMIO_HOME | --k8s POD] [--prebuilt | --build]"
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

banner

# ── Interactive mode picker ──────────────────────────────────────────────────
if [[ -z "$MODE" ]]; then
  echo "Select deployment mode:"
  echo "  1) Docker container  (default: try-dremio)"
  echo "  2) Bare-metal / VM"
  echo "  3) Kubernetes pod"
  read -rp "Choice [1]: " choice
  case "${choice:-1}" in
    1) MODE="docker"
       read -rp "Container name [try-dremio]: " TARGET
       TARGET="${TARGET:-try-dremio}" ;;
    2) MODE="local"
       read -rp "Dremio home [/opt/dremio]: " TARGET
       TARGET="${TARGET:-/opt/dremio}" ;;
    3) MODE="k8s"
       read -rp "Pod name: " TARGET ;;
    *) err "Invalid choice" ;;
  esac
fi

if [[ -z "$JAR_MODE" ]]; then
  echo ""
  echo "Select JAR source:"
  echo "  1) Pre-built JAR from jars/ (no Maven required)"
  echo "  2) Build from source inside the target environment"
  read -rp "Choice [1]: " jchoice
  case "${jchoice:-1}" in
    1) JAR_MODE="prebuilt" ;;
    2) JAR_MODE="build" ;;
    *) err "Invalid choice" ;;
  esac
fi

# ── Helper: detect Dremio JAR dir ────────────────────────────────────────────
detect_jar_dir_docker() {
  local container="$1"
  docker exec "$container" find /opt/dremio/jars/3rdparty -maxdepth 0 -type d 2>/dev/null \
    && echo "/opt/dremio/jars/3rdparty" || echo "/opt/dremio/jars/3rdparty"
}

# ── Build from source ────────────────────────────────────────────────────────
build_in_docker() {
  local container="$1"
  info "Building from source inside container '$container'…"

  # Check for Maven
  if ! docker exec "$container" which mvn &>/dev/null; then
    info "Installing Maven inside container…"
    docker exec -u root "$container" bash -c "
      apt-get update -q && apt-get install -y -q maven 2>/dev/null || \
      (curl -s https://archive.apache.org/dist/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz \
        | tar -xz -C /opt && ln -sf /opt/apache-maven-3.9.6/bin/mvn /usr/local/bin/mvn)
    "
  fi

  # Copy source into container
  local build_dir="/tmp/dynamodb-build-$(date +%s)"
  docker exec "$container" mkdir -p "$build_dir"
  docker cp "${SCRIPT_DIR}/pom.xml" "${container}:${build_dir}/pom.xml"
  docker cp "${SCRIPT_DIR}/src"     "${container}:${build_dir}/src"

  # Install Dremio JARs + build
  docker exec "$container" bash -c "
    set -e
    cd '${build_dir}'
    DREMIO_VER=\$(ls /opt/dremio/jars/dremio-common-*.jar 2>/dev/null | head -1 | sed 's/.*dremio-common-//;s/\\.jar//')
    ARROW_VER=\$(ls /opt/dremio/jars/3rdparty/arrow-vector-*.jar 2>/dev/null | head -1 | sed 's/.*arrow-vector-//;s/\\.jar//')
    CALCITE_VER=\$(ls /opt/dremio/jars/3rdparty/calcite-core-*.jar 2>/dev/null | head -1 | sed 's/.*calcite-core-//;s/\\.jar//')
    JARS=/opt/dremio/jars
    TP=\$JARS/3rdparty
    install() { mvn install:install-file -Dfile=\"\$1\" -DgroupId=\"\$2\" -DartifactId=\"\$3\" -Dversion=\"\$4\" -Dpackaging=jar -q; }
    install \$JARS/dremio-sabot-kernel-\${DREMIO_VER}.jar       com.dremio          dremio-sabot-kernel       \$DREMIO_VER
    install \$JARS/dremio-common-\${DREMIO_VER}.jar             com.dremio          dremio-common             \$DREMIO_VER
    install \$JARS/dremio-connector-\${DREMIO_VER}.jar          com.dremio          dremio-connector          \$DREMIO_VER
    install \$JARS/dremio-sabot-logical-\${DREMIO_VER}.jar      com.dremio          dremio-sabot-logical      \$DREMIO_VER
    install \$JARS/dremio-sabot-vector-tools-\${DREMIO_VER}.jar com.dremio          dremio-sabot-vector-tools \$DREMIO_VER
    install \$JARS/dremio-services-namespace-\${DREMIO_VER}.jar com.dremio          dremio-services-namespace \$DREMIO_VER
    install \$JARS/dremio-common-core-\${DREMIO_VER}.jar        com.dremio          dremio-common-core        \$DREMIO_VER
    install \$JARS/dremio-services-datastore-\${DREMIO_VER}.jar com.dremio          dremio-services-datastore \$DREMIO_VER
    install \$TP/arrow-vector-\${ARROW_VER}.jar                 org.apache.arrow    arrow-vector              \$ARROW_VER
    install \$TP/arrow-memory-core-\${ARROW_VER}.jar            org.apache.arrow    arrow-memory-core         \$ARROW_VER
    install \$TP/calcite-core-\${CALCITE_VER}.jar               org.apache.calcite  calcite-core              \$CALCITE_VER
    install \$TP/calcite-linq4j-\${CALCITE_VER}.jar             org.apache.calcite  calcite-linq4j            \$CALCITE_VER
    install \$TP/guava-33.4.0-jre.jar                           com.google.guava    guava                     33.4.0-jre
    install \$TP/javax.inject-1.jar                             javax.inject        javax.inject              1
    # Update pom.xml versions to match running Dremio
    sed -i \"s|<dremio.version>.*</dremio.version>|<dremio.version>\${DREMIO_VER}</dremio.version>|\" pom.xml
    sed -i \"s|<arrow.version>.*</arrow.version>|<arrow.version>\${ARROW_VER}</arrow.version>|\" pom.xml
    sed -i \"s|<calcite.version>.*</calcite.version>|<calcite.version>\${CALCITE_VER}</calcite.version>|\" pom.xml
    mvn package -DskipTests -q
    echo 'Build complete'
  "

  # Copy built JAR out and deploy
  local built_jar
  built_jar="$(docker exec "$container" ls "${build_dir}/target/"*-plugin.jar 2>/dev/null | head -1)"
  docker exec -u root "$container" \
    cp "$built_jar" "/opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME}"

  # Save JAR back to local jars/ for future --prebuilt installs
  docker cp "${container}:${built_jar}" "${PREBUILT_JAR}"
  ok "Built JAR saved to jars/ for future --prebuilt installs"
}

# ── Deploy: Docker ────────────────────────────────────────────────────────────
if [[ "$MODE" == "docker" ]]; then
  info "Deploying to Docker container: $TARGET"

  # Check container is running
  docker inspect "$TARGET" &>/dev/null || err "Container '$TARGET' not found"
  status="$(docker inspect --format '{{.State.Status}}' "$TARGET")"
  [[ "$status" == "running" ]] || err "Container '$TARGET' is not running (status: $status)"

  if [[ "$JAR_MODE" == "build" ]]; then
    build_in_docker "$TARGET"
  else
    # Pre-built
    [[ -f "$PREBUILT_JAR" ]] || err "Pre-built JAR not found: $PREBUILT_JAR"
    info "Copying pre-built JAR…"
    docker cp "$PREBUILT_JAR" "${TARGET}:/tmp/${PLUGIN_JAR_NAME}"
    docker exec -u root "$TARGET" \
      cp "/tmp/${PLUGIN_JAR_NAME}" "/opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME}"
    docker exec -u root "$TARGET" \
      chmod 644 "/opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME}"
  fi

  ok "JAR deployed to /opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME}"
  info "Restarting Dremio container '$TARGET'…"
  docker restart "$TARGET"
  ok "Container restarted — Dremio will be ready in ~30 seconds"
  echo ""
  echo -e "  Open: ${BOLD}http://localhost:9047${RESET}"
  echo -e "  Then: ${BOLD}Sources → + → Amazon DynamoDB${RESET}"

# ── Deploy: Local ─────────────────────────────────────────────────────────────
elif [[ "$MODE" == "local" ]]; then
  info "Deploying to local Dremio at: $TARGET"
  [[ -d "$TARGET" ]] || err "Dremio home '$TARGET' not found"
  DEST_DIR="${TARGET}/${DREMIO_JAR_SUBDIR}"
  [[ -d "$DEST_DIR" ]] || err "JAR directory not found: $DEST_DIR"

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    [[ -f "$PREBUILT_JAR" ]] || err "Pre-built JAR not found: $PREBUILT_JAR"
    cp "$PREBUILT_JAR" "${DEST_DIR}/${PLUGIN_JAR_NAME}"
  else
    warn "Build from source not supported in --local mode without a running Dremio container"
    err "Use --prebuilt, or run build manually with mvn package -DskipTests"
  fi

  ok "JAR deployed to ${DEST_DIR}/${PLUGIN_JAR_NAME}"
  warn "Restart Dremio to load the new connector"

# ── Deploy: Kubernetes ────────────────────────────────────────────────────────
elif [[ "$MODE" == "k8s" ]]; then
  [[ -n "$TARGET" ]] || err "Kubernetes pod name required: --k8s <pod>"
  info "Deploying to Kubernetes pod: $TARGET"

  if [[ "$JAR_MODE" == "prebuilt" ]]; then
    [[ -f "$PREBUILT_JAR" ]] || err "Pre-built JAR not found: $PREBUILT_JAR"
    kubectl cp "$PREBUILT_JAR" "${TARGET}:/opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME}"
  else
    err "Build from source not supported in --k8s mode. Use --prebuilt or see k8s/KUBERNETES.md for image-based deploy."
  fi

  ok "JAR deployed to pod $TARGET"
  warn "Restart the Dremio pod to load the new connector"
  echo "  kubectl delete pod $TARGET"
fi
