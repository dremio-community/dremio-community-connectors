#!/usr/bin/env bash
# ============================================================================
# Dremio Azure Cosmos DB Connector — Version-aware Rebuild Script
#
# PURPOSE
#   Detects the exact Dremio version currently running in the target environment,
#   updates pom.xml to match, rebuilds the connector JAR against the live JARs,
#   and redeploys — all in one command.
#
#   Run this whenever you upgrade Dremio and need to bring the connector along.
#
# USAGE
#   ./rebuild.sh [--docker CONTAINER] [--local DREMIO_HOME] [--k8s POD]
#
# OPTIONS
#   --docker CONTAINER      Rebuild against a running Docker container (default: try-dremio)
#   --local  DREMIO_HOME    Rebuild against a bare-metal Dremio installation
#   --k8s    POD            Rebuild against a Kubernetes pod (coordinator pod)
#   --namespace  NS  (-n)   Kubernetes namespace (K8s mode only)
#   --pod-selector SEL (-l) Override pod label selector for multi-pod deploy (K8s mode only)
#   --dry-run               Detect version and show what would change, without rebuilding
#   --force                 Rebuild even if the detected version matches pom.xml
#   -h, --help              Show this help
#
# EXAMPLES
#   ./rebuild.sh                                     # Docker, auto-detects container try-dremio
#   ./rebuild.sh --docker my-dremio                  # Docker, named container
#   ./rebuild.sh --local /opt/dremio                 # Bare-metal
#   ./rebuild.sh --k8s dremio-master-0               # K8s, default namespace
#   ./rebuild.sh --k8s dremio-master-0 -n dremio-ns  # K8s, specific namespace
#   ./rebuild.sh --dry-run                           # See what version is running, no changes
#   ./rebuild.sh --force                             # Rebuild even if version unchanged
# ============================================================================

set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM_XML="${SCRIPT_DIR}/pom.xml"
PLUGIN_JAR_NAME="dremio-cosmosdb-connector-1.0.0.jar"

# ── Colour helpers ─────────────────────────────────────────────────────────────
BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; CYAN='\033[0;36m'; RESET='\033[0m'
step()  { echo -e "\n${BOLD}[${1}]${RESET} ${2}"; }
ok()    { echo -e "    ${GREEN}✓${RESET}  ${1}"; }
warn()  { echo -e "    ${YELLOW}⚠${RESET}  ${1}"; }
err()   { echo -e "    ${RED}✗${RESET}  ${1}" >&2; }
info()  { echo -e "    ${CYAN}→${RESET}  ${1}"; }
die()   { err "$*"; exit 1; }

# ── Argument parsing ───────────────────────────────────────────────────────────
MODE="docker"
TARGET="try-dremio"
DRY_RUN=false
FORCE=false
NAMESPACE=""
POD_SELECTOR=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)          MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)           MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --k8s)             MODE="k8s";    TARGET="${2:-}"; shift 2 || shift ;;
    --namespace|-n)    NAMESPACE="$2";    shift 2 ;;
    --pod-selector|-l) POD_SELECTOR="$2"; shift 2 ;;
    --dry-run)         DRY_RUN=true; shift ;;
    --force)           FORCE=true; shift ;;
    -h|--help)
      grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) die "Unknown argument: $1 — use --help for usage" ;;
  esac
done

echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   Dremio Azure Cosmos DB Connector — Version-aware Rebuild${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════${RESET}"

# ── Build the exec command for the target environment ─────────────────────────
NS_FLAG=()
case "$MODE" in
  docker)
    if ! docker ps --format '{{.Names}}' | grep -q "^${TARGET}$"; then
      die "Docker container '${TARGET}' is not running. Start it first."
    fi
    DREMIO_JARS_DIR="/opt/dremio/jars"
    info "Target: Docker container '${TARGET}'"
    ;;
  local)
    [[ -d "$TARGET" ]] || die "Dremio home '${TARGET}' not found."
    DREMIO_JARS_DIR="${TARGET}/jars"
    info "Target: local installation at '${TARGET}'"
    ;;
  k8s)
    [[ -n "$TARGET" ]] || die "--k8s requires a pod name"
    [[ -n "$NAMESPACE" ]] && NS_FLAG=(-n "$NAMESPACE")
    DREMIO_JARS_DIR="/opt/dremio/jars"
    info "Target: Kubernetes pod '${TARGET}'"
    [[ -n "$NAMESPACE" ]] && info "Namespace: ${NAMESPACE}"
    ;;
esac

run() {
  case "$MODE" in
    docker) docker exec "$TARGET" bash -c "$1" ;;
    local)  bash -c "$1" ;;
    k8s)    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c "$1" ;;
  esac
}

# ── Step 1: Detect versions ───────────────────────────────────────────────────
step "1" "Detecting Dremio version from running instance..."

DREMIO_JAR=$(run "ls ${DREMIO_JARS_DIR}/dremio-common-*.jar 2>/dev/null | grep -v proto | grep -v sources | head -1" 2>/dev/null) \
  || die "Could not list Dremio JARs at ${DREMIO_JARS_DIR}. Is Dremio installed?"
[[ -n "$DREMIO_JAR" ]] || die "No dremio-common JAR found in ${DREMIO_JARS_DIR}"

DETECTED_DREMIO=$(basename "$DREMIO_JAR" .jar | sed 's/^dremio-common-//')
ok "Dremio version : ${DETECTED_DREMIO}"

ARROW_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/arrow-vector-*.jar 2>/dev/null | head -1" 2>/dev/null || true)
if [[ -n "$ARROW_JAR" ]]; then
  DETECTED_ARROW=$(basename "$ARROW_JAR" .jar | sed 's/^arrow-vector-//')
  ok "Arrow version  : ${DETECTED_ARROW}"
else
  DETECTED_ARROW=""
  warn "Arrow JAR not found in 3rdparty/ — will skip Arrow version update"
fi

HADOOP_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/hadoop-common-*.jar 2>/dev/null | head -1" 2>/dev/null || true)
if [[ -n "$HADOOP_JAR" ]]; then
  DETECTED_HADOOP=$(basename "$HADOOP_JAR" .jar | sed 's/^hadoop-common-//')
  ok "Hadoop version : ${DETECTED_HADOOP}"
else
  DETECTED_HADOOP=""
fi

CALCITE_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/calcite-core-*.jar 2>/dev/null | head -1" 2>/dev/null || true)
if [[ -n "$CALCITE_JAR" ]]; then
  DETECTED_CALCITE=$(basename "$CALCITE_JAR" .jar | sed 's/^calcite-core-//')
  ok "Calcite version: ${DETECTED_CALCITE}"
else
  DETECTED_CALCITE=""
fi

# ── Step 2: Compare against pom.xml ──────────────────────────────────────────
step "2" "Comparing against current pom.xml..."

CURRENT_DREMIO=$(sed -n 's|.*<dremio.version>\([^<]*\).*|\1|p' "$POM_XML" | head -1)
CURRENT_ARROW=$(sed -n 's|.*<arrow.version>\([^<]*\).*|\1|p' "$POM_XML" | head -1 || echo "")
CURRENT_HADOOP=$(sed -n 's|.*<hadoop.version>\([^<]*\).*|\1|p' "$POM_XML" | head -1 || echo "")

info "pom.xml dremio.version : ${CURRENT_DREMIO}"
[[ -n "$CURRENT_ARROW"  ]] && info "pom.xml arrow.version  : ${CURRENT_ARROW}"
[[ -n "$CURRENT_HADOOP" ]] && info "pom.xml hadoop.version : ${CURRENT_HADOOP}"

VERSIONS_MATCH=true
[[ "$DETECTED_DREMIO" != "$CURRENT_DREMIO" ]] && VERSIONS_MATCH=false
[[ -n "$DETECTED_ARROW"  && -n "$CURRENT_ARROW"  && "$DETECTED_ARROW"  != "$CURRENT_ARROW"  ]] && VERSIONS_MATCH=false
[[ -n "$DETECTED_HADOOP" && -n "$CURRENT_HADOOP" && "$DETECTED_HADOOP" != "$CURRENT_HADOOP" ]] && VERSIONS_MATCH=false

if $VERSIONS_MATCH && ! $FORCE; then
  echo ""
  ok "pom.xml already targets the running Dremio version."
  ok "Connector is up to date — nothing to rebuild."
  info "Run with --force to rebuild anyway."
  exit 0
fi

if ! $VERSIONS_MATCH; then
  warn "Version mismatch detected:"
  [[ "$DETECTED_DREMIO" != "$CURRENT_DREMIO" ]] && \
    warn "  dremio : ${CURRENT_DREMIO} → ${DETECTED_DREMIO}"
  [[ -n "$DETECTED_ARROW"  && -n "$CURRENT_ARROW"  && "$DETECTED_ARROW"  != "$CURRENT_ARROW"  ]] && \
    warn "  arrow  : ${CURRENT_ARROW} → ${DETECTED_ARROW}"
  [[ -n "$DETECTED_HADOOP" && -n "$CURRENT_HADOOP" && "$DETECTED_HADOOP" != "$CURRENT_HADOOP" ]] && \
    warn "  hadoop : ${CURRENT_HADOOP} → ${DETECTED_HADOOP}"
fi

if $DRY_RUN; then
  info "Dry-run mode — no changes made."
  exit 0
fi

# ── Step 3: Update pom.xml ────────────────────────────────────────────────────
step "3" "Updating pom.xml..."

cp "$POM_XML" "${POM_XML}.bak"
ok "Backed up pom.xml → pom.xml.bak"

sed -i.tmp "s|<dremio.version>[^<]*</dremio.version>|<dremio.version>${DETECTED_DREMIO}</dremio.version>|g" "$POM_XML"
[[ -n "$DETECTED_ARROW"  && -n "$CURRENT_ARROW"  ]] && \
  sed -i.tmp "s|<arrow.version>[^<]*</arrow.version>|<arrow.version>${DETECTED_ARROW}</arrow.version>|g" "$POM_XML"
[[ -n "$DETECTED_HADOOP" && -n "$CURRENT_HADOOP" ]] && \
  sed -i.tmp "s|<hadoop.version>[^<]*</hadoop.version>|<hadoop.version>${DETECTED_HADOOP}</hadoop.version>|g" "$POM_XML"
rm -f "${POM_XML}.tmp"
ok "pom.xml updated"

# ── Step 4: Install JARs from running Dremio into Maven local repo ────────────
step "4" "Installing Dremio JARs into Maven local repo..."

if [[ "$MODE" == "docker" ]]; then
  if ! docker exec "$TARGET" which mvn > /dev/null 2>&1; then
    info "Installing Maven in container (one-time)..."
    docker exec -u root "$TARGET" bash -c \
      "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -2"
  fi
elif [[ "$MODE" == "k8s" ]]; then
  kubectl exec "${NS_FLAG[@]}" "$TARGET" -- which mvn > /dev/null 2>&1 || \
    die "Maven not found in pod. Pre-install Maven or use: ./install.sh --k8s $TARGET --prebuilt"
fi

install_jar() {
  local jar_path="$1" group="$2" artifact="$3" version="$4"
  case "$MODE" in
    docker)
      docker exec "$TARGET" bash -c "mvn install:install-file -q \
        -Dfile='${jar_path}' -DgroupId='${group}' -DartifactId='${artifact}' \
        -Dversion='${version}' -Dpackaging=jar 2>/dev/null && \
        echo '  ✓ ${artifact}' || echo '  ⚠ skipped: ${artifact}'" ;;
    local)
      mvn install:install-file -q -Dfile="${jar_path}" -DgroupId="${group}" \
        -DartifactId="${artifact}" -Dversion="${version}" -Dpackaging=jar 2>/dev/null && \
        echo "  ✓ ${artifact}" || echo "  ⚠ skipped: ${artifact}" ;;
    k8s)
      kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c "mvn install:install-file -q \
        -Dfile='${jar_path}' -DgroupId='${group}' -DartifactId='${artifact}' \
        -Dversion='${version}' -Dpackaging=jar 2>/dev/null" && \
        echo "  ✓ ${artifact}" || echo "  ⚠ skipped: ${artifact}" ;;
  esac
}

JARS="${DREMIO_JARS_DIR}"; TP="${DREMIO_JARS_DIR}/3rdparty"; DV="${DETECTED_DREMIO}"
install_jar "${JARS}/dremio-common-${DV}.jar"             com.dremio      dremio-common             "$DV"
install_jar "${JARS}/dremio-sabot-kernel-${DV}.jar"       com.dremio.sabot dremio-sabot-kernel      "$DV"
install_jar "${JARS}/dremio-connector-${DV}.jar"          com.dremio      dremio-connector           "$DV"
install_jar "${JARS}/dremio-sabot-logical-${DV}.jar"      com.dremio      dremio-sabot-logical       "$DV"
install_jar "${JARS}/dremio-common-core-${DV}.jar"        com.dremio      dremio-common-core         "$DV"
install_jar "${JARS}/dremio-services-namespace-${DV}.jar" com.dremio      dremio-services-namespace  "$DV"

[[ -n "$DETECTED_ARROW" ]] && {
  AV="$DETECTED_ARROW"
  install_jar "${TP}/arrow-vector-${AV}.jar"      org.apache.arrow arrow-vector      "$AV"
  install_jar "${TP}/arrow-memory-core-${AV}.jar" org.apache.arrow arrow-memory-core "$AV"
  install_jar "${TP}/arrow-format-${AV}.jar"      org.apache.arrow arrow-format      "$AV"
}
[[ -n "$DETECTED_HADOOP"  ]] && install_jar "${TP}/hadoop-common-${DETECTED_HADOOP}.jar"   org.apache.hadoop  hadoop-common  "$DETECTED_HADOOP"
[[ -n "$DETECTED_CALCITE" ]] && {
  install_jar "${TP}/calcite-core-${DETECTED_CALCITE}.jar"   org.apache.calcite calcite-core   "$DETECTED_CALCITE"
  install_jar "${TP}/calcite-linq4j-${DETECTED_CALCITE}.jar" org.apache.calcite calcite-linq4j "$DETECTED_CALCITE"
}
ok "JAR installation complete"

# ── Step 5: Build ─────────────────────────────────────────────────────────────
step "5" "Building connector against new JARs..."

BUILD_FAILED=false

if [[ "$MODE" == "docker" ]]; then
  docker exec "$TARGET" bash -c "rm -rf /tmp/cosmosdb-rebuild && mkdir -p /tmp/cosmosdb-rebuild"
  docker cp "${SCRIPT_DIR}/src"     "${TARGET}:/tmp/cosmosdb-rebuild/"
  docker cp "${SCRIPT_DIR}/pom.xml" "${TARGET}:/tmp/cosmosdb-rebuild/"
  docker exec -u root "$TARGET" bash -c "chown -R dremio:dremio /tmp/cosmosdb-rebuild"
  docker exec -u dremio "$TARGET" bash -c "
    cd /tmp/cosmosdb-rebuild && mvn package -DskipTests --batch-mode -q
  " || BUILD_FAILED=true

elif [[ "$MODE" == "local" ]]; then
  BUILD_TMPDIR="$(mktemp -d)"
  cp -r "${SCRIPT_DIR}/src" "$BUILD_TMPDIR/"
  cp    "${SCRIPT_DIR}/pom.xml" "$BUILD_TMPDIR/"
  (cd "$BUILD_TMPDIR" && mvn package -DskipTests --batch-mode -q) || BUILD_FAILED=true

else
  kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
    "rm -rf /tmp/cosmosdb-rebuild && mkdir -p /tmp/cosmosdb-rebuild"
  tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
    kubectl exec "${NS_FLAG[@]}" -i "$TARGET" -- tar -C /tmp/cosmosdb-rebuild -xf -
  kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
    "cd /tmp/cosmosdb-rebuild && mvn package -DskipTests -q --batch-mode" || BUILD_FAILED=true
  if ! $BUILD_FAILED; then
    mkdir -p "${SCRIPT_DIR}/jars"
    kubectl cp "${NS_FLAG[@]}" \
      "${TARGET}:/tmp/cosmosdb-rebuild/jars/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
    ok "Extracted JAR to jars/ for multi-pod deploy"
  fi
fi

if $BUILD_FAILED; then
  err "Build FAILED."
  echo -e "  ${YELLOW}Common causes:${RESET}"
  echo    "    • A planning rule base class changed (RelOptRule / RelRule errors)"
  echo    "    • A new abstract method was added to AbstractRecordReader or StoragePlugin"
  echo    "    • A class moved to a different package"
  echo    "  Fix the affected Java file(s) and re-run: ./rebuild.sh --force"
  cp "${POM_XML}.bak" "$POM_XML"
  warn "pom.xml restored. Fix compilation errors then re-run."
  exit 2
fi
ok "Build successful"

# ── Step 6: Deploy ────────────────────────────────────────────────────────────
step "6" "Deploying new connector JAR..."

if [[ "$MODE" == "docker" ]]; then
  docker exec -u root "$TARGET" bash -c \
    "cp /tmp/cosmosdb-rebuild/jars/${PLUGIN_JAR_NAME} /opt/dremio/jars/3rdparty/"
  docker cp "${TARGET}:/tmp/cosmosdb-rebuild/jars/${PLUGIN_JAR_NAME}" \
    "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null && \
    ok "Saved JAR to jars/ for future installs"

elif [[ "$MODE" == "local" ]]; then
  cp "${BUILD_TMPDIR}/target/${PLUGIN_JAR_NAME}" "${TARGET}/jars/3rdparty/"
  rm -rf "$BUILD_TMPDIR"

else
  APP_LABEL=$(kubectl get pod "${NS_FLAG[@]}" "$TARGET" \
    -o jsonpath='{.metadata.labels.app}' 2>/dev/null || true)
  SELECTOR="${POD_SELECTOR:-${APP_LABEL:+app=${APP_LABEL}}}"
  DEPLOY_PODS=$([[ -n "$SELECTOR" ]] && \
    kubectl get pods "${NS_FLAG[@]}" -l "$SELECTOR" \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || \
    echo "$TARGET")
  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl cp "${NS_FLAG[@]}" "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" "${pod}:/tmp/${PLUGIN_JAR_NAME}"
    kubectl exec "${NS_FLAG[@]}" "$pod" -- bash -c \
      "cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/jars/3rdparty/"
    ok "Deployed to ${pod}"
  done <<< "$DEPLOY_PODS"
fi
ok "JAR deployed to 3rdparty/"

# ── Step 7: Restart Dremio ────────────────────────────────────────────────────
step "7" "Restarting Dremio..."

if [[ "$MODE" == "docker" ]]; then
  docker restart "$TARGET" > /dev/null
  info "Waiting for Dremio to start..."
  for i in $(seq 1 60); do
    sleep 3
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9047/apiv2/info" 2>/dev/null || true)
    if [[ "$HTTP" == "200" || "$HTTP" == "404" ]]; then
      ok "Dremio is up (${i} × 3s = $((i*3))s)"; break
    fi
    echo -n "."
  done
  echo ""

elif [[ "$MODE" == "local" ]]; then
  warn "Bare-metal: restart Dremio manually to load the new connector."
  info "  systemctl restart dremio   OR   \$DREMIO_HOME/bin/dremio restart"

else
  warn "Kubernetes: restart Dremio pods to load the new connector."
  SS_NAME=$(kubectl get statefulset "${NS_FLAG[@]}" -l "${SELECTOR:-}" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  [[ -n "$SS_NAME" ]] && info "  kubectl rollout restart statefulset/${SS_NAME}${NAMESPACE:+ -n $NAMESPACE}" || \
    info "  kubectl rollout restart statefulset/<name>${NAMESPACE:+ -n $NAMESPACE}"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}Rebuild complete.${RESET}"
echo -e "  Dremio version : ${DETECTED_DREMIO}"
echo -e "  Connector JAR  : ${PLUGIN_JAR_NAME}"
echo -e "  pom.xml.bak    : previous version saved (delete when satisfied)"
echo ""
