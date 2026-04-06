#!/usr/bin/env bash
# ============================================================================
# Dremio Hudi Connector — Version-aware Rebuild Script
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
#
# WHAT IT DOES
#   1. Detects the Dremio and Arrow JAR versions from the running instance
#   2. Compares them against the versions currently in pom.xml
#   3. If unchanged and --force not set: exits early (nothing to do)
#   4. If changed: updates pom.xml, installs the live JARs into Maven local repo,
#      compiles the connector against them, deploys the new JAR, restarts Dremio
#
# EXIT CODES
#   0  Rebuild successful (or up-to-date, no rebuild needed)
#   1  Version detection failed
#   2  Build failed — see output for compilation errors
#   3  Deployment failed
# ============================================================================

set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM_XML="${SCRIPT_DIR}/pom.xml"
PLUGIN_JAR_NAME="dremio-hudi-connector-1.0.0-SNAPSHOT-plugin.jar"

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
echo -e "${BOLD}${CYAN}   Dremio Apache Hudi Connector — Version-aware Rebuild${RESET}"
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

# run <bash-string> — execute a shell command in the target environment
run() {
  case "$MODE" in
    docker) docker exec "$TARGET" bash -c "$1" ;;
    local)  bash -c "$1" ;;
    k8s)    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c "$1" ;;
  esac
}

# ── Step 1: Detect versions from running Dremio ───────────────────────────────
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

# ── Step 2: Compare against pom.xml ──────────────────────────────────────────
step "2" "Comparing against current pom.xml..."

CURRENT_DREMIO=$(sed -n 's|.*<dremio.version>\([^<]*\)</dremio.version>.*|\1|p' "$POM_XML" | head -1)
CURRENT_ARROW=$(sed -n 's|.*<arrow.version>\([^<]*\)</arrow.version>.*|\1|p' "$POM_XML" | head -1 || echo "")

info "pom.xml dremio.version : ${CURRENT_DREMIO}"
[[ -n "$CURRENT_ARROW" ]] && info "pom.xml arrow.version  : ${CURRENT_ARROW}"

VERSIONS_MATCH=true
[[ "$DETECTED_DREMIO" != "$CURRENT_DREMIO" ]] && VERSIONS_MATCH=false
[[ -n "$DETECTED_ARROW" && -n "$CURRENT_ARROW" && "$DETECTED_ARROW" != "$CURRENT_ARROW" ]] && VERSIONS_MATCH=false

if $VERSIONS_MATCH && ! $FORCE; then
  echo ""
  ok "pom.xml already targets the running Dremio version."
  ok "Connector is up to date — nothing to rebuild."
  info "Run with --force to rebuild anyway."
  exit 0
fi

if ! $VERSIONS_MATCH; then
  echo ""
  warn "Version mismatch detected:"
  [[ "$DETECTED_DREMIO" != "$CURRENT_DREMIO" ]] && \
    warn "  dremio : ${CURRENT_DREMIO} → ${DETECTED_DREMIO}"
  [[ -n "$DETECTED_ARROW" && -n "$CURRENT_ARROW" && "$DETECTED_ARROW" != "$CURRENT_ARROW" ]] && \
    warn "  arrow  : ${CURRENT_ARROW} → ${DETECTED_ARROW}"
fi

if $DRY_RUN; then
  echo ""
  info "Dry-run mode — no changes made."
  info "Re-run without --dry-run to perform the rebuild."
  exit 0
fi

# ── Step 3: Update pom.xml ────────────────────────────────────────────────────
step "3" "Updating pom.xml to match detected versions..."

cp "$POM_XML" "${POM_XML}.bak"
ok "Backed up pom.xml → pom.xml.bak"

sed -i.tmp "s|<dremio.version>[^<]*</dremio.version>|<dremio.version>${DETECTED_DREMIO}</dremio.version>|g" "$POM_XML"

if [[ -n "$DETECTED_ARROW" && -n "$CURRENT_ARROW" ]]; then
  sed -i.tmp "s|<arrow.version>[^<]*</arrow.version>|<arrow.version>${DETECTED_ARROW}</arrow.version>|g" "$POM_XML"
fi

rm -f "${POM_XML}.tmp"
ok "pom.xml updated"

# ── Step 4: Install JARs from running Dremio into Maven local repo ────────────
step "4" "Installing Dremio JARs from running instance into Maven local repo..."

if [[ "$MODE" == "docker" ]]; then
  if ! docker exec "$TARGET" which mvn > /dev/null 2>&1; then
    info "Installing Maven in container (one-time)..."
    docker exec -u root "$TARGET" bash -c \
      "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -2"
  fi
elif [[ "$MODE" == "k8s" ]]; then
  if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- which mvn > /dev/null 2>&1; then
    warn "Maven not found in pod '${TARGET}'."
    warn "Dremio pods typically don't allow apt-get as root."
    info "Options:"
    info "  1. Pre-install Maven in your pod image"
    info "  2. Build locally and use: ./install.sh --k8s $TARGET --prebuilt"
    die "Cannot build from source without Maven in the K8s pod."
  fi
fi

install_jar_from_target() {
  local jar_path="$1"
  local group_id="$2"
  local artifact_id="$3"
  local version="$4"

  if [[ "$MODE" == "docker" ]]; then
    docker exec "$TARGET" bash -c "
      mvn install:install-file -q \
        -Dfile='${jar_path}' \
        -DgroupId='${group_id}' \
        -DartifactId='${artifact_id}' \
        -Dversion='${version}' \
        -Dpackaging=jar 2>/dev/null && echo '  ✓ ${artifact_id}' || echo '  ⚠ skipped: ${artifact_id}'
    "
  elif [[ "$MODE" == "local" ]]; then
    mvn install:install-file -q \
      -Dfile="${jar_path}" \
      -DgroupId="${group_id}" \
      -DartifactId="${artifact_id}" \
      -Dversion="${version}" \
      -Dpackaging=jar 2>/dev/null && echo "  ✓ ${artifact_id}" || echo "  ⚠ skipped: ${artifact_id}"
  else
    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c "
      mvn install:install-file -q \
        -Dfile='${jar_path}' \
        -DgroupId='${group_id}' \
        -DartifactId='${artifact_id}' \
        -Dversion='${version}' \
        -Dpackaging=jar 2>/dev/null
    " && echo "  ✓ ${artifact_id}" || echo "  ⚠ skipped: ${artifact_id}"
  fi
}

JARS="${DREMIO_JARS_DIR}"
TP="${DREMIO_JARS_DIR}/3rdparty"
DV="${DETECTED_DREMIO}"

# Core Dremio JARs
install_jar_from_target "${JARS}/dremio-common-${DV}.jar"             com.dremio        dremio-common             "$DV"
install_jar_from_target "${JARS}/dremio-sabot-kernel-${DV}.jar"       com.dremio        dremio-sabot-kernel       "$DV"
install_jar_from_target "${JARS}/dremio-sabot-kernel-${DV}-proto.jar" com.dremio        dremio-sabot-kernel-proto "$DV"
install_jar_from_target "${JARS}/dremio-sabot-vector-tools-${DV}.jar" com.dremio        dremio-sabot-vector-tools "$DV"
install_jar_from_target "${JARS}/dremio-connector-${DV}.jar"          com.dremio        dremio-connector          "$DV"
install_jar_from_target "${JARS}/dremio-sabot-logical-${DV}.jar"      com.dremio        dremio-sabot-logical      "$DV"
install_jar_from_target "${JARS}/dremio-common-core-${DV}.jar"        com.dremio        dremio-common-core        "$DV"
install_jar_from_target "${JARS}/dremio-services-namespace-${DV}.jar" com.dremio        dremio-services-namespace "$DV"
install_jar_from_target "${JARS}/dremio-services-catalog-${DV}.jar"   com.dremio        dremio-services-catalog   "$DV"
install_jar_from_target "${JARS}/dremio-plugin-common-${DV}.jar"      com.dremio.plugin dremio-plugin-common      "$DV"

# Arrow JARs
if [[ -n "$DETECTED_ARROW" ]]; then
  AV="${DETECTED_ARROW}"
  install_jar_from_target "${TP}/arrow-vector-${AV}.jar"      org.apache.arrow arrow-vector      "$AV"
  install_jar_from_target "${TP}/arrow-memory-core-${AV}.jar" org.apache.arrow arrow-memory-core "$AV"
  install_jar_from_target "${TP}/arrow-format-${AV}.jar"      org.apache.arrow arrow-format      "$AV"
fi

ok "JAR installation complete"

# ── Step 5: Copy source into build environment and compile ────────────────────
step "5" "Building connector against new JARs..."

BUILD_FAILED=false

if [[ "$MODE" == "docker" ]]; then
  docker exec "$TARGET" bash -c "rm -rf /tmp/hudi-rebuild && mkdir -p /tmp/hudi-rebuild"
  docker cp "${SCRIPT_DIR}/src"     "${TARGET}:/tmp/hudi-rebuild/"
  docker cp "${SCRIPT_DIR}/pom.xml" "${TARGET}:/tmp/hudi-rebuild/"
  docker exec -u root "$TARGET" bash -c "chmod -R 777 /tmp/hudi-rebuild"

  if ! docker exec "$TARGET" bash -c "
    cd /tmp/hudi-rebuild
    mvn package -DskipTests -q --batch-mode 2>&1
  "; then
    BUILD_FAILED=true
  fi

elif [[ "$MODE" == "local" ]]; then
  BUILD_TMPDIR="$(mktemp -d)"
  cp -r "${SCRIPT_DIR}/src"     "$BUILD_TMPDIR/"
  cp    "${SCRIPT_DIR}/pom.xml" "$BUILD_TMPDIR/"
  if ! (cd "$BUILD_TMPDIR" && mvn package -DskipTests -q --batch-mode 2>&1); then
    BUILD_FAILED=true
  fi

else
  # Create a clean build dir inside the pod
  kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
    "rm -rf /tmp/hudi-rebuild && mkdir -p /tmp/hudi-rebuild"
  tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
    kubectl exec "${NS_FLAG[@]}" -i "$TARGET" -- tar -C /tmp/hudi-rebuild -xf -
  if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
    "cd /tmp/hudi-rebuild && mvn package -DskipTests -q --batch-mode 2>&1"; then
    BUILD_FAILED=true
  fi
  # Extract the built JAR back to local jars/ for multi-pod deploy
  if ! $BUILD_FAILED; then
    mkdir -p "${SCRIPT_DIR}/jars"
    kubectl cp "${NS_FLAG[@]}" \
      "${TARGET}:/tmp/hudi-rebuild/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
    ok "Extracted JAR to jars/ for multi-pod deploy"
  fi
fi

if $BUILD_FAILED; then
  echo ""
  err "Build FAILED."
  echo ""
  echo -e "  ${YELLOW}This likely means a Dremio API changed between versions.${RESET}"
  echo -e "  ${YELLOW}Common causes:${RESET}"
  echo    "    • A planning rule base class changed (look for RelOptRule / RelRule errors)"
  echo    "    • A new abstract method was added to AbstractRecordReader or StoragePlugin"
  echo    "    • A class moved to a different package (look for import errors)"
  echo    "    • FileSystemPlugin method signature changed (Hudi extends it)"
  echo    ""
  echo -e "  ${YELLOW}What to do:${RESET}"
  echo    "    1. Check the full build output above for the first compilation error"
  echo    "    2. Fix the affected Java file(s) — usually 1-3 files in scan/ or planning/"
  echo    "    3. Re-run: ./rebuild.sh --force"
  echo    ""
  cp "${POM_XML}.bak" "$POM_XML"
  warn "pom.xml restored to previous version. Fix compilation errors then re-run."
  exit 2
fi

ok "Build successful"

# ── Step 6: Deploy new JAR ────────────────────────────────────────────────────
step "6" "Deploying new connector JAR..."

if [[ "$MODE" == "docker" ]]; then
  docker exec "$TARGET" bash -c \
    "cp /tmp/hudi-rebuild/target/${PLUGIN_JAR_NAME} /opt/dremio/jars/3rdparty/"
  docker cp "${TARGET}:/tmp/hudi-rebuild/target/${PLUGIN_JAR_NAME}" \
    "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null && \
    ok "Saved JAR to jars/ for future --prebuilt installs"

elif [[ "$MODE" == "local" ]]; then
  cp "${BUILD_TMPDIR}/target/${PLUGIN_JAR_NAME}" "${TARGET}/jars/3rdparty/"
  cp "${BUILD_TMPDIR}/target/${PLUGIN_JAR_NAME}" "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null || true
  rm -rf "$BUILD_TMPDIR"

else
  # Auto-discover all Dremio pods (coordinator + executors)
  APP_LABEL=$(kubectl get pod "${NS_FLAG[@]}" "$TARGET" \
    -o jsonpath='{.metadata.labels.app}' 2>/dev/null || true)
  if [[ -n "$POD_SELECTOR" ]]; then
    SELECTOR="$POD_SELECTOR"
  elif [[ -n "$APP_LABEL" ]]; then
    SELECTOR="app=${APP_LABEL}"
  else
    SELECTOR=""
  fi

  if [[ -n "$SELECTOR" ]]; then
    DEPLOY_PODS=$(kubectl get pods "${NS_FLAG[@]}" -l "$SELECTOR" \
      --field-selector=status.phase=Running \
      -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null \
      | grep -v '^$' || true)
  else
    DEPLOY_PODS="$TARGET"
  fi

  POD_COUNT=$(echo "$DEPLOY_PODS" | grep -c . || true)
  info "Deploying to ${POD_COUNT} pod(s)${SELECTOR:+ (selector: $SELECTOR)}..."

  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl cp "${NS_FLAG[@]}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" "${pod}:/tmp/${PLUGIN_JAR_NAME}"
    kubectl exec "${NS_FLAG[@]}" "$pod" -- bash -c \
      "cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/jars/3rdparty/ && \
       chown dremio:dremio /opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME} 2>/dev/null || true"
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
      ok "Dremio is up (${i} × 3s = $((i*3))s)"
      break
    fi
    echo -n "."
  done
  echo ""

elif [[ "$MODE" == "local" ]]; then
  warn "Bare-metal install: restart Dremio manually to load the new connector."
  info "  systemctl restart dremio   OR   \$DREMIO_HOME/bin/dremio restart"

else
  warn "Kubernetes: restart Dremio pods to load the new connector."
  echo ""
  SS_NAME=""
  if [[ -n "${SELECTOR:-}" ]]; then
    SS_NAME=$(kubectl get statefulset "${NS_FLAG[@]}" -l "$SELECTOR" \
      -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || true)
  fi
  if [[ -n "$SS_NAME" ]]; then
    info "Option A — Rolling restart (zero-downtime if replicas > 1):"
    info "  kubectl rollout restart statefulset/${SS_NAME}${NAMESPACE:+ -n $NAMESPACE}"
    echo ""
    info "Option B — Immediate restart (brief downtime):"
    info "  kubectl delete pods ${NS_FLAG[*]:-} -l ${SELECTOR}"
  else
    info "  kubectl rollout restart statefulset/<name>${NAMESPACE:+ -n $NAMESPACE}"
  fi
  echo ""
  warn "Note: JAR changes are lost if a pod is replaced by its StatefulSet."
  warn "For persistence across pod restarts, mount jars/3rdparty/ from a PVC."
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}Rebuild complete.${RESET}"
echo -e "  Dremio version : ${DETECTED_DREMIO}"
echo -e "  Connector JAR  : ${PLUGIN_JAR_NAME}"
echo -e "  pom.xml.bak    : previous version saved (delete when satisfied)"
echo ""
