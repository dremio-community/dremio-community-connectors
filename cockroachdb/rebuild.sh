#!/usr/bin/env bash
# ============================================================================
# Dremio CockroachDB Connector — Version-aware Rebuild Script
#
# USAGE
#   ./rebuild.sh [--docker CONTAINER] [--local DREMIO_HOME] [--k8s POD]
#
# OPTIONS
#   --docker CONTAINER    Rebuild against a running Docker container (default: try-dremio)
#   --local  DREMIO_HOME  Rebuild against a bare-metal Dremio installation
#   --k8s    POD          Rebuild against a Kubernetes pod
#   --dry-run             Detect version and show what would change, without rebuilding
#   --force               Rebuild even if the detected version matches pom.xml
#   -h, --help            Show this help
# ============================================================================

set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM_XML="${SCRIPT_DIR}/pom.xml"
PLUGIN_JAR_NAME="dremio-cockroachdb-connector-1.0.0-SNAPSHOT-plugin.jar"

BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; CYAN='\033[0;36m'; RESET='\033[0m'
step()  { echo -e "\n${BOLD}[${1}]${RESET} ${2}"; }
ok()    { echo -e "    ${GREEN}✓${RESET}  ${1}"; }
warn()  { echo -e "    ${YELLOW}⚠${RESET}  ${1}"; }
err()   { echo -e "    ${RED}✗${RESET}  ${1}" >&2; }
info()  { echo -e "    ${CYAN}→${RESET}  ${1}"; }
die()   { err "$*"; exit 1; }

MODE="docker"
TARGET="try-dremio"
DRY_RUN=false
FORCE=false
NAMESPACE=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)     MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)      MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --k8s)        MODE="k8s";    TARGET="${2:-}"; shift 2 || shift ;;
    --namespace|-n) NAMESPACE="$2"; shift 2 ;;
    --dry-run)    DRY_RUN=true; shift ;;
    --force)      FORCE=true; shift ;;
    -h|--help)
      grep "^#" "$0" | grep -v "^#!/" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) die "Unknown argument: $1 — use --help for usage" ;;
  esac
done

echo ""
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   Dremio CockroachDB Connector — Version-aware Rebuild${RESET}"
echo -e "${BOLD}${CYAN}════════════════════════════════════════════════════════════${RESET}"

NS_FLAG=()
case "$MODE" in
  docker)
    if ! docker ps --format '{{.Names}}' | grep -q "^${TARGET}$"; then
      die "Docker container '${TARGET}' is not running."
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
    ;;
esac

run() {
  case "$MODE" in
    docker) docker exec "$TARGET" bash -c "$1" ;;
    local)  bash -c "$1" ;;
    k8s)    kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c "$1" ;;
  esac
}

step "1" "Detecting Dremio version from running instance..."

DREMIO_JAR=$(run "ls ${DREMIO_JARS_DIR}/dremio-common-*.jar 2>/dev/null | grep -v proto | grep -v sources | head -1") \
  || die "Could not list Dremio JARs."
DETECTED_DREMIO=$(basename "$DREMIO_JAR" .jar | sed 's/^dremio-common-//')
ok "Dremio version : ${DETECTED_DREMIO}"

ARROW_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/arrow-vector-*.jar 2>/dev/null | head -1" || true)
DETECTED_ARROW=""
if [[ -n "$ARROW_JAR" ]]; then
  DETECTED_ARROW=$(basename "$ARROW_JAR" .jar | sed 's/^arrow-vector-//')
  ok "Arrow version  : ${DETECTED_ARROW}"
fi

CALCITE_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/calcite-core-*.jar 2>/dev/null | head -1" || true)
DETECTED_CALCITE=""
if [[ -n "$CALCITE_JAR" ]]; then
  DETECTED_CALCITE=$(basename "$CALCITE_JAR" .jar | sed 's/^calcite-core-//')
  ok "Calcite version: ${DETECTED_CALCITE}"
fi

step "2" "Comparing against current pom.xml..."

CURRENT_DREMIO=$(sed -n 's|.*<dremio.version>\([^<]*\)</dremio.version>.*|\1|p' "$POM_XML" | head -1)
VERSIONS_MATCH=true
[[ "$DETECTED_DREMIO" != "$CURRENT_DREMIO" ]] && VERSIONS_MATCH=false

if $VERSIONS_MATCH && ! $FORCE; then
  ok "pom.xml already targets the running Dremio version — nothing to rebuild."
  info "Run with --force to rebuild anyway."
  exit 0
fi

if $DRY_RUN; then
  info "Dry-run mode — no changes made."
  exit 0
fi

step "3" "Updating pom.xml..."
cp "$POM_XML" "${POM_XML}.bak"
sed -i.tmp "s|<dremio.version>[^<]*</dremio.version>|<dremio.version>${DETECTED_DREMIO}</dremio.version>|g" "$POM_XML"
[[ -n "$DETECTED_ARROW" ]] && \
  sed -i.tmp "s|<arrow.version>[^<]*</arrow.version>|<arrow.version>${DETECTED_ARROW}</arrow.version>|g" "$POM_XML"
[[ -n "$DETECTED_CALCITE" ]] && \
  sed -i.tmp "s|<calcite.version>[^<]*</calcite.version>|<calcite.version>${DETECTED_CALCITE}</calcite.version>|g" "$POM_XML"
rm -f "${POM_XML}.tmp"
ok "pom.xml updated"

step "4" "Installing Dremio JARs from running instance into Maven local repo..."

if [[ "$MODE" == "docker" ]]; then
  if ! docker exec "$TARGET" which mvn > /dev/null 2>&1; then
    docker exec -u root "$TARGET" bash -c \
      "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -2"
  fi
fi

install_jar() {
  local jar_path="$1" group_id="$2" artifact_id="$3" version="$4"
  case "$MODE" in
    docker)
      docker exec "$TARGET" bash -c "
        mvn install:install-file -q -Dfile='${jar_path}' \
          -DgroupId='${group_id}' -DartifactId='${artifact_id}' \
          -Dversion='${version}' -Dpackaging=jar 2>/dev/null && echo '  ✓ ${artifact_id}' || true"
      ;;
    local)
      mvn install:install-file -q -Dfile="${jar_path}" \
        -DgroupId="${group_id}" -DartifactId="${artifact_id}" \
        -Dversion="${version}" -Dpackaging=jar 2>/dev/null && echo "  ✓ ${artifact_id}" || true
      ;;
    k8s)
      kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c "
        mvn install:install-file -q -Dfile='${jar_path}' \
          -DgroupId='${group_id}' -DartifactId='${artifact_id}' \
          -Dversion='${version}' -Dpackaging=jar 2>/dev/null" && echo "  ✓ ${artifact_id}" || true
      ;;
  esac
}

JARS="${DREMIO_JARS_DIR}"
TP="${DREMIO_JARS_DIR}/3rdparty"
DV="${DETECTED_DREMIO}"

install_jar "${JARS}/dremio-common-${DV}.jar"             com.dremio          dremio-common               "$DV"
install_jar "${JARS}/dremio-sabot-kernel-${DV}.jar"       com.dremio          dremio-sabot-kernel         "$DV"
install_jar "${JARS}/dremio-sabot-kernel-${DV}-proto.jar" com.dremio          dremio-sabot-kernel-proto   "$DV"
install_jar "${JARS}/dremio-sabot-vector-tools-${DV}.jar" com.dremio          dremio-sabot-vector-tools   "$DV"
install_jar "${JARS}/dremio-connector-${DV}.jar"          com.dremio          dremio-connector            "$DV"
install_jar "${JARS}/dremio-sabot-logical-${DV}.jar"      com.dremio          dremio-sabot-logical        "$DV"
install_jar "${JARS}/dremio-common-core-${DV}.jar"        com.dremio          dremio-common-core          "$DV"
install_jar "${JARS}/dremio-services-namespace-${DV}.jar" com.dremio          dremio-services-namespace   "$DV"
install_jar "${JARS}/dremio-plugin-common-${DV}.jar"      com.dremio.plugin   dremio-plugin-common        "$DV"
install_jar "${JARS}/dremio-services-credentials-${DV}.jar" com.dremio.services dremio-services-credentials "$DV"
install_jar "${JARS}/dremio-services-datastore-${DV}.jar" com.dremio          dremio-services-datastore   "$DV"
install_jar "${JARS}/dremio-services-options-${DV}.jar"   com.dremio          dremio-services-options     "$DV"
install_jar "${JARS}/dremio-ce-jdbc-plugin-${DV}.jar"     com.dremio.plugins  dremio-ce-jdbc-plugin       "$DV"
install_jar "${JARS}/dremio-ce-jdbc-fetcher-api-${DV}.jar" com.dremio.plugins dremio-ce-jdbc-fetcher-api  "$DV"

if [[ -n "$DETECTED_ARROW" ]]; then
  AV="${DETECTED_ARROW}"
  install_jar "${TP}/arrow-vector-${AV}.jar"      org.apache.arrow arrow-vector      "$AV"
  install_jar "${TP}/arrow-memory-core-${AV}.jar" org.apache.arrow arrow-memory-core "$AV"
fi
if [[ -n "$DETECTED_CALCITE" ]]; then
  install_jar "${TP}/calcite-core-${DETECTED_CALCITE}.jar"   org.apache.calcite calcite-core   "$DETECTED_CALCITE"
  install_jar "${TP}/calcite-linq4j-${DETECTED_CALCITE}.jar" org.apache.calcite calcite-linq4j "$DETECTED_CALCITE"
fi

ok "JAR installation complete"

step "5" "Building connector..."

BUILD_FAILED=false
if [[ "$MODE" == "docker" ]]; then
  docker exec "$TARGET" bash -c "rm -rf /tmp/cockroachdb-rebuild && mkdir -p /tmp/cockroachdb-rebuild"
  docker cp "${SCRIPT_DIR}/src"     "${TARGET}:/tmp/cockroachdb-rebuild/"
  docker cp "${SCRIPT_DIR}/pom.xml" "${TARGET}:/tmp/cockroachdb-rebuild/"
  docker exec -u root "$TARGET" bash -c "chmod -R 777 /tmp/cockroachdb-rebuild"
  if ! docker exec "$TARGET" bash -c \
    "cd /tmp/cockroachdb-rebuild && mvn package -DskipTests -q --batch-mode 2>&1"; then
    BUILD_FAILED=true
  fi
elif [[ "$MODE" == "local" ]]; then
  BUILD_TMPDIR="$(mktemp -d)"
  cp -r "${SCRIPT_DIR}/src"     "$BUILD_TMPDIR/"
  cp    "${SCRIPT_DIR}/pom.xml" "$BUILD_TMPDIR/"
  if ! (cd "$BUILD_TMPDIR" && mvn package -DskipTests -q --batch-mode); then
    BUILD_FAILED=true
  fi
else
  kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
    "rm -rf /tmp/cockroachdb-rebuild && mkdir -p /tmp/cockroachdb-rebuild"
  tar -C "$SCRIPT_DIR" --exclude='./target' --exclude='./jars' -cf - . | \
    kubectl exec "${NS_FLAG[@]}" -i "$TARGET" -- tar -C /tmp/cockroachdb-rebuild -xf -
  if ! kubectl exec "${NS_FLAG[@]}" "$TARGET" -- bash -c \
    "cd /tmp/cockroachdb-rebuild && mvn package -DskipTests -q --batch-mode"; then
    BUILD_FAILED=true
  fi
  if ! $BUILD_FAILED; then
    mkdir -p "${SCRIPT_DIR}/jars"
    kubectl cp "${NS_FLAG[@]}" \
      "${TARGET}:/tmp/cockroachdb-rebuild/target/${PLUGIN_JAR_NAME}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}"
  fi
fi

if $BUILD_FAILED; then
  err "Build FAILED."
  cp "${POM_XML}.bak" "$POM_XML"
  warn "pom.xml restored. Fix compilation errors then re-run."
  exit 2
fi
ok "Build successful"

step "6" "Deploying plugin JAR..."

if [[ "$MODE" == "docker" ]]; then
  docker exec "$TARGET" bash -c \
    "cp /tmp/cockroachdb-rebuild/target/${PLUGIN_JAR_NAME} /opt/dremio/jars/3rdparty/"
  docker cp "${TARGET}:/tmp/cockroachdb-rebuild/target/${PLUGIN_JAR_NAME}" \
    "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null && \
    ok "Saved plugin JAR to jars/ for future --prebuilt installs"
elif [[ "$MODE" == "local" ]]; then
  cp "${BUILD_TMPDIR}/target/${PLUGIN_JAR_NAME}" "${TARGET}/jars/3rdparty/"
  cp "${BUILD_TMPDIR}/target/${PLUGIN_JAR_NAME}" "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" 2>/dev/null || true
  rm -rf "$BUILD_TMPDIR"
else
  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl cp "${NS_FLAG[@]}" \
      "${SCRIPT_DIR}/jars/${PLUGIN_JAR_NAME}" "${pod}:/tmp/${PLUGIN_JAR_NAME}"
    kubectl exec "${NS_FLAG[@]}" "$pod" -- bash -c \
      "cp /tmp/${PLUGIN_JAR_NAME} /opt/dremio/jars/3rdparty/ && \
       chown dremio:dremio /opt/dremio/jars/3rdparty/${PLUGIN_JAR_NAME} 2>/dev/null || true"
    ok "Deployed to ${pod}"
  done <<< "$(kubectl get pods "${NS_FLAG[@]}" \
    --field-selector=status.phase=Running \
    -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' 2>/dev/null || echo "$TARGET")"
fi

ok "JAR deployed"

step "7" "Restarting Dremio..."
if [[ "$MODE" == "docker" ]]; then
  docker restart "$TARGET" > /dev/null
  for i in $(seq 1 60); do
    sleep 3
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9047/apiv2/info" 2>/dev/null || true)
    if [[ "$HTTP" == "200" || "$HTTP" == "404" ]]; then
      ok "Dremio is up (${i} × 3s)"
      break
    fi
    echo -n "."
  done
  echo ""
elif [[ "$MODE" == "local" ]]; then
  warn "Restart Dremio manually: \$DREMIO_HOME/bin/dremio restart"
else
  warn "Restart Dremio pods: kubectl rollout restart statefulset/<name>${NAMESPACE:+ -n $NAMESPACE}"
fi

echo ""
echo -e "${GREEN}${BOLD}Rebuild complete.${RESET}"
echo -e "  Dremio version : ${DETECTED_DREMIO}"
echo -e "  Plugin JAR     : ${PLUGIN_JAR_NAME}"
echo ""
