#!/usr/bin/env bash
# Dremio Google Ads Connector — Version-aware Rebuild Script
#
# USAGE
#   ./rebuild.sh [--docker CONTAINER] [--local PATH] [--force] [--dry-run]
set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
POM_XML="${SCRIPT_DIR}/pom.xml"
JAR_NAME="dremio-googleads-connector-1.0.0-SNAPSHOT.jar"

BOLD='\033[1m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
RED='\033[0;31m'; CYAN='\033[0;36m'; RESET='\033[0m'
step()  { echo -e "\n${BOLD}[${1}]${RESET} ${2}"; }
ok()    { echo -e "    ${GREEN}✓${RESET}  ${1}"; }
warn()  { echo -e "    ${YELLOW}⚠${RESET}  ${1}"; }
err()   { echo -e "    ${RED}✗${RESET}  ${1}" >&2; }
info()  { echo -e "    ${CYAN}→${RESET}  ${1}"; }
die()   { err "$*"; exit 1; }

MODE="docker"; TARGET="try-dremio"; FORCE=false; DRY_RUN=false
while [[ $# -gt 0 ]]; do
  case "$1" in
    --docker)  MODE="docker"; TARGET="${2:-try-dremio}"; shift 2 || shift ;;
    --local)   MODE="local";  TARGET="${2:-/opt/dremio}"; shift 2 || shift ;;
    --force)   FORCE=true; shift ;;
    --dry-run) DRY_RUN=true; shift ;;
    *) die "Unknown argument: $1" ;;
  esac
done

echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════════════${RESET}"
echo -e "${BOLD}${CYAN}   Dremio Google Ads Connector — Rebuild${RESET}"
echo -e "${BOLD}${CYAN}══════════════════════════════════════════════════${RESET}"

case "$MODE" in
  docker)
    docker ps --format '{{.Names}}' | grep -q "^${TARGET}$" || die "Container '${TARGET}' not running"
    DREMIO_JARS_DIR="/opt/dremio/jars"
    info "Target: Docker container '${TARGET}'" ;;
  local)
    [[ -d "$TARGET" ]] || die "Dremio home '${TARGET}' not found"
    DREMIO_JARS_DIR="${TARGET}/jars"
    info "Target: local installation at '${TARGET}'" ;;
esac

run() { [[ "$MODE" == "docker" ]] && docker exec "$TARGET" bash -c "$1" || bash -c "$1"; }

step "1" "Detecting Dremio version..."
DREMIO_JAR=$(run "ls ${DREMIO_JARS_DIR}/dremio-common-*.jar 2>/dev/null | grep -v proto | grep -v sources | head -1")
[[ -n "$DREMIO_JAR" ]] || die "No dremio-common JAR found"
DETECTED_DREMIO=$(basename "$DREMIO_JAR" .jar | sed 's/^dremio-common-//')
ok "Dremio : ${DETECTED_DREMIO}"

ARROW_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/arrow-vector-*.jar 2>/dev/null | head -1" || true)
DETECTED_ARROW=""; [[ -n "$ARROW_JAR" ]] && DETECTED_ARROW=$(basename "$ARROW_JAR" .jar | sed 's/^arrow-vector-//')
[[ -n "$DETECTED_ARROW" ]] && ok "Arrow  : ${DETECTED_ARROW}"

CALCITE_JAR=$(run "ls ${DREMIO_JARS_DIR}/3rdparty/calcite-core-*.jar 2>/dev/null | head -1" || true)
DETECTED_CALCITE=""; [[ -n "$CALCITE_JAR" ]] && DETECTED_CALCITE=$(basename "$CALCITE_JAR" .jar | sed 's/^calcite-core-//')
[[ -n "$DETECTED_CALCITE" ]] && ok "Calcite: ${DETECTED_CALCITE}"

step "2" "Comparing against pom.xml..."
CURRENT_DREMIO=$(sed -n 's|.*<dremio.version>\([^<]*\)</dremio.version>.*|\1|p' "$POM_XML" | head -1)
VERSIONS_MATCH=true
[[ "$DETECTED_DREMIO" != "$CURRENT_DREMIO" ]] && VERSIONS_MATCH=false
if $VERSIONS_MATCH && ! $FORCE; then
  ok "Already targeting the running version — nothing to rebuild."
  info "Use --force to rebuild anyway."
  exit 0
fi
$DRY_RUN && { info "Dry-run: no changes."; exit 0; }

step "3" "Updating pom.xml..."
cp "$POM_XML" "${POM_XML}.bak"
sed -i.tmp "s|<dremio.version>[^<]*</dremio.version>|<dremio.version>${DETECTED_DREMIO}</dremio.version>|g" "$POM_XML"
[[ -n "$DETECTED_ARROW" ]] && sed -i.tmp "s|<arrow.version>[^<]*</arrow.version>|<arrow.version>${DETECTED_ARROW}</arrow.version>|g" "$POM_XML"
rm -f "${POM_XML}.tmp"
ok "pom.xml updated"

step "4" "Installing Dremio JARs into Maven local repo..."
install_jar() {
  local path="$1" group="$2" art="$3" ver="$4"
  if [[ "$MODE" == "docker" ]]; then
    docker exec "$TARGET" bash -c "mvn install:install-file -q -Dfile='${path}' -DgroupId='${group}' -DartifactId='${art}' -Dversion='${ver}' -Dpackaging=jar 2>/dev/null" && echo "  ✓ ${art}" || echo "  ⚠ skip ${art}"
  else
    mvn install:install-file -q -Dfile="${path}" -DgroupId="${group}" -DartifactId="${art}" -Dversion="${ver}" -Dpackaging=jar 2>/dev/null && echo "  ✓ ${art}" || echo "  ⚠ skip ${art}"
  fi
}
if [[ "$MODE" == "docker" ]] && ! docker exec "$TARGET" which mvn > /dev/null 2>&1; then
  docker exec -u root "$TARGET" bash -c "apt-get update -qq && apt-get install -y -qq maven 2>&1 | tail -2"
fi
J="${DREMIO_JARS_DIR}"; TP="${J}/3rdparty"; DV="${DETECTED_DREMIO}"
install_jar "${J}/dremio-common-${DV}.jar"               com.dremio          dremio-common               "$DV"
install_jar "${J}/dremio-sabot-kernel-${DV}.jar"         com.dremio          dremio-sabot-kernel         "$DV"
install_jar "${J}/dremio-sabot-kernel-${DV}-proto.jar"   com.dremio          dremio-sabot-kernel-proto   "$DV"
install_jar "${J}/dremio-sabot-vector-tools-${DV}.jar"   com.dremio          dremio-sabot-vector-tools   "$DV"
install_jar "${J}/dremio-connector-${DV}.jar"            com.dremio          dremio-connector            "$DV"
install_jar "${J}/dremio-sabot-logical-${DV}.jar"        com.dremio          dremio-sabot-logical        "$DV"
install_jar "${J}/dremio-common-core-${DV}.jar"          com.dremio          dremio-common-core          "$DV"
install_jar "${J}/dremio-plugin-common-${DV}.jar"        com.dremio.plugin   dremio-plugin-common        "$DV"
install_jar "${J}/dremio-services-namespace-${DV}.jar"   com.dremio          dremio-services-namespace   "$DV"
install_jar "${J}/dremio-services-credentials-${DV}.jar" com.dremio.services dremio-services-credentials "$DV"
install_jar "${J}/dremio-services-datastore-${DV}.jar"   com.dremio          dremio-services-datastore   "$DV"
[[ -n "$DETECTED_ARROW" ]] && {
  AV="${DETECTED_ARROW}"
  install_jar "${TP}/arrow-vector-${AV}.jar"      org.apache.arrow arrow-vector      "$AV"
  install_jar "${TP}/arrow-memory-core-${AV}.jar" org.apache.arrow arrow-memory-core "$AV"
}
[[ -n "$DETECTED_CALCITE" ]] && {
  install_jar "${TP}/calcite-core-${DETECTED_CALCITE}.jar"   org.apache.calcite calcite-core   "$DETECTED_CALCITE"
  install_jar "${TP}/calcite-linq4j-${DETECTED_CALCITE}.jar" org.apache.calcite calcite-linq4j "$DETECTED_CALCITE"
}
ok "JARs installed"

step "5" "Building connector..."
mkdir -p "${SCRIPT_DIR}/jars"
BUILD_FAILED=false
if [[ "$MODE" == "docker" ]]; then
  docker exec "$TARGET" mkdir -p /tmp/googleads-rebuild
  docker cp "${SCRIPT_DIR}/src"     "${TARGET}:/tmp/googleads-rebuild/"
  docker cp "${SCRIPT_DIR}/pom.xml" "${TARGET}:/tmp/googleads-rebuild/"
  docker exec -u root "$TARGET" bash -c "chmod -R 777 /tmp/googleads-rebuild"
  docker exec "$TARGET" bash -c "cd /tmp/googleads-rebuild && mvn package -DskipTests --batch-mode 2>&1" || BUILD_FAILED=true
else
  BUILD_TMPDIR="$(mktemp -d)"
  cp -r "${SCRIPT_DIR}/src" "$BUILD_TMPDIR/" && cp "${SCRIPT_DIR}/pom.xml" "$BUILD_TMPDIR/"
  (cd "$BUILD_TMPDIR" && mvn package -DskipTests --batch-mode 2>&1) || BUILD_FAILED=true
fi

if $BUILD_FAILED; then
  err "Build FAILED — check output above"
  cp "${POM_XML}.bak" "$POM_XML" && warn "pom.xml restored"
  exit 2
fi
ok "Build successful"

step "6" "Deploying JAR..."
if [[ "$MODE" == "docker" ]]; then
  docker cp "${TARGET}:/tmp/googleads-rebuild/target/${JAR_NAME}" "${SCRIPT_DIR}/jars/${JAR_NAME}"
  docker cp "${SCRIPT_DIR}/jars/${JAR_NAME}" "${TARGET}:/opt/dremio/jars/3rdparty/${JAR_NAME}"
else
  cp "${BUILD_TMPDIR}/target/${JAR_NAME}" "${TARGET}/jars/3rdparty/"
  rm -rf "$BUILD_TMPDIR"
fi
ok "JAR deployed"

step "7" "Restarting Dremio..."
if [[ "$MODE" == "docker" ]]; then
  docker restart "$TARGET" > /dev/null
  for i in $(seq 1 60); do
    sleep 3
    HTTP=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9047/apiv2/info" 2>/dev/null || true)
    [[ "$HTTP" == "200" || "$HTTP" == "404" ]] && { ok "Dremio up ($((i*3))s)"; break; }
    echo -n "."
  done; echo
else
  warn "Bare-metal: restart Dremio manually to load the connector."
fi

echo -e "\n${GREEN}${BOLD}Done.${RESET} Connector: ${JAR_NAME}"
