#!/usr/bin/env bash
# =============================================================================
# Dremio Apache Pinot Connector — Install Script
#
# Detects the running Dremio version, installs required JARs, builds the
# connector, and deploys it to the target Dremio instance.
#
# USAGE
#   ./install.sh [--docker CONTAINER] [--local DREMIO_HOME] [--k8s POD]
#
# OPTIONS
#   --docker CONTAINER   Deploy to a running Docker container (default: try-dremio)
#   --local  DREMIO_HOME Deploy to a bare-metal Dremio installation
#   --k8s    POD         Deploy to a Kubernetes pod (coordinator)
#   --namespace NS (-n)  Kubernetes namespace (K8s mode only)
#   --dry-run            Detect version and show what would change
#   -h, --help           Show this help
# =============================================================================
set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

MODE="docker"
TARGET="try-dremio"
K8S_NAMESPACE=""
DRY_RUN=false

while [[ $# -gt 0 ]]; do
  case $1 in
    --docker)     MODE="docker"; TARGET="$2"; shift 2 ;;
    --local)      MODE="local";  TARGET="$2"; shift 2 ;;
    --k8s)        MODE="k8s";    TARGET="$2"; shift 2 ;;
    --namespace|-n) K8S_NAMESPACE="$2"; shift 2 ;;
    --dry-run)    DRY_RUN=true; shift ;;
    -h|--help)    sed -n '3,17p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

NS_ARG=""
[ -n "$K8S_NAMESPACE" ] && NS_ARG="-n $K8S_NAMESPACE"

echo "==> Dremio Apache Pinot Connector Installer"
echo "    Mode: $MODE | Target: $TARGET"

# -- Detect Dremio version ----------------------------------------------------
echo "==> Detecting Dremio version..."
case $MODE in
  docker)
    DREMIO_VERSION=$(docker exec "$TARGET" bash -c \
      "ls /opt/dremio/jars/dremio-common-*.jar" \
      | head -1 | sed 's/.*dremio-common-\(.*\)\.jar/\1/')
    JAR_DIR="/opt/dremio/jars"
    ;;
  local)
    DREMIO_VERSION=$(ls "$TARGET"/jars/dremio-common-*.jar \
      | head -1 | sed 's/.*dremio-common-\(.*\)\.jar/\1/')
    JAR_DIR="$TARGET/jars"
    ;;
  k8s)
    DREMIO_VERSION=$(kubectl exec $NS_ARG "$TARGET" -- bash -c \
      "ls /opt/dremio/jars/dremio-common-*.jar" \
      | head -1 | sed 's/.*dremio-common-\(.*\)\.jar/\1/')
    JAR_DIR="/opt/dremio/jars"
    ;;
esac

echo "    Detected Dremio version: $DREMIO_VERSION"

if [ "$DRY_RUN" = true ]; then
  echo "==> Dry run complete."
  exit 0
fi

# -- Install Dremio JARs into Maven local repo --------------------------------
install_jar() {
  local GROUP="$1" ARTIFACT="$2"
  local TMP_JAR="/tmp/pinot-install-${ARTIFACT}.jar"

  case $MODE in
    docker) docker cp "$TARGET:${JAR_DIR}/${ARTIFACT}-${DREMIO_VERSION}.jar" "$TMP_JAR" 2>/dev/null || return 0 ;;
    local)  cp "${JAR_DIR}/${ARTIFACT}-${DREMIO_VERSION}.jar" "$TMP_JAR" 2>/dev/null || return 0 ;;
    k8s)    kubectl cp $NS_ARG "${TARGET}:${JAR_DIR}/${ARTIFACT}-${DREMIO_VERSION}.jar" "$TMP_JAR" 2>/dev/null || return 0 ;;
  esac

  mvn install:install-file -q \
    -Dfile="$TMP_JAR" \
    -DgroupId="$GROUP" \
    -DartifactId="$ARTIFACT" \
    -Dversion="$DREMIO_VERSION" \
    -Dpackaging=jar
  rm -f "$TMP_JAR"
}

echo "==> Installing Dremio JARs into local Maven repo..."
install_jar com.dremio           dremio-sabot-kernel
install_jar com.dremio           dremio-common
install_jar com.dremio           dremio-sabot-kernel-proto
install_jar com.dremio           dremio-sabot-vector-tools
install_jar com.dremio           dremio-services-namespace
install_jar com.dremio           dremio-connector
install_jar com.dremio           dremio-sabot-logical
install_jar com.dremio           dremio-common-core
install_jar com.dremio           dremio-services-datastore
install_jar com.dremio           dremio-services-options
install_jar com.dremio.plugin    dremio-plugin-common
install_jar com.dremio.services  dremio-services-credentials
install_jar com.dremio.plugins   dremio-ce-jdbc-plugin
install_jar com.dremio.plugins   dremio-ce-jdbc-fetcher-api

# -- Update pom.xml version ---------------------------------------------------
sed -i.bak "s|<dremio.version>.*</dremio.version>|<dremio.version>${DREMIO_VERSION}</dremio.version>|" "$SCRIPT_DIR/pom.xml"

# -- Build --------------------------------------------------------------------
echo "==> Building connector JAR..."
cd "$SCRIPT_DIR"
mvn package -q -DskipTests

PLUGIN_JAR=$(ls target/dremio-pinot-connector-*-plugin.jar | head -1)
echo "    Built: $PLUGIN_JAR"

# -- Deploy -------------------------------------------------------------------
echo "==> Deploying to $MODE/$TARGET..."
case $MODE in
  docker)
    docker exec -u root "$TARGET" mkdir -p /opt/dremio/jars/3rdparty
    docker cp "$PLUGIN_JAR" "$TARGET:/opt/dremio/jars/3rdparty/"
    docker exec -u root "$TARGET" chown dremio:dremio \
      /opt/dremio/jars/3rdparty/$(basename "$PLUGIN_JAR")
    echo "==> Restarting Dremio..."
    docker restart "$TARGET"
    ;;
  local)
    cp "$PLUGIN_JAR" "$TARGET/jars/3rdparty/"
    echo "==> Please restart Dremio manually."
    ;;
  k8s)
    kubectl cp $NS_ARG "$PLUGIN_JAR" "${TARGET}:/opt/dremio/jars/3rdparty/$(basename $PLUGIN_JAR)"
    echo "==> Please perform a rolling restart of your Dremio pods."
    ;;
esac

echo "==> Installation complete!"
echo "    In Dremio UI: Add Source → Apache Pinot"
