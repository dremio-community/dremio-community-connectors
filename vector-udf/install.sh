#!/usr/bin/env bash
# install.sh — Build and deploy the Dremio Vector UDF to a running Dremio container
#
# Usage:
#   ./install.sh [OPTIONS]
#
# Options:
#   --container  NAME    Dremio Docker container name (default: try-dremio)
#   --jar        PATH    Pre-built JAR path (skips Maven build)
#   --no-restart         Don't restart Dremio after installing
#   --help               Show this help

set -euo pipefail

CONTAINER="try-dremio"
JAR_PATH=""
RESTART=true
DREMIO_JAR_DIR="/opt/dremio/jars/3rdparty"

while [[ $# -gt 0 ]]; do
  case $1 in
    --container)  CONTAINER="$2";  shift 2 ;;
    --jar)        JAR_PATH="$2";   shift 2 ;;
    --no-restart) RESTART=false;   shift ;;
    --help|-h)
      sed -n '3,11p' "$0" | sed 's/^# *//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Build if no JAR provided
if [[ -z "$JAR_PATH" ]]; then
  echo "Building JAR with Maven..."
  cd "$SCRIPT_DIR"
  mvn package -DskipTests -q
  JAR_PATH=$(find "$SCRIPT_DIR/target" -name "dremio-vector-udf-*.jar" ! -name "*sources*" | head -1)
  if [[ -z "$JAR_PATH" ]]; then
    echo "ERROR: Could not find built JAR in target/"
    exit 1
  fi
fi

JAR_NAME=$(basename "$JAR_PATH")
echo "JAR: $JAR_PATH"

# Detect docker
DOCKER=$(command -v docker 2>/dev/null || echo "/Applications/Docker.app/Contents/Resources/bin/docker")
if [[ ! -x "$DOCKER" ]]; then
  echo "ERROR: docker not found"
  exit 1
fi

# Check container is running
if ! $DOCKER ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "ERROR: Container '$CONTAINER' is not running"
  exit 1
fi

echo "Deploying to container '$CONTAINER'..."
$DOCKER cp "$JAR_PATH" "${CONTAINER}:${DREMIO_JAR_DIR}/${JAR_NAME}"

echo "Installed: ${DREMIO_JAR_DIR}/${JAR_NAME}"

if $RESTART; then
  echo "Restarting Dremio..."
  $DOCKER restart "$CONTAINER"
  echo "Waiting for Dremio to come up..."
  for i in $(seq 1 30); do
    STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9047/ 2>/dev/null || echo "000")
    if [[ "$STATUS" == "200" ]]; then
      echo "Dremio is up."
      break
    fi
    sleep 5
  done
fi

echo ""
echo "✓ Vector UDF installed successfully"
echo ""
echo "  Test with:"
echo "    SELECT COSINE_SIMILARITY('[1.0, 0.0, 0.0]', '[0.0, 1.0, 0.0]')"
echo "    SELECT L2_DISTANCE('[0.0, 0.0]', '[3.0, 4.0]')"
echo "    SELECT VECTOR_DIMS('[0.1, 0.2, 0.3, 0.4]')"
