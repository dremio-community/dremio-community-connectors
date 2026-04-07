#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Dremio Excel Importer — build script
#
# Usage:
#   bash install.sh                        # build locally (requires Java 11 + Maven)
#   bash install.sh --docker <container>   # build inside a Docker container
#   bash install.sh --skip-tests           # skip unit tests
#   bash install.sh --help
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_OUT="$SCRIPT_DIR/jars/dremio-excel-importer.jar"

DOCKER_CONTAINER=""
SKIP_TESTS=false

# ── Parse args ──
for arg in "$@"; do
  case "$arg" in
    --help|-h)
      echo "Usage: bash install.sh [--docker <container>] [--skip-tests]"
      echo ""
      echo "  --docker <container>   Build inside a running Docker container"
      echo "                         (use when Java/Maven not installed locally)"
      echo "  --skip-tests           Skip unit tests during build"
      exit 0
      ;;
    --docker)
      DOCKER_NEXT=true ;;
    --skip-tests)
      SKIP_TESTS=true ;;
    *)
      if [ "${DOCKER_NEXT:-false}" = "true" ]; then
        DOCKER_CONTAINER="$arg"
        DOCKER_NEXT=false
      fi
      ;;
  esac
done

# ── Build ──
MVN_OPTS="-q"
if [ "$SKIP_TESTS" = "true" ]; then
  MVN_OPTS="$MVN_OPTS -DskipTests"
fi

if [ -n "$DOCKER_CONTAINER" ]; then
  echo "Building inside Docker container: $DOCKER_CONTAINER"
  echo ""

  CONTAINER_PATH="/tmp/dremio-excel-importer"

  docker cp "$SCRIPT_DIR" "${DOCKER_CONTAINER}:${CONTAINER_PATH}" 2>/dev/null || true
  docker exec --user root "$DOCKER_CONTAINER" chmod -R 777 "$CONTAINER_PATH" 2>/dev/null || true
  docker exec "$DOCKER_CONTAINER" bash -c "cd $CONTAINER_PATH && mvn package $MVN_OPTS 2>&1"
  docker cp "${DOCKER_CONTAINER}:${CONTAINER_PATH}/jars/dremio-excel-importer.jar" "$JAR_OUT"

else
  echo "Building locally..."
  echo ""

  # Check Java
  if ! command -v java &>/dev/null; then
    echo "ERROR: Java not found. Install Java 11+ and ensure 'java' is on your PATH."
    echo "See INSTALL.md for instructions."
    exit 1
  fi
  JAVA_VER=$(java -version 2>&1 | awk -F '"' '/version/ {print $2}' | cut -d. -f1)
  if [ -n "$JAVA_VER" ] && [ "$JAVA_VER" -lt 11 ] 2>/dev/null; then
    echo "ERROR: Java 11 or later is required (found: $JAVA_VER)."
    exit 1
  fi

  # Check Maven
  if ! command -v mvn &>/dev/null; then
    echo "ERROR: Maven not found. Install Maven 3.6+ and ensure 'mvn' is on your PATH."
    echo "See INSTALL.md for instructions."
    exit 1
  fi

  cd "$SCRIPT_DIR"
  mvn package $MVN_OPTS
fi

echo ""
echo "Build complete."
echo "  JAR : $JAR_OUT"
ls -lh "$JAR_OUT"
echo ""
echo "Verify with:"
echo "  java -jar \"$JAR_OUT\" --help"
