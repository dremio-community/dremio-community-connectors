#!/usr/bin/env bash
# =============================================================================
# Build and push Docker images for the Dremio Apache Pinot Connector.
#
# Usage:
#   ./k8s/build.sh [--registry <registry>] [--dremio-tag <tag>] [--push]
#
# Options:
#   --registry  <registry>  Docker registry prefix (e.g. myregistry.io/myorg)
#   --dremio-tag <tag>      Dremio base image tag (default: 26.0.5)
#   --push                  Push images after building
#   -h, --help              Show this help
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

REGISTRY=""
DREMIO_TAG="26.0.5"
PUSH=false

CUSTOM_IMAGE="dremio-with-pinot"
INIT_IMAGE="dremio-pinot-connector-init"
CONNECTOR_VERSION="1.0.0-SNAPSHOT"

while [[ $# -gt 0 ]]; do
  case $1 in
    --registry)   REGISTRY="$2/"; shift 2 ;;
    --dremio-tag) DREMIO_TAG="$2"; shift 2 ;;
    --push)       PUSH=true; shift ;;
    -h|--help)
      sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

echo "==> Building connector JAR..."
cd "$REPO_ROOT"
mvn package -q -DskipTests

echo "==> Building custom Dremio image: ${REGISTRY}${CUSTOM_IMAGE}:${DREMIO_TAG}"
docker build \
  --build-arg DREMIO_TAG="${DREMIO_TAG}" \
  -f k8s/Dockerfile \
  -t "${REGISTRY}${CUSTOM_IMAGE}:${DREMIO_TAG}" \
  .

echo "==> Building init container image: ${REGISTRY}${INIT_IMAGE}:${CONNECTOR_VERSION}"
docker build \
  --build-arg CONNECTOR_JAR="dremio-pinot-connector-${CONNECTOR_VERSION}-plugin.jar" \
  -f k8s/Dockerfile.init \
  -t "${REGISTRY}${INIT_IMAGE}:${CONNECTOR_VERSION}" \
  .

if [ "$PUSH" = true ]; then
  echo "==> Pushing images..."
  docker push "${REGISTRY}${CUSTOM_IMAGE}:${DREMIO_TAG}"
  docker push "${REGISTRY}${INIT_IMAGE}:${CONNECTOR_VERSION}"
fi

echo "==> Done."
echo "    Custom image : ${REGISTRY}${CUSTOM_IMAGE}:${DREMIO_TAG}"
echo "    Init image   : ${REGISTRY}${INIT_IMAGE}:${CONNECTOR_VERSION}"
