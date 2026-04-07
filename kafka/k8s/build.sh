#!/usr/bin/env bash
# =============================================================================
# Build script for Dremio Kafka Connector Kubernetes images
#
# Builds two Docker images:
#   1. Full custom Dremio image with Kafka plugin baked in (recommended)
#   2. Minimal init-container image that just holds the plugin JAR
#
# Usage:
#   ./build.sh [OPTIONS]
#
# Options:
#   --registry REGISTRY     Container registry prefix (e.g. gcr.io/my-project)
#   --dremio-image IMAGE    Base Dremio Enterprise image (default: dremio/dremio-ee)
#   --dremio-tag TAG        Dremio version tag (default: 26.0.5)
#   --plugin-version VER    Plugin image tag (default: 1.0.0)
#   --push                  Push images to registry after build
#   --init-only             Build only the init container image (skip full image)
#   --full-only             Build only the full custom image (skip init image)
#   --help                  Show this help
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"   # connector root

# Defaults
REGISTRY=""
DREMIO_IMAGE="dremio/dremio-ee"
DREMIO_TAG="26.0.5"
PLUGIN_VERSION="1.0.0"
PUSH=false
BUILD_FULL=true
BUILD_INIT=true

while [[ $# -gt 0 ]]; do
  case "$1" in
    --registry)       REGISTRY="$2";       shift 2 ;;
    --dremio-image)   DREMIO_IMAGE="$2";   shift 2 ;;
    --dremio-tag)     DREMIO_TAG="$2";     shift 2 ;;
    --plugin-version) PLUGIN_VERSION="$2"; shift 2 ;;
    --push)           PUSH=true;           shift ;;
    --init-only)      BUILD_FULL=false;    shift ;;
    --full-only)      BUILD_INIT=false;    shift ;;
    --help)
      sed -n '/^# Usage/,/^# =====/p' "$0" | grep -v '====='
      exit 0
      ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

# Construct image names
if [[ -n "$REGISTRY" ]]; then
  FULL_IMAGE="${REGISTRY}/dremio-with-kafka:${DREMIO_TAG}"
  INIT_IMAGE="${REGISTRY}/dremio-kafka-plugin-init:${PLUGIN_VERSION}"
else
  FULL_IMAGE="dremio-with-kafka:${DREMIO_TAG}"
  INIT_IMAGE="dremio-kafka-plugin-init:${PLUGIN_VERSION}"
fi

echo "=== Dremio Kafka Connector — Kubernetes Image Builder ==="
echo "  Base Dremio image : ${DREMIO_IMAGE}:${DREMIO_TAG}"
echo "  Full image        : ${FULL_IMAGE}"
echo "  Init image        : ${INIT_IMAGE}"
echo "  Push after build  : ${PUSH}"
echo ""

# ---------------------------------------------------------------------------
# Build full custom Dremio image (multi-stage — includes Maven build)
# ---------------------------------------------------------------------------
if [[ "$BUILD_FULL" == true ]]; then
  echo "--- Building full custom Dremio image ---"
  docker build \
    --build-arg DREMIO_IMAGE="${DREMIO_IMAGE}" \
    --build-arg DREMIO_TAG="${DREMIO_TAG}" \
    -t "${FULL_IMAGE}" \
    -f "${SCRIPT_DIR}/k8s/Dockerfile" \
    "${SCRIPT_DIR}"
  echo "Built: ${FULL_IMAGE}"

  if [[ "$PUSH" == true ]]; then
    docker push "${FULL_IMAGE}"
    echo "Pushed: ${FULL_IMAGE}"
  fi
fi

# ---------------------------------------------------------------------------
# Build init container image (requires pre-built JAR)
# ---------------------------------------------------------------------------
if [[ "$BUILD_INIT" == true ]]; then
  echo ""
  echo "--- Building init container image ---"

  if ! ls "${SCRIPT_DIR}/target/dremio-kafka-connector-"*"-plugin.jar" &>/dev/null; then
    echo ""
    echo "NOTE: No plugin JAR found in target/. Running Maven build first..."
    echo "      (This requires Dremio JARs in your local Maven repo.)"
    echo "      See INSTALL.md for the manual build steps."
    echo ""
    (cd "${SCRIPT_DIR}" && mvn package -DskipTests -q) || {
      echo "ERROR: Maven build failed. Install Dremio JARs into your local Maven repo first." >&2
      echo "       Or use the full image build (--full-only) which handles this automatically." >&2
      exit 1
    }
  fi

  docker build \
    -t "${INIT_IMAGE}" \
    -f "${SCRIPT_DIR}/k8s/Dockerfile.init" \
    "${SCRIPT_DIR}"
  echo "Built: ${INIT_IMAGE}"

  if [[ "$PUSH" == true ]]; then
    docker push "${INIT_IMAGE}"
    echo "Pushed: ${INIT_IMAGE}"
  fi
fi

# ---------------------------------------------------------------------------
# Print next steps
# ---------------------------------------------------------------------------
echo ""
echo "=== Done ==="
echo ""
if [[ "$BUILD_FULL" == true ]]; then
  echo "To use the custom image, update your Helm values:"
  echo "  image: \"${FULL_IMAGE%:*}\""
  echo "  imageTag: \"${FULL_IMAGE##*:}\""
  echo "  See: k8s/helm-values-custom-image.yaml"
  echo ""
fi
if [[ "$BUILD_INIT" == true ]]; then
  echo "To use the init container approach, update helm-values-init-container.yaml"
  echo "with image: ${INIT_IMAGE}"
  echo "  See: k8s/helm-values-init-container.yaml"
fi
