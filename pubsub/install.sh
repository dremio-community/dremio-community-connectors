#!/usr/bin/env bash
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"

DREMIO_CONTAINER="${1:-try-dremio}"
DREMIO_JAR_DIR="/opt/dremio/jars/3rdparty"

PLUGIN_JAR=$(find "$DIR/jars" -name "*-plugin.jar" | head -1)
if [[ -z "$PLUGIN_JAR" ]]; then
  echo "ERROR: No plugin JAR found in jars/. Run ./rebuild.sh first."
  exit 1
fi

echo "==> Installing $(basename "$PLUGIN_JAR") to $DREMIO_CONTAINER:$DREMIO_JAR_DIR"
docker cp "$PLUGIN_JAR" "$DREMIO_CONTAINER:$DREMIO_JAR_DIR/"
docker exec "$DREMIO_CONTAINER" ls -lh "$DREMIO_JAR_DIR/$(basename "$PLUGIN_JAR")"

echo ""
echo "==> Restarting Dremio..."
docker restart "$DREMIO_CONTAINER"
echo "Done. Dremio should be available in ~30 seconds."
