#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_NAME="dremio-cosmosdb-connector-1.0.0.jar"
JAR_PATH="$SCRIPT_DIR/jars/$JAR_NAME"
DREMIO_PLUGIN_DIR="/opt/dremio/jars/3rdparty"

if [[ ! -f "$JAR_PATH" ]]; then
  echo "ERROR: JAR not found at $JAR_PATH — run rebuild.sh first"
  exit 1
fi

echo "=== Installing Cosmos DB Connector ==="
docker cp "$JAR_PATH" "try-dremio:$DREMIO_PLUGIN_DIR/$JAR_NAME"
echo "Installed to $DREMIO_PLUGIN_DIR/$JAR_NAME"

echo "=== Restarting Dremio ==="
docker exec -u root try-dremio bash -c "kill \$(cat /var/run/dremio/dremio.pid 2>/dev/null) 2>/dev/null || true" || true
sleep 5
docker exec -u dremio try-dremio bash -c "/opt/dremio/bin/dremio start"
echo "Dremio restarting — wait ~30 seconds before adding the source."
