#!/usr/bin/env bash
# First-time install of the Cosmos DB connector into a running Dremio instance.
# For version-aware rebuilds (e.g. after a Dremio upgrade), use rebuild.sh instead.
set -euo pipefail

JAR="jars/dremio-cosmosdb-connector-1.0.0.jar"
CONTAINER="${DREMIO_CONTAINER:-try-dremio}"
DREMIO_JARS="${DREMIO_JARS:-/opt/dremio/jars/3rdparty}"

if [[ ! -f "$JAR" ]]; then
  echo "ERROR: $JAR not found. Run rebuild.sh first."
  exit 1
fi

if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER}$"; then
  echo "Installing into Docker container: $CONTAINER"
  docker cp "$JAR" "${CONTAINER}:${DREMIO_JARS}/"
  docker restart "$CONTAINER"
  echo "✅ Installed. Dremio is restarting..."
else
  echo "Container '$CONTAINER' not running. Copying to local path: $DREMIO_JARS"
  cp "$JAR" "$DREMIO_JARS/"
  echo "✅ Copied. Restart Dremio to load the connector."
fi
