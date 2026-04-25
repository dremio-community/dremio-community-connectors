#!/usr/bin/env bash
# First-time install of the Zendesk connector into a running Dremio container or local install.
set -euo pipefail

JAR="jars/dremio-zendesk-connector-1.0.0.jar"
CONTAINER="${DREMIO_CONTAINER:-try-dremio}"
DREMIO_JARS="${DREMIO_JARS:-/opt/dremio/jars/3rdparty}"

if [ ! -f "$JAR" ]; then
  echo "ERROR: $JAR not found. Run 'mvn package' first."
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
