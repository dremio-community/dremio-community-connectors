#!/usr/bin/env bash
# install.sh — build and deploy the Spanner connector JAR to a running Dremio instance
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_NAME="dremio-spanner-plugin-*.jar"

# ── Build ──────────────────────────────────────────────────────────────────────
echo "Building Spanner connector..."
cd "$SCRIPT_DIR"
mvn package -DskipTests -q
JAR_PATH=$(ls target/dremio-spanner-plugin-*.jar 2>/dev/null | head -1)
if [[ -z "$JAR_PATH" ]]; then
  echo "ERROR: build produced no JAR in target/"
  exit 1
fi
echo "Built: $JAR_PATH"

# ── Deploy ─────────────────────────────────────────────────────────────────────
DREMIO_HOME="${DREMIO_HOME:-/opt/dremio}"
JARS_DIR="${JARS_DIR:-$DREMIO_HOME/jars/3rdparty}"

if [[ -d "$JARS_DIR" ]]; then
  echo "Copying to $JARS_DIR ..."
  cp "$JAR_PATH" "$JARS_DIR/"
  echo "Done. Restart Dremio to load the connector."
else
  # Docker path
  CONTAINER="${DREMIO_CONTAINER:-dremio}"
  echo "Copying into Docker container '$CONTAINER'..."
  docker cp "$JAR_PATH" "$CONTAINER:/opt/dremio/jars/3rdparty/"
  echo "Restarting container..."
  docker restart "$CONTAINER"
  echo "Done. Dremio is restarting."
fi
