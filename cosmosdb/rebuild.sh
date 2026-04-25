#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="/tmp/cosmosdb-connector-build"
JAR_NAME="dremio-cosmosdb-connector-1.0.0.jar"

echo "=== Cosmos DB Connector Build ==="

# Copy source into container (clean first)
rm -rf "$BUILD_DIR"
cp -r "$SCRIPT_DIR" "$BUILD_DIR"
docker exec -u root try-dremio bash -c "rm -rf /tmp/cosmosdb-connector-build"
docker cp "$BUILD_DIR" "try-dremio:/tmp/cosmosdb-connector-build"

# Fix ownership so dremio user can write jars/
docker exec -u root try-dremio bash -c "chown -R dremio:dremio /tmp/cosmosdb-connector-build"

# Build inside the running Dremio container
docker exec -u dremio try-dremio bash -c "
  cd /tmp/cosmosdb-connector-build &&
  mvn -q package -DskipTests &&
  echo 'Build successful: jars/$JAR_NAME'
"

echo "=== Copying JAR to jars/ ==="
docker cp "try-dremio:/tmp/cosmosdb-connector-build/jars/$JAR_NAME" "$SCRIPT_DIR/jars/$JAR_NAME"
echo "JAR written to: $SCRIPT_DIR/jars/$JAR_NAME"
