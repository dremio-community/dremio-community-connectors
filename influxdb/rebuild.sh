#!/usr/bin/env bash
# Builds the InfluxDB connector inside the running Dremio container (where Maven lives).
set -euo pipefail
export PATH="/usr/local/bin:/opt/homebrew/bin:/usr/bin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${DREMIO_CONTAINER:-try-dremio}"
JAR_NAME="dremio-influxdb-connector-1.0.0.jar"

echo "Building $JAR_NAME inside container '$CONTAINER'..."

docker exec "$CONTAINER" rm -rf /tmp/influxdb-build
docker exec "$CONTAINER" mkdir -p /tmp/influxdb-build
docker cp "${SCRIPT_DIR}/src"     "${CONTAINER}:/tmp/influxdb-build/"
docker cp "${SCRIPT_DIR}/pom.xml" "${CONTAINER}:/tmp/influxdb-build/"
docker exec -u root "$CONTAINER" bash -c "chown -R dremio:dremio /tmp/influxdb-build"
docker exec -u dremio "$CONTAINER" bash -c \
  "cd /tmp/influxdb-build && mvn package -DskipTests --batch-mode -q"

mkdir -p "${SCRIPT_DIR}/jars"
docker cp "${CONTAINER}:/tmp/influxdb-build/jars/${JAR_NAME}" "${SCRIPT_DIR}/jars/${JAR_NAME}"
echo "JAR ready: jars/${JAR_NAME}"
echo ""
echo "Deploy with: ./install.sh"
