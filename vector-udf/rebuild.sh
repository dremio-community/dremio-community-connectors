#!/usr/bin/env bash
# rebuild.sh — Rebuild the Vector UDF JAR and redeploy to a running Dremio container
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTAINER="${1:-try-dremio}"

echo "=== Rebuilding Dremio Vector UDF ==="
cd "$SCRIPT_DIR"

mvn package -DskipTests -q
JAR=$(find target -name "dremio-vector-udf-*.jar" ! -name "*sources*" | head -1)
echo "Built: $JAR"

DOCKER=$(command -v docker 2>/dev/null || echo "/Applications/Docker.app/Contents/Resources/bin/docker")
$DOCKER cp "$JAR" "${CONTAINER}:/opt/dremio/jars/3rdparty/$(basename $JAR)"
echo "Deployed to $CONTAINER"

$DOCKER restart "$CONTAINER"
echo "Restarted. Waiting..."
for i in $(seq 1 24); do
  STATUS=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:9047/ 2>/dev/null || echo "000")
  [[ "$STATUS" == "200" ]] && echo "Dremio is up." && exit 0
  sleep 5
done
echo "Dremio may still be starting — check http://localhost:9047"
