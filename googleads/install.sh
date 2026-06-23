#!/usr/bin/env bash
# Deploy the pre-built Google Ads connector JAR to a running Dremio instance.
set -euo pipefail

CONTAINER="${1:-try-dremio}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_NAME="dremio-googleads-connector-1.0.0-SNAPSHOT.jar"
JAR_PATH="${SCRIPT_DIR}/jars/${JAR_NAME}"

[[ -f "$JAR_PATH" ]] || { echo "JAR not found: $JAR_PATH — run ./rebuild.sh first"; exit 1; }

docker cp "$JAR_PATH" "${CONTAINER}:/opt/dremio/jars/3rdparty/${JAR_NAME}"
docker restart "$CONTAINER"
echo "Deployed $JAR_NAME to $CONTAINER. Waiting for Dremio..."
for i in $(seq 1 60); do
  sleep 3
  HTTP=$(curl -s -o /dev/null -w "%{http_code}" "http://localhost:9047/apiv2/info" 2>/dev/null || true)
  [[ "$HTTP" == "200" || "$HTTP" == "404" ]] && { echo "Dremio ready."; exit 0; }
  echo -n "."
done
echo
echo "Dremio may still be starting — check http://localhost:9047"
