#!/usr/bin/env bash
set -euo pipefail

CONTAINER="dremio-test"
DV="26.0.5-202509091642240013-f5051a07"
AV="18.1.1-20250709131625-66bbaf1fd7-dremio"

# List of Dremio Jars
JARS=(
  "dremio-common com.dremio dremio-common"
  "dremio-sabot-kernel com.dremio dremio-sabot-kernel"
  "dremio-connector com.dremio dremio-connector"
)

# Install Dremio JARs
for row in "${JARS[@]}"; do
  read -r file_prefix groupId artifactId <<< "$row"
  echo "Installing $artifactId..."
  docker exec "$CONTAINER" mvn install:install-file -q \
    -Dfile="/opt/dremio/jars/${file_prefix}-${DV}.jar" \
    -DgroupId="$groupId" \
    -DartifactId="$artifactId" \
    -Dversion="$DV" \
    -Dpackaging=jar
done

# Install Arrow
echo "Installing arrow-vector..."
docker exec "$CONTAINER" mvn install:install-file -q \
  -Dfile="/opt/dremio/jars/3rdparty/arrow-vector-${AV}.jar" \
  -DgroupId="org.apache.arrow" \
  -DartifactId="arrow-vector" \
  -Dversion="13.0.0" \
  -Dpackaging=jar

echo "All dependencies installed in container."
