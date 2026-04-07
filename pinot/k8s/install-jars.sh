#!/usr/bin/env bash
# Helper script used by Dockerfile to install Dremio JARs into Maven local repo.
# Called as: install-jars.sh <directory-containing-dremio-jars>
set -euo pipefail

JAR_DIR="${1:?Usage: install-jars.sh <jar-directory>}"
DREMIO_VERSION=$(ls "$JAR_DIR"/dremio-common-[0-9]*.jar 2>/dev/null | head -1 | sed 's/.*dremio-common-\(.*\)\.jar/\1/')

if [ -z "$DREMIO_VERSION" ]; then
  echo "ERROR: Could not detect Dremio version from JARs in $JAR_DIR" >&2
  exit 1
fi

echo "Detected Dremio version: $DREMIO_VERSION"

install_jar() {
  local GROUP="$1"
  local ARTIFACT="$2"
  local FILE
  FILE=$(ls "$JAR_DIR"/${ARTIFACT}-[0-9]*.jar 2>/dev/null | head -1)
  if [ -z "$FILE" ]; then
    echo "WARNING: JAR not found for $ARTIFACT" >&2
    return
  fi
  mvn install:install-file \
    -q \
    -Dfile="$FILE" \
    -DgroupId="$GROUP" \
    -DartifactId="$ARTIFACT" \
    -Dversion="$DREMIO_VERSION" \
    -Dpackaging=jar
}

install_jar com.dremio              dremio-sabot-kernel
install_jar com.dremio              dremio-common
install_jar com.dremio              dremio-sabot-kernel-proto
install_jar com.dremio              dremio-sabot-vector-tools
install_jar com.dremio              dremio-services-namespace
install_jar com.dremio              dremio-connector
install_jar com.dremio              dremio-sabot-logical
install_jar com.dremio              dremio-common-core
install_jar com.dremio              dremio-services-datastore
install_jar com.dremio              dremio-services-options
install_jar com.dremio.plugin       dremio-plugin-common
install_jar com.dremio.services     dremio-services-credentials
install_jar com.dremio.plugins      dremio-ce-jdbc-plugin
install_jar com.dremio.plugins      dremio-ce-jdbc-fetcher-api

# Update pom.xml version
sed -i "s|<dremio.version>.*</dremio.version>|<dremio.version>${DREMIO_VERSION}</dremio.version>|" /build/pom.xml

echo "All Dremio JARs installed for version $DREMIO_VERSION"
