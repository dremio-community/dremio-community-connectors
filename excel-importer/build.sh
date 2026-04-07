#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Building Dremio Excel Importer..."
mvn package -q

echo "Build complete: jars/dremio-excel-importer.jar"
ls -lh jars/dremio-excel-importer.jar
