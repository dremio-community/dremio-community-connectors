#!/usr/bin/env bash
set -e

echo "Building Dremio Talend Connector (Arrow Flight)..."
./mvnw clean install

echo "Build complete! Look in target/ for the .car file to deploy into Talend Studio."
