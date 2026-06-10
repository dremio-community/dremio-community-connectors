#!/usr/bin/env bash
# Rebuild and redeploy the PagerDuty connector
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo "Building dremio-pagerduty-connector..."
mvn clean package -DskipTests

echo "Deploying..."
./install.sh
