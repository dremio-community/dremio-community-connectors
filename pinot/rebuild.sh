#!/usr/bin/env bash
# =============================================================================
# Dremio Apache Pinot Connector — Rebuild Script
#
# Detects the running Dremio version, updates pom.xml if needed,
# rebuilds the connector, and redeploys.
#
# USAGE
#   ./rebuild.sh [--docker CONTAINER] [--local DREMIO_HOME] [--k8s POD]
#   ./rebuild.sh --force    # rebuild even if version unchanged
#   ./rebuild.sh --dry-run  # detect version only, no rebuild
# =============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
exec "$SCRIPT_DIR/install.sh" "$@"
