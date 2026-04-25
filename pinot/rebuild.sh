#!/usr/bin/env bash
# rebuild.sh — Rebuild and redeploy the Dremio Apache Pinot Connector
#
# Detects the running Dremio version, updates pom.xml if needed,
# rebuilds the connector JAR from source, and redeploys it.
#
# This is a convenience wrapper around install.sh, which performs all
# build and deploy steps. Run this after a Dremio version upgrade.
#
# USAGE
#   ./rebuild.sh [--docker CONTAINER] [--local DREMIO_HOME] [--k8s POD]
#                [--namespace NS] [--dry-run]
#
# OPTIONS
#   --docker CONTAINER   Target running Docker container (default: try-dremio)
#   --local  DREMIO_HOME Target bare-metal Dremio installation
#   --k8s    POD         Target Kubernetes pod (coordinator)
#   --namespace NS (-n)  Kubernetes namespace (k8s mode only)
#   --dry-run            Detect version only — no build or deploy
#   -h, --help           Show this help
#
# EXAMPLES
#   ./rebuild.sh --docker try-dremio
#   ./rebuild.sh --local /opt/dremio
#   ./rebuild.sh --k8s dremio-0 --namespace dremio
#   ./rebuild.sh --dry-run

set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec "${SCRIPT_DIR}/install.sh" "$@"
