#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# Dremio Excel Importer — macOS launcher
# Double-click this file to start the web UI.
# ─────────────────────────────────────────────────────────────────────────────
cd "$(dirname "$0")"

PORT=8766
JAR="jars/dremio-excel-importer.jar"
UI="importer-ui.py"

# ── Check Java ────────────────────────────────────────────────────────────────
find_java() {
  # Try common locations in order
  for candidate in \
    "$(command -v java 2>/dev/null)" \
    "/usr/bin/java" \
    "$(/usr/libexec/java_home 2>/dev/null)/bin/java" \
    "/opt/homebrew/opt/openjdk/bin/java" \
    "/opt/homebrew/opt/openjdk@11/bin/java" \
    "/opt/homebrew/opt/openjdk@17/bin/java" \
    "/opt/homebrew/opt/openjdk@21/bin/java" \
    "/Library/Java/JavaVirtualMachines/temurin-11.jdk/Contents/Home/bin/java" \
    "/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java" \
    "/Library/Java/JavaVirtualMachines/temurin-21.jdk/Contents/Home/bin/java"
  do
    if [ -n "$candidate" ] && [ -x "$candidate" ]; then
      echo "$candidate"
      return 0
    fi
  done
  return 1
}

JAVA=$(find_java)
DOCKER_MODE=""
KUBECTL_MODE=""
KUBECTL_NS=""

if [ -z "$JAVA" ] || ! "$JAVA" -version &>/dev/null 2>&1; then
  # Java not working — try Docker first, then kubectl
  DOCKER_BIN=$(command -v docker 2>/dev/null || echo "/usr/local/bin/docker")
  if [ -x "$DOCKER_BIN" ]; then
    for candidate in try-dremio dremio; do
      if "$DOCKER_BIN" exec "$candidate" java -version &>/dev/null 2>&1; then
        DOCKER_MODE="$candidate"
        break
      fi
    done
  fi

  if [ -z "$DOCKER_MODE" ]; then
    KUBECTL_BIN=$(command -v kubectl 2>/dev/null || echo "/usr/local/bin/kubectl")
    if [ -x "$KUBECTL_BIN" ]; then
      # Find a running pod that contains "dremio" and has java
      while IFS= read -r pod_line; do
        POD_NAME=$(echo "$pod_line" | awk '{print $1}')
        NS=$(echo "$pod_line" | awk '{print $2}')
        if [ -n "$POD_NAME" ] && "$KUBECTL_BIN" exec "$POD_NAME" -n "$NS" -- java -version &>/dev/null 2>&1; then
          KUBECTL_MODE="$POD_NAME"
          KUBECTL_NS="$NS"
          break
        fi
      done < <("$KUBECTL_BIN" get pods -A 2>/dev/null | grep -i dremio | grep -i running | head -5)
    fi
  fi

  if [ -n "$DOCKER_MODE" ]; then
    echo "Java not found locally — using Docker container: $DOCKER_MODE"
  elif [ -n "$KUBECTL_MODE" ]; then
    echo "Java not found locally — using Kubernetes pod: $KUBECTL_MODE (namespace: $KUBECTL_NS)"
  else
    echo ""
    echo "══════════════════════════════════════════════════════"
    echo "  Java not found."
    echo ""
    echo "  Quick install options:"
    echo "    • Homebrew:  brew install openjdk@21"
    echo "    • Download:  https://adoptium.net"
    echo "══════════════════════════════════════════════════════"
    echo ""
    if command -v brew &>/dev/null; then
      read -r -p "Install Java now via Homebrew? [y/N] " ans
      if [[ "$ans" =~ ^[Yy]$ ]]; then
        brew install openjdk@21
        JAVA="$(brew --prefix openjdk@21)/bin/java"
      else
        read -r -p "Press Enter to open the download page..." _
        open "https://adoptium.net"
        exit 1
      fi
    else
      read -r -p "Press Enter to open the download page..." _
      open "https://adoptium.net"
      exit 1
    fi
  fi
else
  echo "Java : $JAVA ($("$JAVA" -version 2>&1 | head -1))"
fi

# ── Check Python ──────────────────────────────────────────────────────────────
PYTHON=$(command -v python3 2>/dev/null)
if [ -z "$PYTHON" ]; then
  echo ""
  echo "Python 3 not found. Install it from https://python.org"
  read -r -p "Press Enter to open the download page..." _
  open "https://python.org/downloads"
  exit 1
fi
echo "Python: $PYTHON ($("$PYTHON" --version))"

# ── Check JAR ─────────────────────────────────────────────────────────────────
if [ ! -f "$JAR" ]; then
  echo ""
  echo "JAR not found at $JAR"
  echo "Run 'bash install.sh --docker try-dremio' to build it."
  read -r -p "Press Enter to exit..." _
  exit 1
fi

# ── Kill any existing instance on this port ───────────────────────────────────
EXISTING=$(lsof -ti:$PORT 2>/dev/null)
if [ -n "$EXISTING" ]; then
  echo ""
  echo "Port $PORT already in use — stopping existing instance..."
  kill "$EXISTING" 2>/dev/null || true
  sleep 0.5
fi

# ── Launch ────────────────────────────────────────────────────────────────────
echo ""
echo "Starting Dremio Excel Importer UI..."
if [ -n "$DOCKER_MODE" ]; then
  "$PYTHON" "$UI" --port "$PORT" --docker "$DOCKER_MODE" &
elif [ -n "$KUBECTL_MODE" ]; then
  "$PYTHON" "$UI" --port "$PORT" --kubectl "$KUBECTL_MODE" --namespace "$KUBECTL_NS" &
else
  JAVA_BINARY="$JAVA" "$PYTHON" "$UI" --port "$PORT" &
fi
UI_PID=$!

# Wait for server to be ready (up to 5s)
for i in 1 2 3 4 5; do
  sleep 1
  if curl -s "http://localhost:$PORT/" > /dev/null 2>&1; then
    break
  fi
done

echo "Opening http://localhost:$PORT ..."
open "http://localhost:$PORT"

echo ""
echo "Dremio Excel Importer is running."
echo "Close this window to stop the server."
echo ""

# Keep terminal open; kill server when window is closed
trap "kill $UI_PID 2>/dev/null; echo 'Server stopped.'" EXIT
wait "$UI_PID"
