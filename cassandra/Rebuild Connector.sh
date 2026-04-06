#!/usr/bin/env bash
# Linux launcher — run this script to open the Rebuild UI.
#
# FIRST-TIME SETUP (one time only):
#   chmod +x "Rebuild Connector.sh"
#
# THEN, to launch:
#   ./"Rebuild Connector.sh"
#
# For a desktop icon on Linux Mint / Ubuntu / Debian:
#   Copy "Rebuild Connector.desktop" to ~/Desktop/
#   Right-click the icon → Allow Launching
#
# Requires: python3 (sudo apt install python3  — usually pre-installed)

cd "$(dirname "$0")"

# ── Check Python 3 ────────────────────────────────────────────────────────────
PY=""
for candidate in python3 python3.13 python3.12 python3.11 python3.10 python; do
  if command -v "$candidate" &>/dev/null; then
    VER=$("$candidate" -c "import sys; print(sys.version_info.major)" 2>/dev/null)
    if [[ "$VER" == "3" ]]; then
      PY="$candidate"
      break
    fi
  fi
done

if [[ -z "$PY" ]]; then
  # Try to show a GUI dialog if available, otherwise print to terminal
  if command -v zenity &>/dev/null; then
    zenity --error --title="Python 3 not found" \
      --text="Python 3 is required.\n\nInstall it with:\n  sudo apt install python3\n\nThen try again."
  elif command -v notify-send &>/dev/null; then
    notify-send "Rebuild UI" "Python 3 not found. Run: sudo apt install python3"
  fi
  echo "ERROR: Python 3 not found."
  echo "  Install with:  sudo apt install python3"
  exit 1
fi

echo "Starting Rebuild UI…"
echo "Open: http://localhost:8765  (browser will open automatically)"
echo "Press Ctrl+C to stop."
echo ""
"$PY" rebuild-ui.py
