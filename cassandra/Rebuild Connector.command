#!/usr/bin/env bash
# macOS launcher — double-click this file in Finder to open the Rebuild UI.
# (Right-click → Open the first time if macOS shows a security warning.)
#
# Requires: Python 3 (ships with macOS 12+; install from python.org if missing)

cd "$(dirname "$0")"

# ── Check Python 3 ────────────────────────────────────────────────────────────
PY=""
for candidate in python3 python3.13 python3.12 python3.11 python3.10 \
                 /usr/local/bin/python3 /opt/homebrew/bin/python3; do
  if command -v "$candidate" &>/dev/null; then
    PY="$candidate"
    break
  fi
done

if [[ -z "$PY" ]]; then
  osascript -e 'display alert "Python 3 not found" message "Please install Python 3 from python.org or via Homebrew (brew install python), then try again." as critical'
  exit 1
fi

echo "Starting Rebuild UI…"
"$PY" rebuild-ui.py
