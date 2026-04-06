#!/usr/bin/env python3
"""
rebuild-ui.py  —  Browser UI for the Dremio Cassandra Connector rebuild script.

Usage:
    python3 rebuild-ui.py            # starts server on http://localhost:8765
    python3 rebuild-ui.py --port N   # custom port

Double-click "Rebuild Connector.command" on macOS for a zero-command launch.
"""

import argparse
import http.server
import json
import os
import re
import socketserver
import subprocess
import sys
import threading
import time
import webbrowser
from urllib.parse import urlparse, parse_qs

# ── Paths ──────────────────────────────────────────────────────────────────────
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
REBUILD_SH  = os.path.join(SCRIPT_DIR, "rebuild.sh")

# ── Build state (module-level, guarded by _lock) ───────────────────────────────
_lock   = threading.Lock()
_state  = dict(running=False, lines=[], exit_code=None, started_at=None)

# ── ANSI stripping + line classification ──────────────────────────────────────
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[mA-Za-z]")

def _classify(raw: str) -> str:
    t = _ANSI_RE.sub("", raw)
    if any(k in t for k in ("✓", "SUCCESS", "✅", "up-to-date", "nothing to rebuild")):
        return "ok"
    if any(k in t for k in ("⚠", "WARN", "WARNING")):
        return "warn"
    if any(k in t for k in ("✗", "ERROR", "FAILED", "FAIL", "error:")):
        return "err"
    if any(k in t for k in ("→", "Step ", "──", "Detecting", "Comparing",
                             "Updating", "Installing", "Building", "Deploying",
                             "Restarting", "Waiting")):
        return "info"
    return ""

def _clean(raw: str) -> str:
    return _ANSI_RE.sub("", raw).rstrip()

# ── Embedded HTML ──────────────────────────────────────────────────────────────
_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Rebuild — Dremio Cassandra Connector</title>
<style>
  :root {
    --bg:      #0d1117;
    --surface: #161b22;
    --border:  #30363d;
    --text:    #c9d1d9;
    --muted:   #8b949e;
    --ok:      #3fb950;
    --warn:    #d29922;
    --err:     #f85149;
    --info:    #58a6ff;
    --accent:  #238636;
    --accent-h:#2ea043;
    --btn-dry: #1f6feb;
    --btn-dry-h:#388bfd;
  }
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
    font-size: 14px; line-height: 1.5;
    display: flex; flex-direction: column; min-height: 100vh;
  }
  header {
    background: var(--surface); border-bottom: 1px solid var(--border);
    padding: 16px 24px; display: flex; align-items: center; gap: 12px;
  }
  header svg { flex-shrink: 0; }
  header h1 { font-size: 16px; font-weight: 600; }
  header span { color: var(--muted); font-size: 13px; }
  main { flex: 1; padding: 24px; max-width: 860px; width: 100%; margin: 0 auto; }

  /* ── Card ── */
  .card {
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; padding: 20px; margin-bottom: 16px;
  }
  .card h2 { font-size: 14px; font-weight: 600; margin-bottom: 16px; color: var(--text); }

  /* ── Mode tabs ── */
  .tabs { display: flex; gap: 8px; margin-bottom: 16px; }
  .tab {
    flex: 1; padding: 8px 12px; border-radius: 6px; cursor: pointer;
    border: 1px solid var(--border); background: var(--bg);
    color: var(--muted); font-size: 13px; font-weight: 500;
    text-align: center; transition: all .15s;
  }
  .tab:hover { border-color: var(--info); color: var(--text); }
  .tab.active { border-color: var(--info); background: #1c2b3a; color: var(--info); }

  /* ── Inputs ── */
  .field { margin-bottom: 12px; }
  .field label { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
  .field input[type=text] {
    width: 100%; padding: 8px 12px; border-radius: 6px;
    border: 1px solid var(--border); background: var(--bg);
    color: var(--text); font-size: 13px; outline: none;
    transition: border-color .15s;
  }
  .field input[type=text]:focus { border-color: var(--info); }

  /* ── Options ── */
  .options { display: flex; gap: 24px; margin-top: 4px; }
  .opt { display: flex; align-items: center; gap: 8px; cursor: pointer; user-select: none; }
  .opt input[type=checkbox] { width: 15px; height: 15px; accent-color: var(--info); cursor: pointer; }
  .opt span { font-size: 13px; }
  .opt-desc { font-size: 12px; color: var(--muted); margin-top: 2px; }

  /* ── Buttons ── */
  .actions { display: flex; gap: 10px; margin-top: 16px; }
  .btn {
    padding: 9px 18px; border-radius: 6px; font-size: 13px; font-weight: 600;
    cursor: pointer; border: none; transition: background .15s, opacity .15s;
    display: flex; align-items: center; gap: 6px;
  }
  .btn-primary { background: var(--accent); color: #fff; }
  .btn-primary:hover:not(:disabled) { background: var(--accent-h); }
  .btn-dry { background: var(--btn-dry); color: #fff; }
  .btn-dry:hover:not(:disabled) { background: var(--btn-dry-h); }
  .btn:disabled { opacity: .45; cursor: not-allowed; }

  /* ── Status badge ── */
  .status-bar {
    display: flex; align-items: center; gap: 10px;
    padding: 10px 14px; border-radius: 6px; font-size: 13px;
    margin-bottom: 12px; border: 1px solid var(--border);
    background: var(--bg);
  }
  .badge {
    width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
  }
  .badge-idle    { background: var(--muted); }
  .badge-running { background: var(--info); animation: pulse 1s infinite; }
  .badge-ok      { background: var(--ok); }
  .badge-err     { background: var(--err); }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.3} }
  .elapsed { margin-left: auto; color: var(--muted); font-variant-numeric: tabular-nums; font-size: 12px; }

  /* ── Terminal ── */
  #terminal {
    background: #010409; border: 1px solid var(--border); border-radius: 6px;
    padding: 14px 16px; font-family: "SF Mono", "Fira Code", "Cascadia Code",
      "Consolas", monospace; font-size: 12.5px; line-height: 1.65;
    min-height: 360px; max-height: 60vh; overflow-y: auto;
    white-space: pre-wrap; word-break: break-all;
  }
  #terminal .ok   { color: var(--ok); }
  #terminal .warn { color: var(--warn); }
  #terminal .err  { color: var(--err); }
  #terminal .info { color: var(--info); }
  #terminal .prompt { color: var(--muted); user-select: none; }
  .term-header {
    display: flex; align-items: center; gap: 8px;
    margin-bottom: 8px; padding-bottom: 8px;
    border-bottom: 1px solid var(--border);
  }
  .term-header span { font-size: 12px; color: var(--muted); font-family: inherit; }
  .term-copy {
    margin-left: auto; font-size: 11px; color: var(--muted); cursor: pointer;
    padding: 2px 8px; border-radius: 4px; border: 1px solid var(--border);
    background: transparent; font-family: inherit;
  }
  .term-copy:hover { color: var(--text); border-color: var(--muted); }

  /* ── Section hidden ── */
  .hidden { display: none !important; }

  /* ── Footer ── */
  footer {
    text-align: center; font-size: 12px; color: var(--muted);
    padding: 16px; border-top: 1px solid var(--border);
  }
  footer a { color: var(--muted); }
</style>
</head>
<body>

<header>
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none">
    <circle cx="12" cy="12" r="10" stroke="#58a6ff" stroke-width="1.5"/>
    <path d="M12 7v5l3 3" stroke="#58a6ff" stroke-width="1.5" stroke-linecap="round"/>
  </svg>
  <div>
    <h1>Dremio Cassandra Connector — Rebuild</h1>
    <span>Detects your Dremio version, updates pom.xml, recompiles, and redeploys</span>
  </div>
</header>

<main>

  <!-- ── Config card ── -->
  <div class="card">
    <h2>Target Environment</h2>

    <div class="tabs">
      <div class="tab active" data-mode="docker">🐳 Docker</div>
      <div class="tab"        data-mode="local" >🖥  Local / Bare-metal</div>
      <div class="tab"        data-mode="k8s"   >☸️  Kubernetes</div>
    </div>

    <div id="pane-docker" class="mode-pane">
      <div class="field">
        <label>Container name</label>
        <input id="docker-container" type="text" value="try-dremio"
               placeholder="e.g. try-dremio or my-dremio">
      </div>
    </div>

    <div id="pane-local" class="mode-pane hidden">
      <div class="field">
        <label>Dremio home directory</label>
        <input id="local-path" type="text" value="/opt/dremio"
               placeholder="e.g. /opt/dremio">
      </div>
    </div>

    <div id="pane-k8s" class="mode-pane hidden">
      <div class="field">
        <label>Pod name</label>
        <input id="k8s-pod" type="text" value="dremio-0"
               placeholder="e.g. dremio-0">
      </div>
    </div>

    <div class="options">
      <label class="opt">
        <input id="opt-force" type="checkbox">
        <div>
          <span>Force rebuild</span>
          <div class="opt-desc">Rebuild even if detected version matches pom.xml</div>
        </div>
      </label>
    </div>

    <div class="actions">
      <button id="btn-rebuild" class="btn btn-primary" onclick="startBuild(false)">
        ▶ Rebuild &amp; Deploy
      </button>
      <button id="btn-dryrun" class="btn btn-dry" onclick="startBuild(true)">
        🔍 Dry Run
      </button>
    </div>
  </div>

  <!-- ── Output card ── -->
  <div class="card" id="output-card">
    <div class="term-header">
      <span id="term-label">Output</span>
      <button class="term-copy" onclick="copyLog()">Copy log</button>
    </div>

    <div id="status-bar" class="status-bar">
      <div id="badge" class="badge badge-idle"></div>
      <span id="status-text">Ready — click Rebuild to start</span>
      <span id="elapsed" class="elapsed hidden"></span>
    </div>

    <div id="terminal"></div>
  </div>

</main>

<footer>
  rebuild.sh · <a href="https://github.com/mshainman/dremio-cassandra-connector" target="_blank">GitHub</a>
</footer>

<script>
// ── State ─────────────────────────────────────────────────────────────────────
let _mode     = "docker";
let _evtSrc   = null;
let _startTs  = null;
let _ticker   = null;
let _running  = false;

// ── Tab switching ─────────────────────────────────────────────────────────────
document.querySelectorAll(".tab").forEach(tab => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach(t => t.classList.remove("active"));
    document.querySelectorAll(".mode-pane").forEach(p => p.classList.add("hidden"));
    tab.classList.add("active");
    _mode = tab.dataset.mode;
    document.getElementById("pane-" + _mode).classList.remove("hidden");
  });
});

// ── Terminal helpers ──────────────────────────────────────────────────────────
const term = document.getElementById("terminal");

function termAppend(text, cls) {
  const div = document.createElement("div");
  if (cls) div.className = cls;
  div.textContent = text;
  term.appendChild(div);
  term.scrollTop = term.scrollHeight;
}

function termClear() {
  term.innerHTML = "";
}

function copyLog() {
  const lines = Array.from(term.querySelectorAll("div")).map(d => d.textContent);
  navigator.clipboard.writeText(lines.join("\n")).then(() => {
    const btn = document.querySelector(".term-copy");
    btn.textContent = "Copied!";
    setTimeout(() => btn.textContent = "Copy log", 1500);
  });
}

// ── Elapsed timer ─────────────────────────────────────────────────────────────
function startTimer() {
  _startTs = Date.now();
  const el = document.getElementById("elapsed");
  el.classList.remove("hidden");
  _ticker = setInterval(() => {
    const s = Math.floor((Date.now() - _startTs) / 1000);
    el.textContent = `${s}s`;
  }, 500);
}
function stopTimer() {
  clearInterval(_ticker);
  _ticker = null;
}

// ── Status helpers ────────────────────────────────────────────────────────────
function setStatus(state, msg) {
  const badge = document.getElementById("badge");
  const text  = document.getElementById("status-text");
  badge.className = "badge badge-" + state;
  text.textContent = msg;
}

// ── Build ─────────────────────────────────────────────────────────────────────
function startBuild(dryRun) {
  if (_running) return;
  _running = true;

  // Collect params
  let extra = "";
  if (_mode === "docker") {
    const c = document.getElementById("docker-container").value.trim() || "try-dremio";
    extra = c;
  } else if (_mode === "local") {
    const p = document.getElementById("local-path").value.trim() || "/opt/dremio";
    extra = p;
  } else {
    const pod = document.getElementById("k8s-pod").value.trim() || "dremio-0";
    extra = pod;
  }

  const payload = {
    mode:     _mode,
    extra:    extra,
    force:    document.getElementById("opt-force").checked,
    dry_run:  dryRun,
  };

  // UI reset
  termClear();
  setStatus("running", dryRun ? "Dry run in progress…" : "Rebuild in progress…");
  startTimer();
  document.getElementById("btn-rebuild").disabled = true;
  document.getElementById("btn-dryrun").disabled  = true;
  document.getElementById("elapsed").classList.remove("hidden");

  // Close any existing SSE connection
  if (_evtSrc) { _evtSrc.close(); _evtSrc = null; }

  // Post to /start, then open SSE stream
  fetch("/start", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) {
      setStatus("err", "Error: " + data.error);
      stopTimer(); _running = false;
      document.getElementById("btn-rebuild").disabled = false;
      document.getElementById("btn-dryrun").disabled  = false;
      return;
    }
    // Open SSE
    _evtSrc = new EventSource("/events");
    _evtSrc.onmessage = (e) => {
      const msg = JSON.parse(e.data);
      if (msg.done) {
        _evtSrc.close(); _evtSrc = null;
        stopTimer();
        _running = false;
        document.getElementById("btn-rebuild").disabled = false;
        document.getElementById("btn-dryrun").disabled  = false;
        const ok = msg.exit_code === 0;
        setStatus(ok ? "ok" : "err",
          ok ? (dryRun ? "Dry run complete — no changes made"
                       : "Rebuild complete ✓")
             : "Rebuild failed — see output above");
        return;
      }
      if (msg.line !== undefined) {
        termAppend(msg.line, msg.cls || "");
      }
    };
    _evtSrc.onerror = () => {
      _evtSrc.close(); _evtSrc = null;
      stopTimer(); _running = false;
      document.getElementById("btn-rebuild").disabled = false;
      document.getElementById("btn-dryrun").disabled  = false;
      setStatus("err", "Connection lost — check terminal for output");
    };
  })
  .catch(err => {
    setStatus("err", "Request failed: " + err);
    stopTimer(); _running = false;
    document.getElementById("btn-rebuild").disabled = false;
    document.getElementById("btn-dryrun").disabled  = false;
  });
}
</script>
</body>
</html>
"""

# ── HTTP Handler ───────────────────────────────────────────────────────────────
class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):  # silence default access log
        pass

    # ── GET ────────────────────────────────────────────────────────────────────
    def do_GET(self):
        p = urlparse(self.path).path
        if p == "/":
            self._send(_HTML.encode(), "text/html; charset=utf-8")
        elif p == "/events":
            self._stream_events()
        else:
            self._send(b"Not found", "text/plain", 404)

    # ── POST ───────────────────────────────────────────────────────────────────
    def do_POST(self):
        p = urlparse(self.path).path
        if p == "/start":
            self._start_build()
        else:
            self._send(b"Not found", "text/plain", 404)

    # ── /start ─────────────────────────────────────────────────────────────────
    def _start_build(self):
        length = int(self.headers.get("Content-Length", 0))
        body   = json.loads(self.rfile.read(length) or b"{}")

        with _lock:
            if _state["running"]:
                self._send_json({"error": "A build is already running"}, 409)
                return
            _state["running"]    = True
            _state["lines"]      = []
            _state["exit_code"]  = None
            _state["started_at"] = time.time()

        # Assemble command
        cmd  = ["bash", REBUILD_SH]
        mode = body.get("mode", "docker")
        arg  = body.get("extra", "").strip()
        if mode == "docker":
            cmd += ["--docker", arg or "try-dremio"]
        elif mode == "local":
            cmd += ["--local", arg or "/opt/dremio"]
        elif mode == "k8s":
            cmd += ["--k8s", arg or "dremio-0"]
        if body.get("force"):
            cmd.append("--force")
        if body.get("dry_run"):
            cmd.append("--dry-run")

        def _run():
            try:
                proc = subprocess.Popen(
                    cmd,
                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                    text=True, bufsize=1, cwd=SCRIPT_DIR,
                )
                for raw in proc.stdout:
                    clean = _clean(raw)
                    cls   = _classify(raw)
                    with _lock:
                        _state["lines"].append({"line": clean, "cls": cls})
                proc.wait()
                with _lock:
                    _state["exit_code"] = proc.returncode
            except Exception as exc:
                with _lock:
                    _state["lines"].append({"line": f"UI ERROR: {exc}", "cls": "err"})
                    _state["exit_code"] = 1
            finally:
                with _lock:
                    _state["running"] = False

        threading.Thread(target=_run, daemon=True).start()
        self._send_json({"ok": True})

    # ── /events  (SSE) ─────────────────────────────────────────────────────────
    def _stream_events(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.send_header("X-Accel-Buffering", "no")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()

        sent = 0
        try:
            while True:
                with _lock:
                    batch    = _state["lines"][sent:]
                    done     = not _state["running"] and _state["exit_code"] is not None
                    exit_code = _state["exit_code"]

                for item in batch:
                    payload = json.dumps(item)
                    self.wfile.write(f"data: {payload}\n\n".encode())
                    sent += 1

                if done and sent >= len(_state["lines"]):
                    final = json.dumps({"done": True, "exit_code": exit_code})
                    self.wfile.write(f"data: {final}\n\n".encode())
                    self.wfile.flush()
                    break

                self.wfile.flush()
                time.sleep(0.1)
        except (BrokenPipeError, ConnectionResetError):
            pass

    # ── Helpers ────────────────────────────────────────────────────────────────
    def _send(self, body: bytes, ct: str, code: int = 200):
        self.send_response(code)
        self.send_header("Content-Type", ct)
        self.send_header("Content-Length", len(body))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data: dict, code: int = 200):
        body = json.dumps(data).encode()
        self._send(body, "application/json", code)


# ── Threaded server ────────────────────────────────────────────────────────────
class _Server(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads = True


# ── Entry point ────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser(description="Rebuild UI for the Dremio Cassandra Connector")
    ap.add_argument("--port", type=int, default=8765, help="Port to listen on (default: 8765)")
    ap.add_argument("--no-browser", action="store_true", help="Don't auto-open the browser")
    args = ap.parse_args()

    if not os.path.isfile(REBUILD_SH):
        print(f"ERROR: rebuild.sh not found at {REBUILD_SH}", file=sys.stderr)
        sys.exit(1)

    url = f"http://localhost:{args.port}"
    server = _Server(("", args.port), Handler)

    print(f"  Dremio Cassandra Connector — Rebuild UI")
    print(f"  ----------------------------------------")
    print(f"  Listening on  {url}")
    print(f"  Press Ctrl+C to stop\n")

    if not args.no_browser:
        # Small delay so the server is ready before the browser hits it
        threading.Timer(0.4, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n  Server stopped.")


if __name__ == "__main__":
    main()
