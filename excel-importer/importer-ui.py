#!/usr/bin/env python3
"""
Dremio Excel Importer — Web UI
Serves a local web interface for uploading .xlsx files and importing to Dremio.
No external dependencies — uses Python 3 stdlib only.

Usage:
  python3 importer-ui.py [--port 8766] [--jar path/to/dremio-excel-importer.jar]
"""

import http.server
import json
import os
import subprocess
import sys
import tempfile
import threading
import uuid
import cgi
import urllib.parse
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

DEFAULT_PORT = 8766
SCRIPT_DIR   = Path(__file__).parent.resolve()
DEFAULT_JAR  = SCRIPT_DIR / "jars" / "dremio-excel-importer.jar"

# Temp dir for uploaded files (cleaned up on exit)
UPLOAD_DIR = Path(tempfile.mkdtemp(prefix="dremio-excel-ui-"))

# Container mode — set via --docker <container> or --kubectl <pod>
DOCKER_CONTAINER     = ""
KUBECTL_POD          = ""
KUBECTL_NAMESPACE    = "default"
CONTAINER_JAR_PATH   = "/tmp/dremio-excel-importer-ui.jar"
CONTAINER_UPLOAD_DIR = "/tmp/dremio-excel-uploads"

def _docker():
    """Return full path to docker binary."""
    for p in ['/usr/local/bin/docker', '/usr/bin/docker',
              '/opt/homebrew/bin/docker', 'docker']:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    return 'docker'

def _kubectl():
    """Return full path to kubectl binary."""
    for p in ['/usr/local/bin/kubectl', '/usr/bin/kubectl',
              '/opt/homebrew/bin/kubectl', 'kubectl']:
        if os.path.isfile(p) and os.access(p, os.X_OK):
            return p
    return 'kubectl'

# ---------------------------------------------------------------------------
# HTML (single-page UI)
# ---------------------------------------------------------------------------

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Dremio Excel Importer</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
           background: #f0f2f5; color: #333; padding: 24px; min-height: 100vh; }
    .container { max-width: 860px; margin: 0 auto; }
    h1 { font-size: 1.6em; font-weight: 700; color: #1a73e8; margin-bottom: 6px; }
    .subtitle { color: #666; font-size: 0.95em; margin-bottom: 24px; }
    .card { background: #fff; border-radius: 10px; padding: 24px;
            margin-bottom: 20px; box-shadow: 0 1px 4px rgba(0,0,0,.08); }
    .card-title { font-size: 1em; font-weight: 600; color: #444;
                  margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
    .badge { background: #1a73e8; color: #fff; border-radius: 50%;
             width: 22px; height: 22px; display: inline-flex;
             align-items: center; justify-content: center; font-size: 0.75em;
             font-weight: 700; flex-shrink: 0; }

    /* Drop zone */
    .drop-zone { border: 2px dashed #c5cae9; border-radius: 8px; padding: 36px 20px;
                 text-align: center; cursor: pointer; transition: all .2s;
                 background: #fafbff; }
    .drop-zone:hover { border-color: #1a73e8; background: #e8f0fe; }
    .drop-zone.drag-over { border-color: #1a73e8; background: #e8f0fe; }
    .drop-zone.has-file { border-color: #34a853; background: #e6f4ea; }
    .drop-icon { font-size: 2.5em; margin-bottom: 8px; }
    .drop-label { color: #555; font-size: 0.95em; }
    .drop-label strong { color: #1a73e8; }
    .drop-or { margin: 6px 0; font-size: 0.85em; color: #999; }

    /* URL input */
    .url-section { margin-top: 14px; }
    .or-divider { display: flex; align-items: center; gap: 10px;
                  margin: 14px 0; color: #999; font-size: 0.85em; }
    .or-divider::before, .or-divider::after
      { content: ""; flex: 1; height: 1px; background: #e0e0e0; }

    /* Form grid */
    .form-grid { display: grid; gap: 14px; }
    .col-2 { grid-template-columns: 1fr 1fr; }
    .col-3 { grid-template-columns: 2fr 1fr 1fr; }
    .form-group label { display: block; font-size: 0.82em; font-weight: 600;
                        color: #555; margin-bottom: 4px; text-transform: uppercase;
                        letter-spacing: .4px; }
    .form-group input, .form-group select {
      width: 100%; padding: 9px 11px; border: 1px solid #ddd; border-radius: 6px;
      font-size: 0.95em; outline: none; transition: border-color .15s; }
    .form-group input:focus, .form-group select:focus { border-color: #1a73e8; }
    .form-group input:disabled, .form-group select:disabled
      { background: #f8f8f8; color: #999; }
    .checkbox-row { display: flex; align-items: center; gap: 8px;
                    padding-top: 22px; font-size: 0.9em; color: #555; cursor: pointer; }
    .checkbox-row input[type=checkbox] { width: 16px; height: 16px; cursor: pointer; }

    /* Schema table */
    .schema-table { width: 100%; border-collapse: collapse; font-size: 0.88em; }
    .schema-table th { text-align: left; padding: 8px 12px; background: #f8f9fa;
                       border-bottom: 2px solid #e0e0e0; color: #555;
                       font-weight: 600; font-size: 0.82em; text-transform: uppercase; }
    .schema-table td { padding: 8px 12px; border-bottom: 1px solid #f0f0f0; }
    .schema-table tr:hover td { background: #fafafa; }
    .type-badge { display: inline-block; padding: 2px 8px; border-radius: 4px;
                  font-size: 0.8em; font-weight: 700; }
    .type-BIGINT    { background: #e8f5e9; color: #2e7d32; }
    .type-DOUBLE    { background: #e3f2fd; color: #1565c0; }
    .type-BOOLEAN   { background: #fce4ec; color: #880e4f; }
    .type-DATE      { background: #fff3e0; color: #e65100; }
    .type-TIMESTAMP { background: #f3e5f5; color: #6a1b9a; }
    .type-VARCHAR   { background: #f5f5f5; color: #555; }
    .row-count { font-size: 0.85em; color: #666; margin-bottom: 12px; }
    .schema-table input.col-rename {
      border: 1px solid #ddd; border-radius: 4px; padding: 3px 7px;
      font-size: 0.85em; width: 100%; font-family: monospace; background: #fff; }
    .schema-table input.col-rename:focus { border-color: #1a73e8; outline: none; }
    .schema-table tr.excluded td { opacity: 0.38; }
    .schema-table td:first-child { width: 32px; text-align: center; }

    /* Test-connection status */
    .conn-status { font-size: 0.82em; margin-top: 6px; padding: 5px 10px;
                   border-radius: 5px; display: none; }
    .conn-ok  { background: #e6f4ea; color: #137333; }
    .conn-err { background: #fce8e6; color: #c5221f; }

    /* History */
    .history-empty { color: #999; font-size: 0.88em; padding: 10px 0; }
    .history-table { width: 100%; border-collapse: collapse; font-size: 0.85em; }
    .history-table th { text-align:left; padding:6px 10px; background:#f8f9fa;
                        border-bottom:2px solid #e0e0e0; color:#555;
                        font-weight:600; font-size:0.8em; text-transform:uppercase; }
    .history-table td { padding:6px 10px; border-bottom:1px solid #f0f0f0; }
    .history-table tr:hover td { background:#fafafa; }
    .hist-ok  { color: #137333; font-weight: 600; }
    .hist-err { color: #c5221f; font-weight: 600; }

    /* Progress bar */
    .import-progress { margin-top: 12px; }
    .import-progress label { font-size:0.82em; color:#555; display:block; margin-bottom:4px; }
    progress { width:100%; height:10px; border-radius:5px; overflow:hidden; }
    progress::-webkit-progress-bar  { background:#eee; border-radius:5px; }
    progress::-webkit-progress-value{ background:#1a73e8; border-radius:5px; transition:width .3s; }

    /* Sample data */
    .sample-section { margin-top:14px; }
    .sample-toggle { font-size:0.82em; color:#1a73e8; cursor:pointer; user-select:none;
                     background:none; border:none; padding:0; font-weight:600; }
    .sample-table { width:100%; border-collapse:collapse; font-size:0.82em; margin-top:8px; overflow-x:auto; display:block; }
    .sample-table th { background:#f8f9fa; padding:5px 10px; border-bottom:2px solid #e0e0e0;
                       white-space:nowrap; font-size:0.8em; text-transform:uppercase; color:#555; }
    .sample-table td { padding:5px 10px; border-bottom:1px solid #f0f0f0;
                       max-width:180px; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; color:#555; }
    .sample-null { color:#ccc; font-style:italic; }

    /* Connection profiles */
    .profile-bar { display:flex; align-items:center; gap:8px; margin-bottom:14px;
                   padding:10px 12px; background:#f8f9ff; border-radius:7px; border:1px solid #e0e8ff; }
    .profile-bar label { font-size:0.8em; font-weight:600; color:#555; white-space:nowrap; }
    .profile-bar select, .profile-bar input { font-size:0.88em; padding:5px 8px;
                          border:1px solid #ddd; border-radius:5px; }
    .profile-bar select { min-width:160px; }
    .profile-bar input  { flex:1; min-width:100px; }

    /* Append validation */
    .validation-box { margin-top:12px; padding:12px; border-radius:7px; font-size:0.85em; }
    .validation-ok   { background:#e6f4ea; }
    .validation-warn { background:#fff8e1; }
    .validation-err  { background:#fce8e6; }
    .val-table { width:100%; border-collapse:collapse; margin-top:8px; }
    .val-table td { padding:4px 8px; }
    .val-match   { color:#137333; }
    .val-missing { color:#c5221f; font-weight:600; }
    .val-extra   { color:#e65100; font-weight:600; }
    .val-type    { color:#c5221f; font-weight:600; }

    /* Multi-sheet progress */
    .sheet-progress { margin-top: 12px; }
    .sheet-row { display: flex; align-items: center; gap: 10px; padding: 5px 0;
                 font-size: 0.88em; border-bottom: 1px solid #f0f0f0; }
    .sheet-row:last-child { border-bottom: none; }
    .sheet-name { flex: 1; font-weight: 500; }
    .sheet-status { font-size: 0.8em; font-weight: 600; }
    .ss-pending  { color: #999; }
    .ss-running  { color: #1a73e8; }
    .ss-ok       { color: #137333; }
    .ss-err      { color: #c5221f; }

    /* Buttons */
    .btn { padding: 10px 22px; border: none; border-radius: 6px; cursor: pointer;
           font-size: 0.95em; font-weight: 600; transition: all .15s; }
    .btn-primary { background: #1a73e8; color: #fff; }
    .btn-primary:hover:not(:disabled) { background: #1557b0; }
    .btn-primary:disabled { background: #93bef5; cursor: not-allowed; }
    .btn-secondary { background: #f1f3f4; color: #444; }
    .btn-secondary:hover { background: #e0e0e0; }
    .btn-danger { background: #fce8e6; color: #c5221f; }
    .btn-danger:hover { background: #f5c6c3; }
    .btn-row { display: flex; gap: 10px; margin-top: 6px; }

    /* Log output */
    .log-box { background: #1e1e2e; color: #cdd6f4; border-radius: 8px;
               padding: 16px; font-family: "SF Mono", "Fira Code", monospace;
               font-size: 0.82em; line-height: 1.55; height: 320px;
               overflow-y: auto; white-space: pre-wrap; margin-top: 14px; }
    .log-ok  { color: #a6e3a1; }
    .log-err { color: #f38ba8; }
    .log-dim { color: #6c7086; }

    /* Alerts */
    .alert { padding: 10px 14px; border-radius: 6px; font-size: 0.9em;
             margin-top: 10px; display: flex; align-items: center; gap: 8px; }
    .alert-success { background: #e6f4ea; color: #137333; }
    .alert-error   { background: #fce8e6; color: #c5221f; }
    .alert-info    { background: #e8f0fe; color: #1a73e8; }

    .hidden { display: none !important; }
    .spinner { display: inline-block; width: 14px; height: 14px;
               border: 2px solid #fff; border-top-color: transparent;
               border-radius: 50%; animation: spin .7s linear infinite; }
    @keyframes spin { to { transform: rotate(360deg); } }
  </style>
</head>
<body>
<div class="container">
  <h1><img src="data:image/png;base64,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" alt="Dremio" style="height:48px;vertical-align:middle;margin-right:12px">Dremio Excel Importer</h1>
  <p class="subtitle">Import .xlsx spreadsheets or Google Sheets into Dremio Iceberg tables</p>

  <!-- ── Card 1: File / URL ── -->
  <div class="card">
    <div class="card-title"><span class="badge">1</span> Select File</div>

    <div class="drop-zone" id="dropZone"
         onclick="document.getElementById('fileInput').click()"
         ondragover="onDragOver(event)"
         ondragleave="onDragLeave(event)"
         ondrop="onDrop(event)">
      <div class="drop-icon">📊</div>
      <div class="drop-label" id="dropLabel">
        <strong>Click to browse</strong> or drag &amp; drop an .xlsx or .csv file here
      </div>
    </div>
    <input type="file" id="fileInput" accept=".xlsx,.csv" style="display:none"
           onchange="handleFile(this.files[0])">

    <div class="or-divider">or import from URL</div>

    <div class="form-grid col-2 url-section" style="align-items:flex-end">
      <div class="form-group">
        <label>URL (Google Sheets export, OneDrive, etc.)</label>
        <input type="url" id="urlInput"
               placeholder="Paste Google Sheets URL (edit or export) — sheet must be shared publicly">
      </div>
      <div>
        <button class="btn btn-secondary" onclick="handleUrl()">Load from URL</button>
      </div>
    </div>
  </div>

  <!-- ── Card 2: Config ── -->
  <div class="card hidden" id="configCard">
    <div class="card-title"><span class="badge">2</span> Configure</div>

    <div class="form-grid col-2" style="margin-bottom:6px">
      <div class="form-group" style="display:flex;gap:10px;align-items:flex-end">
        <div style="flex:1">
          <label>Sheet</label>
          <select id="sheetSelect" onchange="loadPreview()"></select>
        </div>
        <div style="min-width:90px">
          <label>Header row</label>
          <input type="number" id="headerRow" value="0" min="0" max="99"
                 style="width:100%" oninput="loadPreviewDebounced()" title="0 = first row">
        </div>
      </div>
      <div class="form-group">
        <label>Destination (source.schema.table) <span style="color:#e53935;font-weight:700">*</span></label>
        <div style="display:flex;gap:6px;align-items:center">
          <input type="text" id="destPath" style="flex:1"
                 placeholder="e.g. iceberg_minio.my_schema.my_table"
                 oninput="updateImportReady()" onchange="updateImportReady(); validateSchemaDebounced()">
          <button class="btn btn-secondary" id="browseBtn" onclick="toggleBrowse()"
                  style="white-space:nowrap;padding:9px 12px;font-size:0.85em">
            Browse ▾
          </button>
        </div>
        <div id="destHint" style="font-size:0.78em;color:#e53935;margin-top:3px;display:none">
          Required — enter or browse to a Dremio destination to enable import.
        </div>
      </div>
    </div>

    <!-- Catalog browser — full card width so all 3 fields have room -->
    <div id="browsePicker" style="display:none;margin-bottom:14px;padding:14px;
         background:#f8f9ff;border:1px solid #c5cae9;border-radius:8px">
      <div style="font-size:0.8em;font-weight:600;color:#555;margin-bottom:10px">
        Browse Dremio Catalog
        <span id="browseSpinner" style="display:none;margin-left:8px;color:#1a73e8">Loading…</span>
      </div>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:14px">
        <div class="form-group">
          <label>Source</label>
          <select id="browseSource" onchange="loadSchemas()" disabled>
            <option value="">— loading —</option>
          </select>
        </div>
        <div class="form-group">
          <label>Schema / Folder</label>
          <select id="browseSchema" onchange="updateDestFromBrowse()" disabled>
            <option value="">— pick source first —</option>
          </select>
        </div>
        <div class="form-group">
          <label>Table name</label>
          <input type="text" id="browseTable" placeholder="e.g. my_table"
                 oninput="updateDestFromBrowse()" disabled>
        </div>
      </div>
      <div id="browseError" style="color:#e53935;font-size:0.82em;margin-top:6px;display:none"></div>
    </div>

    <div class="profile-bar">
      <label>Profile</label>
      <select id="profileSelect" onchange="loadProfile()">
        <option value="">— unsaved —</option>
      </select>
      <input type="text" id="profileName" placeholder="Profile name">
      <button class="btn btn-secondary" onclick="saveProfile()"
              style="padding:5px 10px;font-size:0.82em;white-space:nowrap">Save</button>
      <button class="btn btn-danger" onclick="deleteProfile()"
              style="padding:5px 10px;font-size:0.82em;white-space:nowrap">Delete</button>
    </div>

    <div class="form-grid col-3" style="margin-bottom:14px">
      <div class="form-group">
        <label>Host</label>
        <input type="text" id="dremioHost" value="localhost" oninput="saveSettings()">
      </div>
      <div class="form-group">
        <label>Port</label>
        <input type="number" id="dremioPort" value="9047" oninput="saveSettings()">
      </div>
      <div class="form-group">
        <label>Mode</label>
        <select id="importMode" onchange="onModeChange()">
          <option value="create">Create (new table)</option>
          <option value="append">Append (existing table)</option>
        </select>
      </div>
    </div>
    <div id="validationBox" style="display:none"></div>

    <div class="form-grid col-2">
      <div class="form-group">
        <label>Username</label>
        <input type="text" id="dremioUser" value="mark" oninput="saveSettings()">
      </div>
      <div class="form-group">
        <label>Password</label>
        <input type="password" id="dremioPass" value="">
      </div>
    </div>
    <div style="margin-top:8px;display:flex;align-items:center;gap:10px">
      <button class="btn btn-secondary" onclick="testConnection()"
              style="padding:7px 14px;font-size:0.85em" id="testConnBtn">
        Test Connection
      </button>
      <span id="connStatus" class="conn-status"></span>
    </div>

    <div style="margin-top:12px">
      <label class="checkbox-row">
        <input type="checkbox" id="overwriteCheck"> Overwrite if exists
      </label>
    </div>
  </div>

  <!-- ── Card 3: Schema Preview ── -->
  <div class="card hidden" id="previewCard">
    <div class="card-title">
      <span class="badge">3</span> Schema Preview
      <span id="previewSheetLabel" style="margin-left:8px;font-size:0.85em;font-weight:400;color:#888"></span>
      <span style="margin-left:auto;font-size:0.78em;font-weight:400;color:#aaa">
        Change sheet above to preview a different sheet
      </span>
    </div>
    <div id="previewContent"></div>
  </div>

  <!-- ── Card 4: Import ── -->
  <div class="card hidden" id="importCard">
    <div class="card-title"><span class="badge">4</span> Import</div>
    <div class="btn-row">
      <button class="btn btn-primary" id="importBtn" onclick="runImport()" disabled>
        Import to Dremio
      </button>
      <button class="btn btn-secondary" id="importAllBtn" onclick="runMultiSheetImport()" disabled
              title="Import every sheet, each as a separate table">
        Import All Sheets
      </button>
      <button class="btn btn-secondary" onclick="loadPreview()">Refresh Preview</button>
    </div>
    <div id="progressArea" class="import-progress hidden">
      <label id="progressLabel">Inserting…</label>
      <progress id="progressBar" value="0" max="1"></progress>
    </div>
    <div class="log-box hidden" id="logBox"></div>
    <div id="importStatus"></div>
    <div id="multiSheetProgress" class="sheet-progress hidden"></div>
  </div>

  <!-- ── Card 5: History ── -->
  <div class="card" id="historyCard">
    <div class="card-title" style="cursor:pointer;user-select:none" onclick="toggleHistory()">
      <span class="badge" style="background:#888">5</span>
      Import History
      <span id="historyToggle" style="margin-left:auto;font-size:0.85em;color:#888">▾</span>
    </div>
    <div id="historyBody">
      <div id="historyContent"><div class="history-empty">No imports yet.</div></div>
      <div style="margin-top:10px;display:flex;justify-content:flex-end">
        <button class="btn btn-danger" onclick="clearHistory()"
                style="padding:6px 12px;font-size:0.82em">Clear History</button>
      </div>
    </div>
  </div>
</div>

<script>
// ── State ──
let uploadedPath  = null;   // path on server of uploaded file
let previewData   = null;   // last schema preview result
let importing     = false;
let columnState   = {};     // {normalizedName: {included: bool, rename: string}}
let currentFileName = '';   // original filename (for table auto-suggest)
let allSheetNames = [];     // populated when sheets are loaded

// ── Drag & Drop ──
function onDragOver(e) {
  e.preventDefault();
  document.getElementById('dropZone').classList.add('drag-over');
}
function onDragLeave(e) {
  document.getElementById('dropZone').classList.remove('drag-over');
}
function onDrop(e) {
  e.preventDefault();
  onDragLeave(e);
  const f = e.dataTransfer.files[0];
  if (f) handleFile(f);
}

// ── File handling ──
function handleFile(file) {
  if (!file) return;
  if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.csv')) {
    alert('Please select an .xlsx or .csv file.');
    return;
  }
  currentFileName = file.name;
  uploadFile(file);
}

function handleUrl() {
  let url = document.getElementById('urlInput').value.trim();
  if (!url) { alert('Please enter a URL.'); return; }
  url = normalizeGoogleSheetsUrl(url);
  document.getElementById('urlInput').value = url;
  fetchSheets(null, url);
}

function normalizeGoogleSheetsUrl(url) {
  // Convert any Google Sheets URL to a full-workbook export URL (no gid).
  // gid restricts export to a single sheet — omitting it gives all sheets.
  const gsMatch = url.match(/^(https:\/\/docs\.google\.com\/spreadsheets\/d\/[^\/]+)\/(edit|view|pub|export)(.*)/);
  if (gsMatch) {
    return gsMatch[1] + '/export?format=xlsx';
  }
  return url;
}

function uploadFile(file) {
  const dz = document.getElementById('dropZone');
  document.getElementById('dropLabel').textContent = 'Uploading ' + file.name + '...';

  const fd = new FormData();
  fd.append('file', file);
  fd.append('filename', file.name);

  fetch('/upload', { method: 'POST', body: fd })
    .then(r => r.json())
    .then(data => {
      if (data.error) throw new Error(data.error);
      uploadedPath = data.path;
      dz.classList.add('has-file');
      document.getElementById('dropLabel').textContent = '✓ ' + file.name;
      autoSuggestTableName(file.name);
      fetchSheets(uploadedPath, null);
    })
    .catch(e => {
      document.getElementById('dropLabel').innerHTML =
        '<strong>Click to browse</strong> or drag &amp; drop an .xlsx file here';
      alert('Upload failed: ' + e.message);
    });
}

// ── Sheets ──
function fetchSheets(path, url) {
  const payload = path ? { path } : { url };
  document.getElementById('previewContent').innerHTML =
    '<div class="alert alert-info">Loading schema…</div>';
  show('configCard');
  show('previewCard');
  show('importCard');

  fetch('/sheets', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) throw new Error(data.error);
    const sheets = data.sheets || [];
    allSheetNames = sheets;
    const sel = document.getElementById('sheetSelect');
    sel.innerHTML = sheets.map(s =>
      `<option value="${escHtml(s)}">${escHtml(s)}</option>`
    ).join('');
    document.getElementById('importAllBtn').disabled = sheets.length < 2;

    // Use the preview that was fetched in parallel — no second round-trip needed
    if (data.preview) {
      previewData = data.preview;
      renderPreview(data.preview);
      updateImportReady();
    } else {
      loadPreview();
    }
  })
  .catch(e => {
    document.getElementById('previewContent').innerHTML =
      `<div class="alert alert-error">${escHtml(e.message)}</div>`;
    alert('Could not load sheets: ' + e.message);
  });
}

// ── Schema Preview ──
function loadPreview() {
  const path = uploadedPath;
  const url  = document.getElementById('urlInput').value.trim();
  if (!path && !url) return;

  const sheet     = document.getElementById('sheetSelect').value;
  const headerRow = parseInt(document.getElementById('headerRow').value) || 0;
  const payload   = { sheet, headerRow };
  if (path) payload.path = path; else payload.url = url;

  document.getElementById('previewContent').innerHTML =
    '<div class="alert alert-info">Loading schema...</div>';

  fetch('/preview', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) throw new Error(data.error);
    previewData = data;
    renderPreview(data);
    updateImportReady();
  })
  .catch(e => {
    document.getElementById('previewContent').innerHTML =
      `<div class="alert alert-error">Schema error: ${escHtml(e.message)}</div>`;
  });
}

function renderPreview(data) {
  // Preserve existing user edits when re-rendering (e.g. sheet change resets)
  const prevState = columnState;
  columnState = {};
  for (const col of data.columns) {
    const prev = prevState[col.name];
    columnState[col.name] = {
      included: prev ? prev.included : true,
      rename:   prev ? prev.rename   : ''
    };
  }

  const TYPES = ['BIGINT','DOUBLE','BOOLEAN','DATE','TIMESTAMP','VARCHAR'];

  document.getElementById('previewSheetLabel').textContent = '— ' + data.sheet;
  const rowsLabel = `${data.rowCount} row${data.rowCount !== 1 ? 's' : ''} detected`;
  const total = data.columns.length;
  let html = `<div class="row-count">${rowsLabel}, ${total} columns
    <span style="color:#888;font-size:0.9em;margin-left:8px">(uncheck to exclude; edit name or type per column)</span>
  </div>`;
  html += `<table class="schema-table">
    <thead><tr>
      <th></th><th>Column Name (editable)</th><th>Type</th><th>Original Header</th>
    </tr></thead><tbody>`;
  for (const col of data.columns) {
    const state = columnState[col.name];
    // Seed type in columnState if not set
    if (!state.type) state.type = col.type;
    const rowCls = state.included ? '' : ' class="excluded"';
    const chk    = state.included ? 'checked' : '';
    const rname  = escHtml(state.rename || col.name);
    const orig   = col.name !== col.originalName ? escHtml(col.originalName) : '';
    const typeOpts = TYPES.map(t =>
      `<option value="${t}"${t === state.type ? ' selected' : ''}>${t}</option>`).join('');
    html += `<tr id="row-${escHtml(col.name)}"${rowCls}>
      <td><input type="checkbox" ${chk}
           onchange="toggleColumn('${escHtml(col.name)}', this.checked)"></td>
      <td><input class="col-rename" type="text" value="${rname}"
           data-col="${escHtml(col.name)}"
           oninput="renameColumn('${escHtml(col.name)}', this.value)"></td>
      <td><select style="font-size:0.82em;padding:3px 5px;border:1px solid #ddd;border-radius:4px"
            onchange="setColumnType('${escHtml(col.name)}', this.value)">${typeOpts}</select></td>
      <td style="color:#888;font-size:0.9em">${orig}</td>
    </tr>`;
  }
  html += '</tbody></table>';

  // Sample data section
  if (data.sampleRows && data.sampleRows.length > 0) {
    html += `<div class="sample-section">
      <button class="sample-toggle" onclick="toggleSample(this)">▸ Show sample data (${data.sampleRows.length} rows)</button>
      <div class="sample-data-body" style="display:none;overflow-x:auto">
        <table class="sample-table"><thead><tr>`;
    for (const col of data.columns) {
      html += `<th>${escHtml(col.name)}</th>`;
    }
    html += '</tr></thead><tbody>';
    for (const row of data.sampleRows) {
      html += '<tr>';
      for (let i = 0; i < data.columns.length; i++) {
        const v = row[i];
        if (v === null || v === undefined) {
          html += '<td class="sample-null">null</td>';
        } else {
          html += `<td title="${escHtml(String(v))}">${escHtml(String(v))}</td>`;
        }
      }
      html += '</tr>';
    }
    html += '</tbody></table></div></div>';
  }

  document.getElementById('previewContent').innerHTML = html;
}

function setColumnType(name, type) {
  if (columnState[name]) columnState[name].type = type;
}

function toggleSample(btn) {
  const body = btn.nextElementSibling;
  const visible = body.style.display !== 'none';
  body.style.display = visible ? 'none' : 'block';
  btn.textContent = (visible ? '▸ Show' : '▾ Hide') + btn.textContent.slice(btn.textContent.indexOf(' '));
}

function toggleColumn(name, included) {
  if (columnState[name]) columnState[name].included = included;
  const row = document.getElementById('row-' + name);
  if (row) row.className = included ? '' : 'excluded';
}

function renameColumn(name, value) {
  if (columnState[name]) columnState[name].rename = value.trim();
}

function updateImportReady() {
  const dest = document.getElementById('destPath').value.trim();
  const ready = dest && previewData && !importing;
  document.getElementById('importBtn').disabled = !ready;
  const hint = document.getElementById('destHint');
  if (hint) hint.style.display = (!dest && previewData) ? 'block' : 'none';
}

// ── Import ──
function runImport() {
  if (importing) return;
  importing = true;
  document.getElementById('importBtn').disabled = true;
  document.getElementById('importBtn').innerHTML =
    '<span class="spinner"></span> Importing...';

  const path     = uploadedPath;
  const url      = document.getElementById('urlInput').value.trim();

  // Collect excluded, renamed, and type-overridden columns from columnState
  const excluded = Object.entries(columnState)
    .filter(([, s]) => !s.included).map(([k]) => k);
  const renames = Object.entries(columnState)
    .filter(([k, s]) => s.included && s.rename && s.rename !== k)
    .map(([k, s]) => `${k}=${s.rename}`);
  // Type overrides: columns whose type was changed from the inferred type
  const typeOverrides = Object.entries(columnState)
    .filter(([k, s]) => s.included && s.type && previewData &&
      previewData.columns.find(c => c.name === k)?.type !== s.type)
    .map(([k, s]) => `${k}=${s.type}`);

  const payload  = {
    sheet:      document.getElementById('sheetSelect').value,
    dest:       document.getElementById('destPath').value.trim(),
    host:       document.getElementById('dremioHost').value.trim(),
    port:       parseInt(document.getElementById('dremioPort').value),
    user:       document.getElementById('dremioUser').value.trim(),
    password:   document.getElementById('dremioPass').value.trim(),
    mode:       document.getElementById('importMode').value,
    overwrite:  document.getElementById('overwriteCheck').checked,
    overrides:  typeOverrides.join(','),
    exclude:    excluded.join(','),
    rename:     renames.join(','),
    headerRow:  parseInt(document.getElementById('headerRow').value) || 0,
  };
  if (path) payload.path = path; else payload.url = url;

  const logBox = document.getElementById('logBox');
  logBox.classList.remove('hidden');
  logBox.textContent = '';
  document.getElementById('importStatus').innerHTML = '';
  document.getElementById('progressArea').classList.remove('hidden');
  document.getElementById('progressBar').value = 0;
  document.getElementById('progressBar').max = 1;
  document.getElementById('progressLabel').textContent = 'Starting…';

  const histEntry = {
    file:  currentFileName || payload.url || '(unknown)',
    sheet: payload.sheet,
    dest:  payload.dest,
    ts:    new Date().toLocaleString(),
    rows:  previewData ? previewData.rowCount : '?',
  };

  const es = new EventSource('/import?' + new URLSearchParams(payload));

  es.onmessage = function(e) {
    const line = e.data;
    if (line === '__DONE__') {
      es.close();
      document.getElementById('progressBar').value = document.getElementById('progressBar').max;
      document.getElementById('progressLabel').textContent = 'Complete';
      doneImport(true, histEntry);
    } else if (line === '__ERROR__') {
      es.close();
      doneImport(false, histEntry);
    } else {
      appendLog(line);
      updateProgress(line);
    }
  };
  es.onerror = function() {
    es.close();
    doneImport(false, histEntry);
  };
}

function appendLog(line) {
  const logBox = document.getElementById('logBox');
  let cls = '';
  if (line.includes('OK') || line.includes('complete') || line.includes('PASS'))
    cls = 'log-ok';
  else if (line.includes('Exception') || line.includes('FAILED') || line.includes('Error'))
    cls = 'log-err';
  else if (line.startsWith('  '))
    cls = 'log-dim';
  const span = document.createElement('span');
  if (cls) span.className = cls;
  span.textContent = line + '\n';
  logBox.appendChild(span);
  logBox.scrollTop = logBox.scrollHeight;
}

function doneImport(success, histEntry) {
  importing = false;
  document.getElementById('importBtn').disabled = false;
  document.getElementById('importBtn').textContent = 'Import to Dremio';
  document.getElementById('progressArea').classList.add('hidden');
  document.getElementById('importStatus').innerHTML = success
    ? '<div class="alert alert-success">✓ Import completed successfully!</div>'
    : '<div class="alert alert-error">✗ Import failed — see log above for details.</div>';
  if (histEntry) {
    histEntry.status = success ? 'ok' : 'error';
    saveHistory(histEntry);
  }
}

// ── Utilities ──
function show(id)   { document.getElementById(id).classList.remove('hidden'); }
function hide(id)   { document.getElementById(id).classList.add('hidden'); }
function escHtml(s) { return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;')
                                      .replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }

// ── Debounce helper ──
function debounce(fn, ms) {
  let t;
  return (...args) => { clearTimeout(t); t = setTimeout(() => fn(...args), ms); };
}
const loadPreviewDebounced      = debounce(loadPreview, 400);
const validateSchemaDebounced   = debounce(validateAppendSchema, 600);

// ── Progress bar ──
function updateProgress(line) {
  // Parse "  Batch 2/5 (rows 501-1000)... OK"
  const m = line.match(/Batch\s+(\d+)\/(\d+)/);
  if (m) {
    const cur = parseInt(m[1]), total = parseInt(m[2]);
    const bar = document.getElementById('progressBar');
    bar.max = total;
    bar.value = cur;
    document.getElementById('progressLabel').textContent =
      `Batch ${cur} of ${total} (${Math.round(cur/total*100)}%)`;
  } else if (line.includes('Inserting')) {
    document.getElementById('progressLabel').textContent = line.trim();
  } else if (line.includes('Connecting')) {
    document.getElementById('progressLabel').textContent = line.trim();
  } else if (line.includes('Creating table')) {
    document.getElementById('progressLabel').textContent = line.trim();
  }
}

// ── localStorage — connection settings ──
function saveSettings() {
  localStorage.setItem('dremio_host', document.getElementById('dremioHost').value);
  localStorage.setItem('dremio_port', document.getElementById('dremioPort').value);
  localStorage.setItem('dremio_user', document.getElementById('dremioUser').value);
}
function loadSettings() {
  const host = localStorage.getItem('dremio_host');
  const port = localStorage.getItem('dremio_port');
  const user = localStorage.getItem('dremio_user');
  if (host) document.getElementById('dremioHost').value = host;
  if (port) document.getElementById('dremioPort').value = port;
  if (user) document.getElementById('dremioUser').value = user;
  renderProfileSelect();
  renderHistory();
}
window.onload = loadSettings;

// ── Connection Profiles ──
const PROFILES_KEY = 'dremio_connection_profiles';

function loadProfileList() {
  try { return JSON.parse(localStorage.getItem(PROFILES_KEY) || '[]'); } catch(e) { return []; }
}

function renderProfileSelect() {
  const sel = document.getElementById('profileSelect');
  const profiles = loadProfileList();
  const current = sel.value;
  sel.innerHTML = '<option value="">— unsaved —</option>';
  profiles.forEach(p => {
    const opt = document.createElement('option');
    opt.value = p.name;
    opt.textContent = p.name;
    sel.appendChild(opt);
  });
  if (current) sel.value = current;
}

function saveProfile() {
  const name = document.getElementById('profileName').value.trim();
  if (!name) { alert('Enter a profile name first.'); return; }
  let profiles = loadProfileList();
  profiles = profiles.filter(p => p.name !== name);
  profiles.unshift({
    name,
    host: document.getElementById('dremioHost').value.trim(),
    port: document.getElementById('dremioPort').value.trim(),
    user: document.getElementById('dremioUser').value.trim(),
  });
  localStorage.setItem(PROFILES_KEY, JSON.stringify(profiles));
  renderProfileSelect();
  document.getElementById('profileSelect').value = name;
}

function loadProfile() {
  const name = document.getElementById('profileSelect').value;
  if (!name) return;
  const profiles = loadProfileList();
  const p = profiles.find(x => x.name === name);
  if (!p) return;
  document.getElementById('dremioHost').value = p.host || '';
  document.getElementById('dremioPort').value = p.port || '9047';
  document.getElementById('dremioUser').value = p.user || '';
  document.getElementById('profileName').value = p.name;
  saveSettings();
}

function deleteProfile() {
  const name = document.getElementById('profileSelect').value;
  if (!name) { alert('Select a profile to delete.'); return; }
  if (!confirm(`Delete profile "${name}"?`)) return;
  let profiles = loadProfileList();
  profiles = profiles.filter(p => p.name !== name);
  localStorage.setItem(PROFILES_KEY, JSON.stringify(profiles));
  document.getElementById('profileSelect').value = '';
  document.getElementById('profileName').value = '';
  renderProfileSelect();
}

// ── Append Schema Validation ──
function onModeChange() {
  const mode = document.getElementById('importMode').value;
  const box  = document.getElementById('validationBox');
  if (mode !== 'append') { box.style.display = 'none'; return; }
  const dest = document.getElementById('destPath').value.trim();
  if (dest) validateSchemaDebounced();
}

function validateAppendSchema() {
  if (document.getElementById('importMode').value !== 'append') return;
  const dest = document.getElementById('destPath').value.trim();
  if (!dest || !previewData) return;
  const box = document.getElementById('validationBox');
  box.style.display = 'block';
  box.innerHTML = '<div class="validation-box" style="background:#e8f0fe;color:#1a73e8">Checking destination schema…</div>';

  fetch('/validate', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      host:     document.getElementById('dremioHost').value.trim(),
      port:     document.getElementById('dremioPort').value.trim(),
      user:     document.getElementById('dremioUser').value.trim(),
      password: document.getElementById('dremioPass').value.trim(),
      dest,
    })
  })
  .then(r => r.json())
  .then(data => {
    if (data.error) {
      box.innerHTML = `<div class="validation-box validation-err">
        ✗ Could not read destination schema: ${escHtml(data.error)}</div>`;
      return;
    }
    renderValidation(box, data.fields);
  })
  .catch(e => {
    box.innerHTML = `<div class="validation-box validation-err">✗ ${escHtml(e.message)}</div>`;
  });
}

function renderValidation(box, destFields) {
  if (!previewData) return;
  const srcCols = previewData.columns
    .filter(c => columnState[c.name] && columnState[c.name].included)
    .map(c => ({
      name: (columnState[c.name].rename || c.name).toLowerCase(),
      type: (columnState[c.name].type || c.type)
    }));

  const destMap = {};
  destFields.forEach(f => destMap[f.name.toLowerCase()] = f.type.toUpperCase());

  let matches = 0, typeMismatches = 0, missing = 0, extra = 0;
  let rows = '';

  // Source columns vs destination
  for (const sc of srcCols) {
    const dt = destMap[sc.name];
    if (!dt) {
      missing++;
      rows += `<tr><td>${escHtml(sc.name)}</td><td class="val-missing">${sc.type}</td>
               <td class="val-missing">— not in dest —</td>
               <td class="val-missing">⚠ Column missing from destination table</td></tr>`;
    } else if (!typesCompatible(sc.type, dt)) {
      typeMismatches++;
      rows += `<tr><td>${escHtml(sc.name)}</td><td>${sc.type}</td>
               <td class="val-type">${dt}</td>
               <td class="val-type">⚠ Type mismatch</td></tr>`;
    } else {
      matches++;
      rows += `<tr><td>${escHtml(sc.name)}</td><td>${sc.type}</td>
               <td>${dt}</td><td class="val-match">✓</td></tr>`;
    }
  }
  // Dest columns not in source
  for (const [dn] of Object.entries(destMap)) {
    if (!srcCols.find(s => s.name === dn)) {
      extra++;
      rows += `<tr><td>${escHtml(dn)}</td><td class="val-extra">— not in source —</td>
               <td>${destMap[dn]}</td>
               <td class="val-extra">ℹ Extra dest column (will be NULL)</td></tr>`;
    }
  }

  const cls = (typeMismatches || missing > 0) ? 'validation-warn' : 'validation-ok';
  const summary = typeMismatches || missing
    ? `⚠ ${typeMismatches} type mismatch(es), ${missing} missing column(s)`
    : `✓ Schema compatible — ${matches} column(s) match`;

  box.innerHTML = `<div class="validation-box ${cls}">
    <strong>${summary}</strong>
    <table class="val-table">
      <tr><th>Column</th><th>Source type</th><th>Dest type</th><th>Status</th></tr>
      ${rows}
    </table>
  </div>`;
}

function typesCompatible(src, dst) {
  if (src === dst) return true;
  // Widen common compatible pairs
  const compat = { 'BIGINT': ['DOUBLE','VARCHAR'], 'DOUBLE': ['VARCHAR'],
                   'DATE': ['TIMESTAMP','VARCHAR'], 'TIMESTAMP': ['VARCHAR'],
                   'BOOLEAN': ['VARCHAR'] };
  return (compat[src] || []).includes(dst);
}

// ── Auto-suggest table name from filename ──
function autoSuggestTableName(filename) {
  // Strip extension, lowercase, replace non-alphanumeric with _, collapse repeats
  let name = filename.replace(/\.[^.]+$/, '')
                     .toLowerCase()
                     .replace(/[^a-z0-9]+/g, '_')
                     .replace(/^_+|_+$/g, '');
  if (!name) return;
  // Pre-fill browseTable
  const bt = document.getElementById('browseTable');
  if (bt && !bt.value) bt.value = name;
  // Pre-fill destPath table segment if dest has a schema prefix already
  const dest = document.getElementById('destPath');
  if (dest && !dest.value) {
    // If browse schema is set, compose full path; otherwise just put name
    const schema = document.getElementById('browseSchema').value;
    dest.value = schema ? schema + '.' + name : name;
    updateImportReady();
  } else if (dest && dest.value) {
    // Replace only the last segment (table part)
    const parts = dest.value.split('.');
    parts[parts.length - 1] = name;
    dest.value = parts.join('.');
    updateImportReady();
  }
}

// ── Test Connection ──
function testConnection() {
  const btn = document.getElementById('testConnBtn');
  const status = document.getElementById('connStatus');
  btn.disabled = true;
  btn.textContent = 'Testing…';
  status.className = 'conn-status';
  status.style.display = 'none';

  fetch('/browse', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({
      host:     document.getElementById('dremioHost').value.trim(),
      port:     document.getElementById('dremioPort').value.trim(),
      user:     document.getElementById('dremioUser').value.trim(),
      password: document.getElementById('dremioPass').value.trim(),
      path:     ''
    })
  })
  .then(r => r.json())
  .then(data => {
    btn.disabled = false;
    btn.textContent = 'Test Connection';
    if (data.error) throw new Error(data.error);
    status.textContent = '✓ Connected — ' + data.length + ' source(s) found';
    status.className = 'conn-status conn-ok';
    status.style.display = 'inline-block';
  })
  .catch(e => {
    btn.disabled = false;
    btn.textContent = 'Test Connection';
    status.textContent = '✗ ' + e.message;
    status.className = 'conn-status conn-err';
    status.style.display = 'inline-block';
  });
}

// ── Import History ──
const HISTORY_KEY = 'dremio_import_history';
const HISTORY_MAX = 50;

function saveHistory(entry) {
  let hist = loadHistoryData();
  hist.unshift(entry);
  if (hist.length > HISTORY_MAX) hist = hist.slice(0, HISTORY_MAX);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(hist));
  renderHistory();
}

function loadHistoryData() {
  try { return JSON.parse(localStorage.getItem(HISTORY_KEY) || '[]'); }
  catch(e) { return []; }
}

function clearHistory() {
  if (!confirm('Clear all import history?')) return;
  localStorage.removeItem(HISTORY_KEY);
  renderHistory();
}

let historyCollapsed = false;
function toggleHistory() {
  historyCollapsed = !historyCollapsed;
  document.getElementById('historyBody').style.display = historyCollapsed ? 'none' : '';
  document.getElementById('historyToggle').textContent = historyCollapsed ? '▸' : '▾';
}

function renderHistory() {
  const hist = loadHistoryData();
  const el = document.getElementById('historyContent');
  if (!hist.length) {
    el.innerHTML = '<div class="history-empty">No imports yet.</div>';
    return;
  }
  let html = `<table class="history-table">
    <thead><tr><th>Time</th><th>File</th><th>Sheet</th><th>Destination</th><th>Rows</th><th>Status</th></tr></thead>
    <tbody>`;
  for (const e of hist) {
    const cls = e.status === 'ok' ? 'hist-ok' : 'hist-err';
    const lbl = e.status === 'ok' ? '✓ OK' : '✗ Error';
    html += `<tr>
      <td style="white-space:nowrap;color:#888">${escHtml(e.ts)}</td>
      <td>${escHtml(e.file)}</td>
      <td>${escHtml(e.sheet || '')}</td>
      <td><code style="font-size:0.85em">${escHtml(e.dest || '')}</code></td>
      <td>${escHtml(String(e.rows || '?'))}</td>
      <td class="${cls}">${lbl}</td>
    </tr>`;
  }
  html += '</tbody></table>';
  el.innerHTML = html;
}

// ── Multi-Sheet Import ──
async function runMultiSheetImport() {
  if (importing) return;
  const dest = document.getElementById('destPath').value.trim();
  if (!dest) { alert('Set a destination path first (the sheet name will be appended).'); return; }

  // Use full dest as the prefix — sheet name gets appended as suffix
  const basePath = dest;

  const sheets = allSheetNames;
  if (!sheets.length) return;

  importing = true;
  document.getElementById('importBtn').disabled = true;
  document.getElementById('importAllBtn').disabled = true;
  document.getElementById('importAllBtn').innerHTML = '<span class="spinner"></span> Importing…';

  const prog = document.getElementById('multiSheetProgress');
  prog.classList.remove('hidden');

  // If using a URL, download it once server-side so each sheet reuses the local file
  let resolvedPath = uploadedPath;
  const urlVal = document.getElementById('urlInput').value.trim();
  if (!resolvedPath && urlVal) {
    prog.innerHTML = '<div class="sheet-row"><span class="sheet-name">Downloading spreadsheet…</span></div>';
    try {
      const r = await fetch('/fetch-url', {
        method: 'POST', headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({url: urlVal})
      });
      const d = await r.json();
      if (d.error) throw new Error(d.error);
      resolvedPath = d.path;
    } catch(e) {
      prog.innerHTML = `<div class="alert alert-error">Failed to download: ${escHtml(e.message)}</div>`;
      importing = false;
      document.getElementById('importAllBtn').disabled = false;
      document.getElementById('importAllBtn').textContent = 'Import All Sheets';
      return;
    }
  }

  // Build initial status rows
  const stateMap = {};
  let html = '';
  for (const s of sheets) {
    stateMap[s] = 'pending';
    const tableName = s.toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
    html += `<div class="sheet-row" id="sr-${escHtml(s)}">
      <span class="sheet-name">${escHtml(s)}</span>
      <span style="color:#888;font-size:0.8em">→ ${escHtml(basePath + '_' + tableName)}</span>
      <span class="sheet-status ss-pending" id="ss-${escHtml(s)}">Pending</span>
    </div>`;
  }
  prog.innerHTML = html;

  const logBox = document.getElementById('logBox');
  logBox.classList.remove('hidden');
  logBox.textContent = '';
  document.getElementById('importStatus').innerHTML = '';

  let allOk = true;

  for (const sheet of sheets) {
    const tableName = sheet.toLowerCase().replace(/[^a-z0-9]+/g,'_').replace(/^_+|_+$/g,'');
    const sheetDest = basePath + '_' + tableName;

    document.getElementById('ss-' + sheet).className = 'sheet-status ss-running';
    document.getElementById('ss-' + sheet).textContent = 'Running…';
    appendLog(`\n── Sheet: ${sheet} → ${sheetDest} ──`);

    const excluded = Object.entries(columnState)
      .filter(([, s]) => !s.included).map(([k]) => k);
    const renames = Object.entries(columnState)
      .filter(([k, s]) => s.included && s.rename && s.rename !== k)
      .map(([k, s]) => `${k}=${s.rename}`);
    const typeOvr = Object.entries(columnState)
      .filter(([k, s]) => s.included && s.type && previewData &&
        previewData.columns.find(c => c.name === k)?.type !== s.type)
      .map(([k, s]) => `${k}=${s.type}`);

    const payload = {
      sheet,
      dest:      sheetDest,
      host:      document.getElementById('dremioHost').value.trim(),
      port:      document.getElementById('dremioPort').value.trim(),
      user:      document.getElementById('dremioUser').value.trim(),
      password:  document.getElementById('dremioPass').value.trim(),
      mode:      document.getElementById('importMode').value,
      overwrite: document.getElementById('overwriteCheck').checked,
      overrides: typeOvr.join(','),
      exclude:   excluded.join(','),
      rename:    renames.join(','),
      headerRow: parseInt(document.getElementById('headerRow').value) || 0,
    };
    if (resolvedPath) payload.path = resolvedPath;
    else payload.url = urlVal;

    const ok = await new Promise(resolve => {
      const es = new EventSource('/import?' + new URLSearchParams(payload));
      es.onmessage = e => {
        if (e.data === '__DONE__') { es.close(); resolve(true); }
        else if (e.data === '__ERROR__') { es.close(); resolve(false); }
        else appendLog('  ' + e.data);
      };
      es.onerror = () => { es.close(); resolve(false); };
    });

    if (ok) {
      document.getElementById('ss-' + sheet).className = 'sheet-status ss-ok';
      document.getElementById('ss-' + sheet).textContent = '✓ Done';
      saveHistory({ file: currentFileName || '(url)', sheet, dest: sheetDest,
                    ts: new Date().toLocaleString(), rows: '?', status: 'ok' });
    } else {
      allOk = false;
      document.getElementById('ss-' + sheet).className = 'sheet-status ss-err';
      document.getElementById('ss-' + sheet).textContent = '✗ Error';
      saveHistory({ file: currentFileName || '(url)', sheet, dest: sheetDest,
                    ts: new Date().toLocaleString(), rows: '?', status: 'error' });
    }
  }

  importing = false;
  document.getElementById('importBtn').disabled = false;
  document.getElementById('importAllBtn').disabled = false;
  document.getElementById('importAllBtn').textContent = 'Import All Sheets';
  document.getElementById('importStatus').innerHTML = allOk
    ? '<div class="alert alert-success">✓ All sheets imported successfully!</div>'
    : '<div class="alert alert-error">Some sheets failed — see log above.</div>';
}

// ── Catalog Browser ──
function toggleBrowse() {
  const picker = document.getElementById('browsePicker');
  const btn    = document.getElementById('browseBtn');
  if (picker.style.display === 'none') {
    picker.style.display = 'block';
    btn.textContent = 'Browse ▴';
    loadSources();
  } else {
    picker.style.display = 'none';
    btn.textContent = 'Browse ▾';
  }
}

function _browseBody(path) {
  return JSON.stringify({
    host:     document.getElementById('dremioHost').value.trim(),
    port:     document.getElementById('dremioPort').value.trim(),
    user:     document.getElementById('dremioUser').value.trim(),
    password: document.getElementById('dremioPass').value.trim(),
    path:     path || ''
  });
}

function _browseSetErr(msg) {
  const el = document.getElementById('browseError');
  el.textContent = msg;
  el.style.display = msg ? 'block' : 'none';
}

function loadSources() {
  const sel = document.getElementById('browseSource');
  sel.innerHTML = '<option value="">Loading…</option>';
  sel.disabled = true;
  document.getElementById('browseSchema').innerHTML = '<option value="">— pick source first —</option>';
  document.getElementById('browseSchema').disabled = true;
  document.getElementById('browseTable').value = '';
  document.getElementById('browseTable').disabled = true;
  document.getElementById('browseSpinner').style.display = 'inline';
  _browseSetErr('');

  fetch('/browse', {method:'POST', headers:{'Content-Type':'application/json'}, body:_browseBody('')})
    .then(r => r.json())
    .then(data => {
      document.getElementById('browseSpinner').style.display = 'none';
      if (data.error) throw new Error(data.error);
      sel.innerHTML = '<option value="">— select source —</option>';
      data.forEach(item => {
        const opt = document.createElement('option');
        opt.value = item.path.join('.');
        opt.dataset.path = JSON.stringify(item.path);
        const tag = item.containerType === 'SPACE' ? ' (space)' :
                    item.containerType === 'SOURCE' ? '' : '';
        opt.textContent = item.name + tag;
        sel.appendChild(opt);
      });
      sel.disabled = false;
    })
    .catch(e => {
      document.getElementById('browseSpinner').style.display = 'none';
      sel.innerHTML = '<option value="">— error —</option>';
      _browseSetErr('Could not connect to Dremio: ' + e.message +
                    '. Check host, port and credentials below.');
    });
}

function loadSchemas() {
  const sourceSel = document.getElementById('browseSource');
  const schemaSel = document.getElementById('browseSchema');
  if (!sourceSel.value) {
    schemaSel.innerHTML = '<option value="">— pick source first —</option>';
    schemaSel.disabled = true;
    document.getElementById('browseTable').disabled = true;
    return;
  }
  const path = JSON.parse(sourceSel.options[sourceSel.selectedIndex].dataset.path || '[]');
  schemaSel.innerHTML = '<option value="">Loading…</option>';
  schemaSel.disabled = true;
  document.getElementById('browseSpinner').style.display = 'inline';

  fetch('/browse', {method:'POST', headers:{'Content-Type':'application/json'}, body:_browseBody(path.join('.'))})
    .then(r => r.json())
    .then(data => {
      document.getElementById('browseSpinner').style.display = 'none';
      if (data.error) throw new Error(data.error);
      schemaSel.innerHTML = '<option value="">— select schema —</option>';
      const folders = data.filter(i => i.type === 'CONTAINER');
      if (folders.length === 0) {
        // Source has no sub-folders — table goes directly in source
        const opt = document.createElement('option');
        opt.value = sourceSel.value;
        opt.textContent = '(root of source)';
        schemaSel.appendChild(opt);
      } else {
        folders.forEach(item => {
          const opt = document.createElement('option');
          opt.value = item.path.join('.');
          opt.textContent = item.name;
          schemaSel.appendChild(opt);
        });
      }
      schemaSel.disabled = false;
      document.getElementById('browseTable').disabled = false;
      updateDestFromBrowse();
    })
    .catch(e => {
      document.getElementById('browseSpinner').style.display = 'none';
      schemaSel.innerHTML = '<option value="">— error —</option>';
      _browseSetErr('Error loading schemas: ' + e.message);
    });
}

function updateDestFromBrowse() {
  const schema = document.getElementById('browseSchema').value;
  const table  = document.getElementById('browseTable').value.trim();
  if (schema && table) {
    document.getElementById('destPath').value = schema + '.' + table;
    updateImportReady();
  }
}
</script>
</body>
</html>
"""

# ---------------------------------------------------------------------------
# HTTP Request Handler
# ---------------------------------------------------------------------------

class ImporterHandler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass  # suppress default access log noise

    def do_GET(self):
        if self.path == '/':
            self._send_html(HTML)
        elif self.path.startswith('/import?'):
            self._handle_import_sse()
        else:
            self._send_error(404, "Not found")

    def do_POST(self):
        if self.path == '/upload':
            self._handle_upload()
        elif self.path == '/sheets':
            self._handle_sheets()
        elif self.path == '/preview':
            self._handle_preview()
        elif self.path == '/browse':
            self._handle_browse()
        elif self.path == '/fetch-url':
            self._handle_fetch_url()
        elif self.path == '/validate':
            self._handle_validate()
        else:
            self._send_error(404, "Not found")

    # ── File Upload ──────────────────────────────────────────────────────────

    def _handle_upload(self):
        try:
            ctype, pdict = cgi.parse_header(self.headers.get('Content-Type', ''))
            if ctype != 'multipart/form-data':
                return self._send_json({'error': 'Expected multipart/form-data'})

            pdict['boundary'] = pdict['boundary'].encode()
            pdict['CONTENT-LENGTH'] = int(self.headers.get('Content-Length', 0))
            fields = cgi.parse_multipart(self.rfile, pdict)

            data = fields.get('file', [None])[0]
            if data is None:
                return self._send_json({'error': 'No file field'})

            uid = str(uuid.uuid4())[:8]
            fname_raw = fields.get('filename', [b'upload.xlsx'])[0]
            fname = fname_raw.decode('utf-8', errors='replace') if isinstance(fname_raw, bytes) else str(fname_raw)
            ext = '.csv' if fname.lower().endswith('.csv') else '.xlsx'
            dest = UPLOAD_DIR / f"upload-{uid}{ext}"
            with open(dest, 'wb') as f:
                f.write(data if isinstance(data, bytes) else data.encode('latin-1'))

            if DOCKER_CONTAINER or KUBECTL_POD:
                _copy_to_container(str(dest), _resolve_path(str(dest)))

            self._send_json({'path': str(dest)})
        except Exception as e:
            self._send_json({'error': str(e)})

    # ── List Sheets (+ first-sheet preview in parallel) ──────────────────────

    def _handle_sheets(self):
        try:
            body = json.loads(self._read_body())
            path = body.get('path')
            url  = body.get('url')
            if not path and not url:
                return self._send_json({'error': 'path or url required'})

            sheets_result = [None]
            preview_result = [None]
            errors = []

            def run_list():
                cmd = _base_cmd()
                if path: cmd += ['--file', _resolve_path(path)]
                else:    cmd += ['--url', url]
                cmd += ['--list-sheets', '--json']
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                out = r.stdout.strip()
                idx = out.find('[')
                if idx >= 0:
                    sheets_result[0] = json.loads(out[idx:])
                else:
                    errors.append('No sheet list in output: ' + out + r.stderr)

            def run_preview():
                cmd = _base_cmd()
                if path: cmd += ['--file', _resolve_path(path)]
                else:    cmd += ['--url', url]
                cmd += ['--preview', '--json']
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                out = r.stdout.strip()
                idx = out.find('{')
                if idx >= 0:
                    preview_result[0] = json.loads(out[idx:])

            t1 = threading.Thread(target=run_list)
            t2 = threading.Thread(target=run_preview)
            t1.start(); t2.start()
            t1.join(); t2.join()

            if errors:
                return self._send_json({'error': errors[0]})
            self._send_json({'sheets': sheets_result[0] or [],
                             'preview': preview_result[0]})
        except Exception as e:
            self._send_json({'error': str(e)})

    # ── Schema Preview ───────────────────────────────────────────────────────

    def _handle_preview(self):
        try:
            body = json.loads(self._read_body())
            path       = body.get('path')
            url        = body.get('url')
            sheet      = body.get('sheet')
            header_row = body.get('headerRow', 0)

            cmd = _base_cmd()
            if path: cmd += ['--file', _resolve_path(path)]
            elif url: cmd += ['--url', url]
            else: return self._send_json({'error': 'path or url required'})
            if sheet: cmd += ['--sheet', sheet]
            if header_row: cmd += ['--header-row', str(header_row)]
            cmd += ['--preview', '--json']

            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            out = result.stdout.strip()
            idx = out.find('{')
            if idx >= 0:
                self._send_json(json.loads(out[idx:]))
            else:
                self._send_json({'error': 'No JSON in output: ' + out + result.stderr})
        except Exception as e:
            self._send_json({'error': str(e)})

    # ── Import (SSE) ─────────────────────────────────────────────────────────

    def _handle_import_sse(self):
        qs  = urllib.parse.parse_qs(self.path.split('?', 1)[1])
        get = lambda k, d='': qs.get(k, [d])[0]

        path      = get('path')
        url       = get('url')
        sheet     = get('sheet')
        dest      = get('dest')
        host      = get('host', 'localhost')
        port      = get('port', '9047')
        user      = get('user')
        password  = get('password')
        mode      = get('mode', 'create')
        overwrite = get('overwrite') == 'true'
        overrides = get('overrides', '').strip()
        exclude    = get('exclude', '').strip()
        rename     = get('rename', '').strip()
        header_row = get('headerRow', '0').strip()

        cmd = _base_cmd()
        if path: cmd += ['--file', _resolve_path(path)]
        elif url: cmd += ['--url', url]
        if sheet:      cmd += ['--sheet', sheet]
        if dest:       cmd += ['--dest', dest]
        if host:       cmd += ['--host', host]
        if port:       cmd += ['--port', port]
        if user:       cmd += ['--user', user]
        if password:   cmd += ['--password', password]
        if mode:       cmd += ['--mode', mode]
        if overrides:  cmd += ['--types', overrides]
        if exclude:    cmd += ['--exclude', exclude]
        if rename:     cmd += ['--rename', rename]
        if header_row and header_row != '0': cmd += ['--header-row', header_row]
        if overwrite: cmd.append('--overwrite')
        cmd.append('--yes')

        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1
            )
            for line in proc.stdout:
                line = line.rstrip('\n')
                self._sse_send(line)
            proc.wait()
            self._sse_send('__DONE__' if proc.returncode == 0 else '__ERROR__')
        except Exception as e:
            self._sse_send('Error: ' + str(e))
            self._sse_send('__ERROR__')

    def _sse_send(self, data):
        try:
            msg = f"data: {data}\n\n".encode('utf-8')
            self.wfile.write(msg)
            self.wfile.flush()
        except BrokenPipeError:
            pass

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _send_html(self, html):
        body = html.encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'text/html; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, data):
        body = json.dumps(data).encode('utf-8')
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def _send_error(self, code, msg):
        body = msg.encode()
        self.send_response(code)
        self.send_header('Content-Type', 'text/plain')
        self.send_header('Content-Length', len(body))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self):
        length = int(self.headers.get('Content-Length', 0))
        return self.rfile.read(length).decode('utf-8')

    # ── Fetch URL to local file ───────────────────────────────────────────────

    def _handle_fetch_url(self):
        try:
            body = json.loads(self._read_body())
            url  = body.get('url', '')
            if not url:
                return self._send_json({'error': 'url required'})
            uid  = str(uuid.uuid4())[:8]
            dest = UPLOAD_DIR / f"upload-{uid}.xlsx"
            req  = urllib.request.Request(url, headers={'User-Agent': 'Mozilla/5.0'})
            with urllib.request.urlopen(req, timeout=30) as resp:
                code = resp.getcode()
                if code and code >= 400:
                    return self._send_json({'error': f'HTTP {code} downloading URL'})
                with open(dest, 'wb') as f:
                    f.write(resp.read())
            if DOCKER_CONTAINER or KUBECTL_POD:
                _copy_to_container(str(dest), _resolve_path(str(dest)))
            self._send_json({'path': str(dest)})
        except Exception as e:
            self._send_json({'error': str(e)})

    # ── Append Schema Validation ─────────────────────────────────────────────

    def _handle_validate(self):
        try:
            body     = json.loads(self._read_body())
            host     = body.get('host', 'localhost')
            port     = int(body.get('port', 9047))
            user     = body.get('user', '')
            password = body.get('password', '')
            dest     = body.get('dest', '')

            token = _dremio_login(host, port, user, password)
            path_parts = [p for p in dest.split('.') if p]
            encoded = '/'.join(urllib.parse.quote(str(p), safe='') for p in path_parts)
            url = f'http://{host}:{port}/api/v3/catalog/by-path/{encoded}'
            req = urllib.request.Request(
                url, headers={'Authorization': f'_dremio{token}',
                              'Content-Type': 'application/json'})
            with urllib.request.urlopen(req, timeout=10) as resp:
                data = json.loads(resp.read())

            fields = []
            schema = data.get('fields', data.get('schema', {}).get('fieldList', []))
            for f in schema:
                name = f.get('name', '')
                type_info = f.get('type', {})
                type_name = type_info.get('name', 'VARCHAR') if isinstance(type_info, dict) else str(type_info)
                if name:
                    fields.append({'name': name, 'type': type_name})
            self._send_json({'fields': fields})
        except Exception as e:
            self._send_json({'error': str(e)})

    # ── Catalog Browser ──────────────────────────────────────────────────────

    def _handle_browse(self):
        try:
            body = json.loads(self._read_body())
            host     = body.get('host', 'localhost')
            port     = int(body.get('port', 9047))
            user     = body.get('user', '')
            password = body.get('password', '')
            path_str = body.get('path', '')

            token = _dremio_login(host, port, user, password)
            path_parts = [p for p in path_str.split('.') if p] if path_str else []
            items = _dremio_catalog_children(host, port, token, path_parts)
            self._send_json(items)
        except Exception as e:
            self._send_json({'error': str(e)})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _jar_path():
    return str(JAR_PATH)

def _base_cmd():
    if DOCKER_CONTAINER:
        return [_docker(), 'exec', DOCKER_CONTAINER, 'java', '-jar', CONTAINER_JAR_PATH]
    if KUBECTL_POD:
        return [_kubectl(), 'exec', KUBECTL_POD, '-n', KUBECTL_NAMESPACE,
                '--', 'java', '-jar', CONTAINER_JAR_PATH]
    java = os.environ.get('JAVA_BINARY', 'java')
    return [java, '-jar', _jar_path()]

def _resolve_path(host_path):
    """Translate a host upload path to a container/pod path."""
    if (DOCKER_CONTAINER or KUBECTL_POD) and host_path:
        return f"{CONTAINER_UPLOAD_DIR}/{Path(host_path).name}"
    return host_path

def _copy_to_container(src, dest):
    """Copy a local file into the active container (Docker or kubectl)."""
    if DOCKER_CONTAINER:
        subprocess.run(
            [_docker(), 'cp', src, f'{DOCKER_CONTAINER}:{dest}'],
            capture_output=True
        )
    elif KUBECTL_POD:
        subprocess.run(
            [_kubectl(), 'cp', src, f'{KUBECTL_POD}:{dest}', '-n', KUBECTL_NAMESPACE],
            capture_output=True
        )

def _exec_in_container(cmd_args):
    """Run a command inside the active container (Docker or kubectl). Returns CompletedProcess."""
    if DOCKER_CONTAINER:
        return subprocess.run(
            [_docker(), 'exec', DOCKER_CONTAINER] + cmd_args,
            capture_output=True
        )
    elif KUBECTL_POD:
        return subprocess.run(
            [_kubectl(), 'exec', KUBECTL_POD, '-n', KUBECTL_NAMESPACE, '--'] + cmd_args,
            capture_output=True
        )

# ---------------------------------------------------------------------------
# Dremio catalog browser helpers (stdlib HTTP — no extra deps)
# ---------------------------------------------------------------------------

def _dremio_login(host, port, user, password):
    data = json.dumps({'userName': user, 'password': password}).encode()
    req = urllib.request.Request(
        f'http://{host}:{port}/apiv2/login',
        data=data,
        headers={'Content-Type': 'application/json'}
    )
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read())['token']

def _dremio_catalog_children(host, port, token, path_parts):
    headers = {'Authorization': f'_dremio{token}', 'Content-Type': 'application/json'}
    if path_parts:
        encoded = '/'.join(urllib.parse.quote(str(p), safe='') for p in path_parts)
        url = f'http://{host}:{port}/api/v3/catalog/by-path/{encoded}'
    else:
        url = f'http://{host}:{port}/api/v3/catalog'
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        data = json.loads(resp.read())

    raw = data.get('children', data.get('data', []))
    items = []
    for child in raw:
        ctype = child.get('type', '')
        ct    = child.get('containerType', '')
        path  = child.get('path', [])
        name  = path[-1] if path else ''
        if not name:
            continue
        if ct == 'HOME':
            continue  # skip personal home spaces
        items.append({'name': name, 'type': ctype, 'containerType': ct, 'path': path})

    items.sort(key=lambda x: (x['type'] != 'CONTAINER', x['name'].lower()))
    return items


def setup_docker():
    """Copy the JAR into the container/pod and create the upload dir."""
    if DOCKER_CONTAINER:
        label = f"Docker container: {DOCKER_CONTAINER}"
    else:
        label = f"Kubernetes pod: {KUBECTL_POD} (namespace: {KUBECTL_NAMESPACE})"
    print(f"  {label}")
    print(f"  Copying JAR...", end=" ", flush=True)
    r_cp = None
    if DOCKER_CONTAINER:
        r_cp = subprocess.run(
            [_docker(), 'cp', str(JAR_PATH), f'{DOCKER_CONTAINER}:{CONTAINER_JAR_PATH}'],
            capture_output=True, text=True
        )
    elif KUBECTL_POD:
        r_cp = subprocess.run(
            [_kubectl(), 'cp', str(JAR_PATH),
             f'{KUBECTL_POD}:{CONTAINER_JAR_PATH}', '-n', KUBECTL_NAMESPACE],
            capture_output=True, text=True
        )
    if r_cp and r_cp.returncode != 0:
        print(f"\nERROR: {r_cp.stderr.strip()}")
        sys.exit(1)
    _exec_in_container(['mkdir', '-p', CONTAINER_UPLOAD_DIR])
    print("OK")

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    global JAR_PATH, DOCKER_CONTAINER, KUBECTL_POD, KUBECTL_NAMESPACE

    port      = DEFAULT_PORT
    jar_path  = DEFAULT_JAR
    docker    = ""
    kubectl   = ""
    namespace = "default"

    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == '--port' and i + 1 < len(args):
            port = int(args[i + 1]); i += 2
        elif args[i] == '--jar' and i + 1 < len(args):
            jar_path = Path(args[i + 1]); i += 2
        elif args[i] == '--docker' and i + 1 < len(args):
            docker = args[i + 1]; i += 2
        elif args[i] == '--kubectl' and i + 1 < len(args):
            kubectl = args[i + 1]; i += 2
        elif args[i] in ('--namespace', '-n') and i + 1 < len(args):
            namespace = args[i + 1]; i += 2
        else:
            i += 1

    if docker and kubectl:
        print("ERROR: Use --docker OR --kubectl, not both.")
        sys.exit(1)

    if not Path(jar_path).exists():
        print(f"ERROR: JAR not found: {jar_path}")
        print("Run 'bash install.sh' first to build the JAR.")
        sys.exit(1)

    JAR_PATH          = jar_path
    DOCKER_CONTAINER  = docker
    KUBECTL_POD       = kubectl
    KUBECTL_NAMESPACE = namespace

    print(f"Dremio Excel Importer UI")
    if DOCKER_CONTAINER or KUBECTL_POD:
        setup_docker()
    else:
        print(f"  JAR  : {JAR_PATH}")

    import socketserver
    class ThreadedHTTPServer(socketserver.ThreadingMixIn, http.server.HTTPServer):
        daemon_threads = True
    server = ThreadedHTTPServer(('', port), ImporterHandler)
    url = f"http://localhost:{port}"
    print(f"  URL  : {url}")
    print(f"  Temp : {UPLOAD_DIR}")
    print(f"\nOpen {url} in your browser. Press Ctrl+C to stop.")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        import shutil
        shutil.rmtree(UPLOAD_DIR, ignore_errors=True)


if __name__ == '__main__':
    main()
