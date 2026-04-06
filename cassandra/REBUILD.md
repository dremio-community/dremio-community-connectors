# Rebuilding the Connector After a Dremio Upgrade

When you upgrade Dremio to a new version, the connector JAR must be recompiled against
the new Dremio JARs. This guide covers both the one-click browser UI and the command-line
tool for every supported platform.

---

## The Quickest Way: Double-Click

No terminal required. Pick your platform:

### macOS
Double-click **`Rebuild Connector.command`** in Finder.

> **First time only:** macOS may show a security warning ("unidentified developer").
> Right-click the file → **Open** → **Open** to approve it once.

### Windows
Double-click **`Rebuild Connector.bat`** in File Explorer.

> Requires Python 3 installed from [python.org](https://www.python.org/downloads/)
> with **"Add Python to PATH"** checked during installation.

### Linux / Linux Mint (terminal)
```bash
cd /path/to/dremio-cassandra-connector
chmod +x "Rebuild Connector.sh"    # once only
./"Rebuild Connector.sh"
```

### Linux Mint (desktop icon)
1. Open **`Rebuild Connector.desktop`** in a text editor
2. Replace both `/HOME/USER/dremio-cassandra-connector` lines with your actual repo path:
   ```
   Exec=/bin/bash -c 'cd "/home/mark/dremio-cassandra-connector" && ./"Rebuild Connector.sh"'
   Path=/home/mark/dremio-cassandra-connector
   ```
3. Copy the file to your Desktop:
   ```bash
   cp "Rebuild Connector.desktop" ~/Desktop/
   ```
4. Right-click the new desktop icon → **Allow Launching**

After that it's a single double-click — no terminal needed.

---

## Using the Browser UI

All launchers start the same web server. You can also start it manually:

```bash
python3 rebuild-ui.py
```

Your browser opens automatically at **http://localhost:8765**. The UI looks like this:

```
┌─────────────────────────────────────────────────┐
│  🔵 Dremio Cassandra Connector — Rebuild        │
│  Detects your Dremio version, updates pom.xml,  │
│  recompiles, and redeploys                      │
├─────────────────────────────────────────────────┤
│  Target Environment                             │
│  [ 🐳 Docker ] [ 🖥 Local ] [ ☸️ Kubernetes ]  │
│                                                 │
│  Container name: [try-dremio          ]         │
│                                                 │
│  ☐ Force rebuild                                │
│                                                 │
│  [▶ Rebuild & Deploy]  [🔍 Dry Run]             │
├─────────────────────────────────────────────────┤
│  Output                             [Copy log]  │
│  ● Ready — click Rebuild to start              │
│                                                 │
│  > Step 1: Detecting Dremio version...          │
│  ✓ Detected: 26.1.0-...                        │
│  → Updating pom.xml                            │
│  ✓ pom.xml updated                             │
│  ...                                            │
└─────────────────────────────────────────────────┘
```

### UI Options

| Control | What it does |
|---|---|
| **Docker** tab | Targets a running Docker container; enter its name |
| **Local** tab | Targets a bare-metal Dremio install; enter the home directory path |
| **Kubernetes** tab | Targets a K8s pod; enter the pod name |
| **Force rebuild** | Rebuilds even if the detected Dremio version already matches `pom.xml` — useful after source code changes |
| **▶ Rebuild & Deploy** | Full rebuild: detect → update pom → compile → deploy → restart |
| **🔍 Dry Run** | Detects the running version and shows what would change — no files are modified |
| **Copy log** | Copies the full terminal output to your clipboard |

### Output Colors

| Color | Meaning |
|---|---|
| 🟢 Green | Step succeeded |
| 🟡 Yellow | Warning (non-fatal) |
| 🔴 Red | Error — rebuild failed |
| 🔵 Blue | Progress step |
| White | Informational output |

Press **Ctrl+C** in the terminal (or close the terminal window) to stop the server.

---

## Command-Line Usage (no browser)

If you prefer the terminal directly:

```bash
# Docker container (default name: try-dremio)
./rebuild.sh --docker try-dremio

# Custom Docker container name
./rebuild.sh --docker my-dremio-container

# Bare-metal / VM install
./rebuild.sh --local /opt/dremio

# Kubernetes pod (default namespace)
./rebuild.sh --k8s dremio-0

# Kubernetes pod with namespace
./rebuild.sh --k8s dremio-master-0 --namespace dremio-ns
./rebuild.sh --k8s dremio-master-0 -n dremio-ns

# Kubernetes pod with custom pod selector (for multi-pod deploy override)
./rebuild.sh --k8s dremio-master-0 -n dremio-ns --pod-selector app=dremio

# Dry run — see what version is running and what would change
./rebuild.sh --dry-run

# Force rebuild even if version unchanged (e.g. after code edits)
./rebuild.sh --docker try-dremio --force
```

### What the script does

1. **Detect versions** — reads JAR filenames from the running instance to determine
   the exact Dremio, Arrow, Hadoop, and Calcite versions currently installed
2. **Compare** — diffs detected versions against `pom.xml`
3. **Exit early** — if versions match and `--force` not set, prints "nothing to rebuild"
   and exits cleanly (exit code 0)
4. **Update `pom.xml`** — patches version strings in-place; saves `pom.xml.bak` first
5. **Install JARs** — copies JARs from the running instance and installs them into
   your local Maven repo (`~/.m2`)
6. **Compile** — runs `mvn package -DskipTests --batch-mode`
7. **Deploy** — copies the new JAR to `jars/3rdparty/` and saves a copy to `jars/`
   for future `--prebuilt` installs
8. **Restart Dremio** — `docker restart` (Docker); warns to restart manually (local/K8s)

**If compilation fails:**
- `pom.xml.bak` is restored automatically — you are not left with a broken POM
- The output shows which of the three common failure modes to investigate:
  - Planning rules (Calcite API changes)
  - Abstract method changes (Dremio plugin interface updates)
  - Package moves (classes relocated between Dremio modules)

### Exit codes

| Code | Meaning |
|---|---|
| `0` | Success (or already up-to-date) |
| `1` | Version detection failed (Dremio not running, or wrong container name) |
| `2` | Build failed — see output for compilation errors |
| `3` | Deployment failed |

---

## Requirements

| Requirement | Notes |
|---|---|
| **Python 3** | For the browser UI only. Ships with macOS 12+; `sudo apt install python3` on Linux; install from [python.org](https://www.python.org/downloads/) on Windows. |
| **Maven 3.8+** | Required for rebuild. Usually pre-installed in the Dremio container; `rebuild.sh` installs it automatically if not found. |
| **Docker / K8s / bare-metal** | Whichever environment your Dremio runs in. |
| **Bash** | For `rebuild.sh` — standard on macOS and Linux. On Windows, use Git Bash or WSL. |

---

## Troubleshooting

**"Python 3 not found" on macOS**
```bash
# Install via Homebrew
brew install python
# Or download from https://www.python.org/downloads/
```

**"Python 3 not found" on Linux Mint / Ubuntu**
```bash
sudo apt install python3
```

**Browser doesn't open automatically**
Navigate manually to **http://localhost:8765**.

**Port 8765 is already in use**
```bash
python3 rebuild-ui.py --port 9000
```

**"Dremio container not found"**
Check the correct container name with `docker ps`, then pass it with `--docker NAME`.

**Build fails with `NoSuchMethodError` / `AbstractMethodError`**
This means Dremio changed a plugin API method signature. See the `BUILD_JOURNAL.md`
Phase 18 section and the output from `rebuild.sh` for guidance on which class to fix.

**`pom.xml.bak` left behind after a successful rebuild**
Safe to delete: `rm pom.xml.bak`. It is only kept as a safety net during the build.
