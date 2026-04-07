# Installation

## The Short Version

The JAR is pre-built and included. All you need is **Java 11+**.

| Platform | How to start |
|---|---|
| **macOS** | Double-click **`Start Importer.command`** |
| **Windows** | Double-click **`Start Importer.bat`** |
| **Linux / CLI** | `python3 importer-ui.py` |

The launcher checks for Java, starts the web UI, and opens your browser automatically.

---

## Step 1 — Install Java (if you don't have it)

Check if Java is already installed:
```bash
java -version
```

If you see `java version "11"` or higher you're done — skip to Step 2.

If not:

**macOS (easiest):**
```bash
brew install openjdk@21
```
Or download from **https://adoptium.net** and run the installer.

**Windows:**
Download the `.msi` installer from **https://adoptium.net**, run it, done.

**Ubuntu / Debian:**
```bash
sudo apt install openjdk-21-jdk
```

> The `Start Importer.command` launcher on macOS will offer to install Java via Homebrew automatically if it's missing.

---

## Step 2 — Launch

**macOS:** Double-click `Start Importer.command`

A Terminal window opens, the server starts, and your browser goes to **http://localhost:8766**.

**Windows:** Double-click `Start Importer.bat`

Same result — browser opens automatically.

**Linux or manual start:**
```bash
python3 importer-ui.py
# → open http://localhost:8766
```

---

## That's it

No build step. No Maven. No dependencies to install. The `jars/dremio-excel-importer.jar` file already contains everything needed.

---

## Troubleshooting

**"Java not found" even though I installed it**
The launcher searches common locations. If yours is in an unusual path, start the UI manually:
```bash
JAVA_BINARY=/path/to/java python3 importer-ui.py
```

**No local Java — running Dremio in Docker**
```bash
python3 importer-ui.py --docker try-dremio
```

**No local Java — running Dremio on Kubernetes**
```bash
# The Dremio pod name + namespace (check with: kubectl get pods -A | grep dremio)
python3 importer-ui.py --kubectl dremio-master-0 --namespace dremio

# If port 9047 isn't externally exposed, forward it first:
kubectl port-forward svc/dremio 9047:9047 &
python3 importer-ui.py --kubectl dremio-master-0 --namespace dremio
```

The macOS launcher auto-detects Docker containers and Kubernetes pods — no manual flags needed for most setups.

**Port 8766 already in use**
```bash
python3 importer-ui.py --port 8767
```

**macOS: "cannot be opened because the developer cannot be verified"**
Right-click `Start Importer.command` → Open → Open (this one-time approval lets macOS run unsigned scripts).

**`Job FAILED: You cannot create a table in a space`**
The destination must be an Iceberg-compatible source, not a Dremio Space. See [Choosing a Destination](README.md#choosing-a-destination) in the README.

**`Job FAILED: Table already exists`**
Check **Overwrite if exists** in the web UI, or use `--overwrite` on the CLI.

**Importing a CSV file**
CSV files (`.csv`) work the same as `.xlsx` — drag and drop the file or use `--file data.csv`. The delimiter (comma, tab, or semicolon) is auto-detected.

**Wrong column types (e.g. zip codes as numbers)**
In the web UI, use the type dropdown next to each column in the Schema Preview to change the type before importing. On the CLI, use `--types "zip_code=VARCHAR"`.

---

## Building from Source (optional)

Only needed if you want to modify the Java code.

**With local Maven:**
```bash
bash install.sh
```

**Without local Maven (builds inside Docker):**
```bash
bash install.sh --docker try-dremio
```

Requires Java 11+ and Maven 3.6+, or a running Docker container that has them.
