# Dremio Splunk Connector — Installation Guide

---

## Prerequisites

| Requirement | Details |
|-------------|---------|
| Dremio OSS or Enterprise | 26.x (connector is version-pinned; rebuild for other versions) |
| Java 11+ | Required on the build machine if building from source |
| Maven 3.8+ | Required for build-from-source only; auto-installed in Docker if missing |
| `curl` | Required by `add-splunk-source.sh` and `test-connection.sh` |
| `python3` | Required by `add-splunk-source.sh` (for JSON payload building) |
| Splunk | On-prem (port 8089) or Splunk Cloud (port 443) reachable from Dremio's network |

---

## Step 1 — Install the Connector JAR

The connector ships as a single fat JAR (`dremio-splunk-connector-1.0.0-SNAPSHOT-plugin.jar`).
It must be placed in Dremio's `jars/3rdparty/` directory and Dremio must be restarted.

### Docker

```bash
# Default container name (try-dremio)
./install.sh --docker try-dremio --prebuilt

# Or just:
./install.sh
# (interactive — picks up running Dremio containers automatically)
```

### Bare-metal / VM

```bash
./install.sh --local /opt/dremio --prebuilt
# Then restart Dremio manually:
sudo systemctl restart dremio
```

### Kubernetes

```bash
./install.sh --k8s dremio-master-0 --namespace dremio --prebuilt
# Then rolling-restart:
kubectl rollout restart statefulset/dremio -n dremio
```

### Manual

```bash
cp jars/dremio-splunk-connector-1.0.0-SNAPSHOT-plugin.jar \
   /opt/dremio/jars/3rdparty/
# Restart Dremio
```

---

## Step 2 — Register a Splunk Source

After Dremio restarts, the **Splunk** source type appears under **Sources → +**.

### Using the script

```bash
# On-prem Splunk with username/password
./add-splunk-source.sh \
  --name splunk \
  --host splunk.example.com \
  --splunk-user admin \
  --splunk-pass changeme

# Splunk Cloud with bearer token
./add-splunk-source.sh \
  --name splunk_cloud \
  --host mycompany.splunkcloud.com \
  --cloud \
  --token eyJraWQ...

# Local dev Splunk (no SSL)
./add-splunk-source.sh \
  --name splunk_dev \
  --host localhost \
  --no-ssl \
  --splunk-user admin \
  --splunk-pass changeme
```

### Using the Dremio UI

1. Go to **Sources → + → Splunk**
2. Fill in Hostname, Port, and authentication credentials
3. Enable **Disable SSL Verification** for self-signed certs (dev/Docker)
4. Click **Save**

---

## Step 3 — Run Smoke Tests

```bash
./test-connection.sh \
  --source splunk \
  --index main \
  --user dremio \
  --password yourpassword
```

Expected output: **20/20 tests passing**.

---

## Keeping Up with Dremio Upgrades

When you upgrade Dremio, the connector JAR must be rebuilt against the new version:

```bash
# Automatically detects new Dremio version, rebuilds, and redeploys
./rebuild.sh

# Or against a specific container / install:
./rebuild.sh --docker my-dremio
./rebuild.sh --local /opt/dremio
./rebuild.sh --k8s dremio-0 --namespace dremio
```

`rebuild.sh` will:
1. Detect the running Dremio version
2. Update `pom.xml` to match
3. Install the live Dremio JARs into the local Maven repo
4. Compile the connector from source
5. Deploy the new JAR and restart Dremio

If nothing changed, it exits immediately with "Connector is up to date."

---

## SSL Notes

- **On-prem with valid cert**: set `--ssl` (default). No extra flags needed.
- **Docker / bare IP / self-signed cert**: add `--no-ssl` for plain HTTP, or keep SSL and enable **Disable SSL Verification** in the source config.
- **Splunk Cloud**: always uses HTTPS on port 443. Use `--cloud` flag or a bearer token.

The connector uses Java 11's built-in `HttpClient`. When `Disable SSL Verification` is enabled it installs a full-bypass `X509ExtendedTrustManager` that skips both certificate chain validation and hostname verification.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---------|-------------|-----|
| Source type `SPLUNK` not in dropdown | JAR not in `jars/3rdparty/` or Dremio not restarted | Check path, restart Dremio |
| `Splunk client not initialized` | Authentication failed or Splunk unreachable | Check hostname, port, credentials, SSL setting |
| `SSLHandshakeException: No subject alternative names` | SSL hostname check failing on bare IP | Enable **Disable SSL Verification** in source config |
| `UnsupportedOperationException: Timestamp(MILLISECOND, UTC)` | Old JAR before timestamp fix | Rebuild with `./rebuild.sh --force` |
| Jobs fail with `SYSTEM ERROR` | Schema/vector type mismatch | Open a GitHub issue with the job error ID |
| Schema missing fields | Splunk is schemaless — new fields not in sample | Refresh metadata in Dremio (right-click source → Refresh) |
