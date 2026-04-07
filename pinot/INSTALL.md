# Installing the Dremio Apache Pinot Connector

## Prerequisites

- Running Dremio 26.x instance (Docker, bare-metal, or Kubernetes)
- Apache Pinot cluster accessible from Dremio (controller port reachable)
- Maven 3.6+ and Java 11+ (for building from source)

## Installation Methods

### Option A: Docker (Recommended for Development)

```bash
# Build and deploy to a running Docker container
./install.sh --docker <container-name>

# Example with default container name:
./install.sh --docker try-dremio
```

The script will:
1. Detect the Dremio version from the container
2. Install required JARs from the container into Maven local repo
3. Build the connector JAR (includes the Pinot JDBC driver)
4. Copy the JAR to `jars/3rdparty/` and restart Dremio

### Option B: Bare Metal

```bash
./install.sh --local /opt/dremio
```

### Option C: Kubernetes

```bash
./install.sh --k8s <coordinator-pod> --namespace <ns>
```

See [k8s/KUBERNETES.md](k8s/KUBERNETES.md) for full Kubernetes options.

## Manual Installation

If you prefer to build and deploy manually:

```bash
# 1. Install Dremio JARs from your running instance into Maven local repo
#    (adjust the path to your Dremio installation)
mvn install:install-file -Dfile=/opt/dremio/jars/dremio-ce-jdbc-plugin-*.jar \
  -DgroupId=com.dremio.plugins -DartifactId=dremio-ce-jdbc-plugin \
  -Dversion=<dremio-version> -Dpackaging=jar

mvn install:install-file -Dfile=/opt/dremio/jars/dremio-ce-jdbc-fetcher-api-*.jar \
  -DgroupId=com.dremio.plugins -DartifactId=dremio-ce-jdbc-fetcher-api \
  -Dversion=<dremio-version> -Dpackaging=jar

# ... (see install.sh for all required JARs)

# 2. Build
mvn package -DskipTests

# 3. Deploy — single JAR (includes Pinot JDBC driver)
cp target/dremio-pinot-connector-*-plugin.jar /opt/dremio/jars/3rdparty/

# 4. Restart Dremio
```

## Adding the Source in Dremio UI

1. Click **Add Source** in the left panel
2. Select **Apache Pinot**
3. Configure:
   - **Controller Host**: hostname or IP of your Pinot controller
   - **Controller Port**: `9000` (default) or your custom port
   - **Username / Password**: only if your cluster has authentication enabled
4. Click **Save** — Dremio will connect and discover all Pinot tables

## Verifying the Installation

```sql
-- List all Pinot tables visible from Dremio
SHOW TABLES IN pinot_source;

-- Run a test query
SELECT * FROM pinot_source.my_table LIMIT 10;
```

## Rebuilding After a Dremio Upgrade

```bash
./rebuild.sh --docker <container-name>
```

The rebuild script detects the new Dremio version, updates `pom.xml`, rebuilds, and redeploys automatically.

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---|---|---|
| Source shows red / connection failed | Controller not reachable | Check host, port, and network connectivity from Dremio to Pinot controller |
| `ClassNotFoundException: org.apache.pinot.client.PinotDriver` | JAR not in 3rdparty | Rebuild with `./install.sh`; check that `*-plugin.jar` is in `jars/3rdparty/` |
| No tables visible | Auth failure or wrong controller | Verify credentials; check Pinot controller logs |
| Query timeout | Large scan, no LIMIT | Add `LIMIT` clause or push down a time filter |
| TLS handshake failure | Certificate mismatch | Verify Pinot controller TLS config; check if `useTls=true` is needed |
