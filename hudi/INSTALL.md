# Dremio Apache Hudi Connector — Install Guide

Adds **read and write** support for Apache Hudi tables to Dremio 26.x.

> **Dremio 26.x DML note:** `INSERT INTO` is Iceberg-only in Dremio 26.x and is not
> supported on Hudi tables. Use `CTAS` to write data. Tables are **queryable immediately
> after CTAS** — no manual promotion step required. `HudiFormatMatcher` auto-discovers
> Hudi tables by detecting `.hoodie/` directories. See [README.md](README.md) for details.

---

## Prerequisites

| Requirement | Version |
|---|---|
| Dremio | 26.0.5 (match `dremio.version` in `pom.xml`) |
| Java | 11+ (inside the container — host does not need Java or Maven) |
| Docker | Any recent version (for the quick-start path) |

---

## Quick Start — Docker (recommended)

### 1. Verify Dremio is running

```bash
docker ps | grep try-dremio
```

If not running, start it and complete the initial Dremio setup (admin user, etc.) first.

### 2. Run the installer

From the `dremio-hudi-connector/` directory:

```bash
chmod +x install.sh
./install.sh
```

The script does everything inside the container:
- Installs Maven on first run
- Installs all required Dremio + Arrow + Hadoop JARs into the container's local Maven repo
- Builds the connector fat JAR (158 MB, bundles Hudi 0.15.0 + Avro + Parquet-Avro)
- Deploys the JAR to `/opt/dremio/jars/3rdparty/`
- Restarts Dremio automatically

### 3. Wait for Dremio to restart

Allow ~30 seconds, then open `http://localhost:9047`.

### 4. Add the Hudi source

1. Go to **Sources** → **+ Add Source** → select **Apache Hudi**
2. Fill in:

| Field | Description | Example |
|---|---|---|
| **Root Path** | Base directory containing your Hudi tables | `/test-data/hudi` |
| **Default Table Type** | COW or MOR for new tables | `COPY_ON_WRITE` |
| **Default Record Key Field** | Unique record key column | `user_id` |
| **Default Precombine Field** | Tiebreaker on upsert | `created_at` |

3. Click **Save**

---

## Manual Install (non-Docker)

### 1. Install Dremio JARs into your local Maven repo

These JARs are not on Maven Central and must be installed from your Dremio installation:

```bash
DREMIO_HOME=/opt/dremio   # adjust to your install path
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
HADOOP_VER=3.3.6-dremio-202507241551560856-75923ad5

install_jar() {
  mvn install:install-file \
    -Dfile="$1" -DgroupId="$2" -DartifactId="$3" -Dversion="$4" \
    -Dpackaging=jar -q
}

JARS=$DREMIO_HOME/jars
TP=$JARS/3rdparty

install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar          com.dremio dremio-sabot-kernel          $DREMIO_VER
install_jar $JARS/dremio-common-${DREMIO_VER}.jar                com.dremio dremio-common                $DREMIO_VER
install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar         com.dremio.plugin dremio-plugin-common  $DREMIO_VER
install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar    com.dremio dremio-sabot-kernel-proto    $DREMIO_VER
install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar    com.dremio dremio-sabot-vector-tools    $DREMIO_VER
install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar    com.dremio dremio-services-namespace    $DREMIO_VER
install_jar $JARS/dremio-connector-${DREMIO_VER}.jar             com.dremio dremio-connector             $DREMIO_VER
install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar         com.dremio dremio-sabot-logical         $DREMIO_VER
install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar           com.dremio dremio-common-core           $DREMIO_VER
install_jar $TP/arrow-vector-${ARROW_VER}.jar                    org.apache.arrow arrow-vector           $ARROW_VER
install_jar $TP/arrow-memory-core-${ARROW_VER}.jar               org.apache.arrow arrow-memory-core      $ARROW_VER
install_jar $TP/arrow-format-${ARROW_VER}.jar                    org.apache.arrow arrow-format           $ARROW_VER
install_jar $TP/hadoop-common-${HADOOP_VER}.jar                  org.apache.hadoop hadoop-common         $HADOOP_VER
install_jar $TP/config-1.4.3.jar                                 com.typesafe config                     1.4.3
install_jar $TP/guava-33.4.0-jre.jar                             com.google.guava guava                  33.4.0-jre
install_jar $TP/javax.inject-1.jar                               javax.inject javax.inject               1
```

### 2. Build

```bash
cd dremio-hudi-connector
mvn package -DskipTests
```

Output: `target/dremio-hudi-connector-1.0.0-SNAPSHOT-plugin.jar`

### 3. Deploy

```bash
cp target/dremio-hudi-connector-1.0.0-SNAPSHOT-plugin.jar \
   $DREMIO_HOME/jars/3rdparty/
```

### 4. Restart Dremio

```bash
# Standalone:
$DREMIO_HOME/bin/dremio restart

# Systemd:
sudo systemctl restart dremio
```

---

## After Install: Writing and Reading a Hudi Table

### Write (CTAS)

```sql
CREATE TABLE hudi_local.my_table
STORE AS (type => 'hudi')
AS SELECT * FROM some_source.some_table;
```

### Read (no promotion required)

Tables are immediately queryable after CTAS. `HudiFormatMatcher` auto-detects the
`.hoodie/` directory and registers the table in Dremio's catalog automatically.

```sql
SELECT COUNT(*) FROM hudi_local.my_table;
SELECT * FROM hudi_local.my_table LIMIT 100;
```

> **Existing Hudi tables** (pre-populated environments): point Dremio at the source root
> and run a metadata refresh — all directories containing `.hoodie/` will be auto-registered
> as datasets without any manual promotion.

---

## Keeping Up with Dremio Upgrades

When Dremio is upgraded, the connector JAR must be rebuilt against the new Dremio JARs.
`rebuild.sh` automates the entire process — version detection, `pom.xml` update, compile, deploy, restart:

```bash
# Docker (default container: try-dremio)
./rebuild.sh

# Named container
./rebuild.sh --docker my-dremio

# Bare-metal
./rebuild.sh --local /opt/dremio

# Kubernetes — auto-deploys to coordinator + all executor pods
./rebuild.sh --k8s dremio-master-0
./rebuild.sh --k8s dremio-master-0 --namespace dremio-ns   # with namespace

# Preview what would change without touching anything
./rebuild.sh --dry-run

# Force rebuild even if version strings already match (after source code edits)
./rebuild.sh --force
```

**What it does:**
1. Reads JAR filenames from the running instance to detect `dremio.version` and `arrow.version`
2. Compares against `pom.xml` — exits cleanly if nothing changed
3. Updates `pom.xml`, backs up the previous version to `pom.xml.bak`
4. Installs Dremio JARs from the running instance into the Maven local repo
5. Builds the connector from source against the new JARs
6. Deploys the new JAR to every pod / container
7. Restarts Dremio (Docker only) or prints restart instructions

If the build fails, `rebuild.sh` restores `pom.xml` automatically and prints a diagnostic guide for common API-change errors.

---

## Troubleshooting

**Source doesn't appear in Add Source**
- Confirm the JAR is in `$DREMIO_HOME/jars/3rdparty/`
- Check Dremio startup logs: `grep -i hudi /opt/dremio/log/server.log`
- Confirm `sabot-module.conf` is inside the JAR: `jar tf dremio-hudi-connector-*-plugin.jar | grep sabot`

**CTAS succeeds but SELECT returns "Object not found"**
- Try querying the table by its full path: `SELECT * FROM hudi_local.output.my_table LIMIT 1`
- Right-click the source in Dremio → **Refresh Metadata** → retry the query.
- If still not found, check Dremio logs for FormatMatcher errors: `docker logs try-dremio | grep -i hudi`
- As a fallback, manually promote the folder: hover → `⋮` → Format → Parquet → Save

**INSERT INTO fails with "not configured to support DML operations"**
- This is a Dremio 26.x architectural constraint. INSERT INTO requires Iceberg-backed tables.
- Use CTAS instead. See [README.md](README.md) for the full explanation.

**SELECT returns stale or wrong row count after re-running CTAS**
- Delete the old data from the container: `docker exec -u root try-dremio rm -rf /test-data/hudi/my_table`
- Re-run CTAS
- Right-click the source → **Refresh Metadata** if the table doesn't update automatically

**Build fails with "artifact not found"**
- Run the JAR install step (Step 1 above) before building.

**Write fails with schema or record key errors**
- Ensure `defaultRecordKeyField` matches an actual column name in your SELECT list.
- The record key field must be present in every row — nulls are not permitted.

**ClassNotFoundException on first query after deploy**
- Dremio needs a full restart (not just a metadata refresh) to load new JARs.
- `docker restart try-dremio`
