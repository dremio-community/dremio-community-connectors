# Dremio PagerDuty Native Connector

A native Java-based storage plugin for Dremio that enables direct SQL querying of PagerDuty operational data (`incidents`, `services`, `users`, and `on-calls`). 

This connector runs natively within Dremio's Sabot execution engine. It uses Java 11's asynchronous `HttpClient` to fetch data from the PagerDuty REST API and maps JSON payloads directly into Apache Arrow vectors on-the-fly, completely bypassing any external database or staging files.

---

## Codebase Assets & File Paths

Below is the complete inventory of files that constitute this connector:

### 1. Build and Scaffolding
- **Project POM:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/pom.xml`
- **Dependency Installer:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/install_deps_in_container.sh`
- **Rebuild script:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/rebuild.sh`
- **Deploy script:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/install.sh`
- **Compiled Plugin Package:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/jars/dremio-pagerduty-connector-1.0.0.jar`

### 2. Configuration & Resources
- **UI Form Layout:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/src/main/resources/pagerduty-layout.json`
- **SVG Branding Icon:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/src/main/resources/PAGERDUTY_REST.svg`
- **Sabot Registration:** `/Users/mark/Desktop/Claude Projects/dremio-community-connectors/pagerduty/src/main/resources/sabot-module.conf`

### 3. Java Source Code (`src/main/java/com/dremio/plugins/pagerduty/`)
- **Plugin Lifecycle & Metadata:** `PagerDutyStoragePlugin.java`
- **Source Configuration:** `PagerDutyConf.java`
- **REST Client & Table Schemas:** `PagerDutyConnection.java`
- **Arrow Vector Record Reader:** `PagerDutyRecordReader.java`
- **Scan Specification POJO:** `PagerDutyScanSpec.java`
- **Calcite Rules Registry:** `PagerDutyRulesFactory.java`
- **Logical Calcite Scan Rule:** `PagerDutyScanRule.java`
- **Physical Calcite Scan Rule:** `PagerDutyScanPrule.java`
- **Logical Relational Scan Node:** `PagerDutyScanDrel.java`
- **Physical Relational Scan Node:** `PagerDutyScanPrel.java`
- **Execution Split Coordinator:** `PagerDutyGroupScan.java`
- **Execution Sub-Scan Task:** `PagerDutySubScan.java`
- **Operator Execution Creator:** `PagerDutyScanCreator.java`

---

## Build & Deployment

Because this connector compiles against Dremio's internal API libraries, it is built directly inside the target Dremio Docker container (`dremio-test`).

### Step 1: Install Build Tools & Cache Dependencies
Run the dependency installer to copy Dremio's internal libraries (`dremio-common`, `dremio-sabot-kernel`, etc.) into the container's local Maven repository:
```bash
./install_deps_in_container.sh
```

### Step 2: Compile & Build
Copy the workspace source code to the container's `/tmp/` filesystem and trigger the Maven package build:
```bash
docker exec dremio-test rm -rf /tmp/pagerduty-rebuild
docker exec dremio-test mkdir -p /tmp/pagerduty-rebuild
docker cp src dremio-test:/tmp/pagerduty-rebuild/
docker cp pom.xml dremio-test:/tmp/pagerduty-rebuild/
docker exec dremio-test bash -c "cd /tmp/pagerduty-rebuild && mvn package -DskipTests --batch-mode"
```

### Step 3: Deploy & Load
Copy the compiled JAR to Dremio's third-party classpath and restart the container:
```bash
# Copy compiled jar from container to host local jars/ directory
mkdir -p jars
docker cp dremio-test:/tmp/pagerduty-rebuild/jars/dremio-pagerduty-connector-1.0.0.jar jars/

# Copy compiled jar to container's Dremio classpath
docker exec -u root dremio-test cp /tmp/pagerduty-rebuild/jars/dremio-pagerduty-connector-1.0.0.jar /opt/dremio/jars/3rdparty/

# Restart Dremio to load plugin
docker restart dremio-test
```

---

## Configuration Options

When configuring the PagerDuty source in the Dremio console, you will be prompted for:
1. **API Token**: A secure PagerDuty API key (either account-level or user-level).
2. **EU Account Region**: Check this box if your PagerDuty account is hosted in the EU region (`api.eu.pagerduty.com`).
3. **Page Size**: Number of records fetched per API page (defaults to `100`, max `100`).
4. **Query Timeout**: Timeout limit for REST HTTP calls (defaults to `120` seconds).

---

## Schema & Tables

The connector registers 4 native tables in Dremio:

### 1. `incidents`
- `id` (VARCHAR)
- `incident_number` (INT)
- `title` (VARCHAR)
- `status` (VARCHAR): `triggered`, `acknowledged`, or `resolved`.
- `created_at` (TIMESTAMP)
- `urgency` (VARCHAR)
- `service_id` (VARCHAR)
- `service_summary` (VARCHAR)
- `assignees` (VARCHAR): Comma-separated list of assigned user IDs.
- `teams` (VARCHAR): Comma-separated list of team IDs associated.

### 2. `services`
- `id` (VARCHAR)
- `name` (VARCHAR)
- `status` (VARCHAR)
- `created_at` (TIMESTAMP)
- `escalation_policy_id` (VARCHAR)

### 3. `users`
- `id` (VARCHAR)
- `name` (VARCHAR)
- `email` (VARCHAR)
- `role` (VARCHAR)
- `time_zone` (VARCHAR)

### 4. `oncalls`
- `user_id` (VARCHAR)
- `user_summary` (VARCHAR)
- `escalation_policy_id` (VARCHAR)
- `escalation_level` (INT)
- `start` (TIMESTAMP)
- `end` (TIMESTAMP)

---

## Example Queries

Once configured, you can perform analytics directly via SQL in Dremio:

### Count Incidents by Urgency & Status
```sql
SELECT urgency, status, COUNT(*) as count
FROM pagerduty.incidents
GROUP BY urgency, status;
```

### Identify Who is Currently On-Call
```sql
SELECT user_summary, escalation_level, start, "end"
FROM pagerduty.oncalls
WHERE CURRENT_TIMESTAMP BETWEEN start AND "end"
ORDER BY escalation_level ASC;
```

### Join Incidents with Service Details
```sql
SELECT i.incident_number, i.title, s.name as service_name, i.urgency, i.created_at
FROM pagerduty.incidents i
JOIN pagerduty.services s ON i.service_id = s.id
WHERE i.status <> 'resolved';
```
