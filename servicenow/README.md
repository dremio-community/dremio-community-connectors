# Dremio ServiceNow Connector

A native Dremio 26.x storage plugin that exposes ServiceNow data as queryable SQL tables via the ServiceNow REST Table API. No JDBC driver required — uses Java 11's built-in `HttpClient` with Basic Auth.

## Tables

| Table | ServiceNow Endpoint |
|-------|-----------------|
| `incident` | `/api/now/table/incident` |
| `task` | `/api/now/table/task` |
| `sys_user` | `/api/now/table/sys_user` |
| `problem` | `/api/now/table/problem` |
| `change_request` | `/api/now/table/change_request` |

All timestamp columns are returned as `TIMESTAMP` (millisecond precision, timezone-naive). Array fields and complex objects are serialized as JSON strings.

## Prerequisites

- Dremio 26.x (tested on `26.0.5`)
- ServiceNow Instance URL (e.g., `https://dev12345.service-now.com`)
- ServiceNow Integration User account with Basic Auth access and read privileges to the targeted tables.

## Installation

### Docker (recommended for development)

```bash
# Copy the JAR into Dremio's 3rdparty plugins directory
docker cp jars/dremio-servicenow-connector-1.0.0.jar try-dremio:/opt/dremio/jars/3rdparty/

# Restart Dremio to pick up the new plugin
docker restart try-dremio
```

### Bare-metal / Kubernetes

```bash
cp jars/dremio-servicenow-connector-1.0.0.jar /opt/dremio/jars/3rdparty/
# Then restart Dremio
```

## Configuration

Create a ServiceNow source in Dremio via the UI or the REST API.

### Via Dremio UI

1. Go to **Sources → Add Source → ServiceNow**
2. Fill in:
   - **Instance URL**: your full ServiceNow instance URL (e.g. `https://dev12345.service-now.com`)
   - **Username**: your ServiceNow API user
   - **Password**: your ServiceNow API password

### Via REST API

```bash
./add-servicenow-source.sh --name servicenow \
  --instance-url https://dev12345.service-now.com \
  --username admin \
  --password secret \
  --dremio-password YOUR_DREMIO_PASSWORD
```

Or directly:

```bash
TOKEN=$(curl -s -X POST http://localhost:9047/apiv2/login \
  -H 'Content-Type: application/json' \
  -d '{"userName":"dremio","password":"dremio123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

curl -X PUT http://localhost:9047/apiv2/source/servicenow \
  -H 'Content-Type: application/json' \
  -H "Authorization: _dremio${TOKEN}" \
  -d '{
    "name": "servicenow",
    "type": "SERVICENOW_REST",
    "config": {
      "instanceUrl": "https://dev12345.service-now.com",
      "username": "admin",
      "password": "secret",
      "pageSize": 100,
      "queryTimeoutSeconds": 120
    },
    "metadataPolicy": {
      "autoPromoteDatasets": true
    }
  }'
```

**Important:** Set `autoPromoteDatasets: true` so tables are immediately queryable without manual promotion.

## Usage

```sql
-- Recent open incidents
SELECT number, short_description, priority, opened_at
FROM servicenow.incident
WHERE state = 1
ORDER BY opened_at DESC
LIMIT 20;

-- Users created in the last 30 days
SELECT user_name, first_name, last_name, email, sys_created_on
FROM servicenow.sys_user
WHERE sys_created_on >= CURRENT_TIMESTAMP - INTERVAL '30' DAY;
```

## Building from Source

The `rebuild.sh` script detects the running Dremio version, installs its JARs into your local Maven repo, compiles the connector against them, and deploys + restarts — all in one step.

```bash
# Build against the default Docker container (try-dremio)
./rebuild.sh

# Build against a named container
./rebuild.sh --docker dremio

# Force rebuild even if version is unchanged
./rebuild.sh --force

# Preview what would change without rebuilding
./rebuild.sh --dry-run
```

## Architecture

```
ServiceNowStoragePlugin          — registers source type SERVICENOW_REST, handles metadata
  └─ ServiceNowConnection        — HTTP client, table registry, pagination
       └─ ServiceNowScanDrel     — logical planning node
            └─ ServiceNowScanPrel → ServiceNowGroupScan → ServiceNowSubScan
                                                         └─ ServiceNowScanCreator
                                                              └─ ServiceNowRecordReader
```

**Key implementation notes:**

- Pagination: ServiceNow cursor-based pagination via standard HTTP `Link` headers (rel="next"). The connector stops when no `next` link is provided.
- Schema: Static Arrow schema per table — no schema inference at runtime.
- Vector writing: Uses `mutator.getVector()` in `setup()` (never `addField()`), matching Dremio's internal connector pattern.
- Timestamps: ServiceNow returns timestamps in a space-delimited `yyyy-MM-dd HH:mm:ss` UTC format. The connector natively converts this to ISO8601 to feed into `ArrowType.Timestamp(MILLISECOND, null)`.

## Authentication

ServiceNow API uses Basic Auth with standard `Username` and `Password` combination for API service accounts.

## Limitations

- Read-only (SELECT only — no INSERT/UPDATE/DELETE)
- No predicate pushdown — all filtering happens in Dremio after fetching
- ServiceNow rate limits apply: the connector pages at 100 records/request by default
