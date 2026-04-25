# Dremio Zendesk Connector

A native Dremio 26.x storage plugin that exposes Zendesk Support data as queryable SQL tables via the Zendesk REST API. No JDBC driver required — uses Java 11's built-in `HttpClient` with Basic Auth (email/token).

## Tables

| Table | Zendesk Endpoint | Key Columns |
|-------|-----------------|-------------|
| `tickets` | `/api/v2/tickets.json` | id, subject, status, priority, requester_id, assignee_id, created_at, updated_at |
| `users` | `/api/v2/users.json` | id, name, email, role, active, organization_id, created_at |
| `organizations` | `/api/v2/organizations.json` | id, name, domain_names, group_id, created_at |
| `groups` | `/api/v2/groups.json` | id, name, description, deleted, created_at |
| `ticket_metrics` | `/api/v2/ticket_metrics.json` | id, ticket_id, reopens, replies, first_reply_time_calendar, full_resolution_time_calendar |
| `satisfaction_ratings` | `/api/v2/satisfaction_ratings.json` | id, ticket_id, score, comment, created_at |

> **Note:** `groups` is a SQL reserved word. Use double quotes: `SELECT * FROM zendesk."groups"`.

All timestamp columns are returned as `TIMESTAMP` (millisecond precision, timezone-naive). Array fields (e.g. `tags`, `domain_names`) are serialized as JSON strings.

## Prerequisites

- Dremio 26.x (tested on `26.0.5`)
- Zendesk account with API token access
- Zendesk agent role or higher (for `/api/v2/users/me.json` health check)

## Installation

### Docker (recommended for development)

```bash
# Copy the JAR into Dremio's 3rdparty plugins directory
docker cp jars/dremio-zendesk-connector-1.0.0.jar try-dremio:/opt/dremio/jars/3rdparty/

# Restart Dremio to pick up the new plugin
docker restart try-dremio
```

### Bare-metal / Kubernetes

```bash
cp jars/dremio-zendesk-connector-1.0.0.jar /opt/dremio/jars/3rdparty/
# Then restart Dremio
```

## Configuration

Create a Zendesk source in Dremio via the UI or the REST API.

### Via Dremio UI

1. Go to **Sources → Add Source → Zendesk**
2. Fill in:
   - **Subdomain**: your Zendesk subdomain (e.g. `acme` for `acme.zendesk.com`)
   - **Email**: your Zendesk agent email
   - **API Token**: generate one at `Admin → Apps & Integrations → Zendesk API`

### Via REST API

```bash
export ZENDESK_SUBDOMAIN=acme
export ZENDESK_EMAIL=agent@example.com
export ZENDESK_API_TOKEN=your_api_token

./add-zendesk-source.sh
```

Or directly:

```bash
TOKEN=$(curl -s -X POST http://localhost:9047/apiv2/login \
  -H 'Content-Type: application/json' \
  -d '{"userName":"dremio","password":"dremio123"}' | python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

curl -X PUT http://localhost:9047/apiv2/source/zendesk \
  -H 'Content-Type: application/json' \
  -H "Authorization: _dremio${TOKEN}" \
  -d '{
    "name": "zendesk",
    "type": "ZENDESK_REST",
    "config": {
      "subdomain": "acme",
      "email": "agent@example.com",
      "apiToken": "your_api_token",
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
-- Recent open tickets
SELECT id, subject, status, priority, created_at
FROM zendesk.tickets
WHERE status = 'open'
ORDER BY created_at DESC
LIMIT 20;

-- Ticket volume by assignee (join with users)
SELECT u.name AS assignee, COUNT(t.id) AS ticket_count
FROM zendesk.tickets t
JOIN zendesk.users u ON t.assignee_id = u.id
GROUP BY u.name
ORDER BY ticket_count DESC;

-- Average first-reply time by group (in minutes)
SELECT g.name AS group_name,
       AVG(tm.first_reply_time_calendar) AS avg_first_reply_minutes
FROM zendesk.ticket_metrics tm
JOIN zendesk."groups" g ON tm.ticket_id = g.id
GROUP BY g.name;

-- CSAT score distribution
SELECT score, COUNT(*) AS count
FROM zendesk.satisfaction_ratings
GROUP BY score;

-- Users created in the last 30 days
SELECT id, name, email, role, created_at
FROM zendesk.users
WHERE created_at >= CURRENT_TIMESTAMP - INTERVAL '30' DAY;
```

## Building from Source

The `rebuild.sh` script detects the running Dremio version, installs its JARs into your local Maven repo, compiles the connector against them, and deploys + restarts — all in one step.

```bash
# Build against the default Docker container (try-dremio)
./rebuild.sh

# Build against a named container
./rebuild.sh --docker my-dremio

# Force rebuild even if version is unchanged
./rebuild.sh --force

# Preview what would change without rebuilding
./rebuild.sh --dry-run
```

## Architecture

```
ZendeskStoragePlugin          — registers source type ZENDESK_REST, handles metadata
  └─ ZendeskConnection        — HTTP client, table registry, pagination
       └─ ZendeskScanDrel     — logical planning node
            └─ ZendeskScanPrel → ZendeskGroupScan → ZendeskSubScan
                                                         └─ ZendeskScanCreator
                                                              └─ ZendeskRecordReader
```

**Key implementation notes:**

- Pagination: Zendesk cursor-based pagination via `links.next`. The connector stops when `meta.has_more = false`, even though `links.next` may still be present.
- Schema: Static Arrow schema per table — no schema inference at runtime.
- Vector writing: Uses `mutator.getVector()` in `setup()` (never `addField()`), matching Dremio's internal connector pattern.
- Timestamps: All timestamps use `ArrowType.Timestamp(MILLISECOND, null)` (timezone-naive) because Dremio 26.x does not support timezone-aware Arrow timestamps in its vector class lookup.
- Nested fields: Dot-notation paths (e.g. `via.channel`, `first_resolution_time_in_minutes.calendar`) are resolved recursively.

## Authentication

Zendesk API uses Basic Auth with `email/token:api_token` format:

```
Authorization: Basic base64(email/token:api_token)
```

Generate an API token at: **Admin Center → Apps & Integrations → APIs → Zendesk API → Add API token**

## Limitations

- Read-only (SELECT only — no INSERT/UPDATE/DELETE)
- No predicate pushdown — all filtering happens in Dremio after fetching
- `ticket_comments` and `ticket_audits` are not included (high-volume child resources)
- Custom fields (`custom_fields` array on tickets/users) are returned as a JSON string, not expanded columns
- Zendesk rate limits apply: 700 requests/minute for most plans; the connector pages at 100 records/request
