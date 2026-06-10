# Dremio PagerDuty Connector

A native Dremio storage plugin that exposes [PagerDuty](https://www.pagerduty.com/) operational data as SQL tables using the PagerDuty REST API v2. Query incidents, services, users, and on-call schedules directly from Dremio — no ETL required.

## Tables

| Table | Description |
|-------|-------------|
| `incidents` | All incidents — id, number, title, status, urgency, service, assignee, escalation policy, teams |
| `services` | Configured services — id, name, description, status, escalation policy |
| `users` | Account users — id, name, email, role, time zone |
| `oncalls` | Current and scheduled on-call entries — user, schedule, escalation policy, level, start/end times |

## Requirements

- Dremio 26.x
- PagerDuty account with a REST API token (account-level or user-level)

## Install

```bash
./install.sh --docker dremio-test --prebuilt   # Docker, pre-built JAR
./install.sh --docker dremio-test --build      # Docker, build from source
./install.sh --local /opt/dremio --prebuilt    # bare-metal
./install.sh --k8s dremio-0 --prebuilt         # Kubernetes
```

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| API Token | _(required)_ | PagerDuty REST API token — account-level or user-level |
| EU Region | `false` | Check if your account is hosted in the EU (`api.eu.pagerduty.com`) |
| Page Size | `100` | Records fetched per API page (max 100) |
| Query Timeout | `120` | HTTP request timeout in seconds |

### Getting an API Token

1. Log into your PagerDuty account
2. Go to **My Profile → User Settings → Create API User Token**
3. Copy the token and paste it into the Dremio source configuration

## Example Queries

```sql
-- Count open incidents by urgency
SELECT urgency, status, COUNT(*) AS cnt
FROM pagerduty.incidents
WHERE status != 'resolved'
GROUP BY urgency, status
ORDER BY cnt DESC;

-- Who is currently on-call?
SELECT user_name, escalation_policy_name, escalation_level, start, "end"
FROM pagerduty.oncalls
WHERE CURRENT_TIMESTAMP BETWEEN start AND "end"
ORDER BY escalation_level ASC;

-- Join incidents to services
SELECT i.incident_number, i.title, s.name AS service_name, i.urgency, i.created_at
FROM pagerduty.incidents i
JOIN pagerduty.services s ON i.service_id = s.id
WHERE i.status != 'resolved'
ORDER BY i.created_at DESC;

-- On-call users with contact details
SELECT o.user_name, u.email, o.escalation_policy_name, o.escalation_level
FROM pagerduty.oncalls o
JOIN pagerduty.users u ON o.user_id = u.id
ORDER BY o.escalation_level ASC;
```

## Schema

### `incidents`
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR | Unique incident ID |
| `incident_number` | INT | Sequential incident number |
| `title` | VARCHAR | Incident title |
| `status` | VARCHAR | `triggered`, `acknowledged`, or `resolved` |
| `urgency` | VARCHAR | `high` or `low` |
| `created_at` | TIMESTAMP | When the incident was created |
| `html_url` | VARCHAR | Link to the incident in PagerDuty |
| `service_id` | VARCHAR | ID of the affected service |
| `service_name` | VARCHAR | Name of the affected service |
| `assignee_id` | VARCHAR | ID of the primary assignee |
| `assignee_name` | VARCHAR | Name of the primary assignee |
| `escalation_policy_id` | VARCHAR | Escalation policy ID |
| `escalation_policy_name` | VARCHAR | Escalation policy name |
| `team_ids` | VARCHAR | Comma-separated team IDs |
| `team_names` | VARCHAR | Comma-separated team names |

### `services`
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR | Unique service ID |
| `name` | VARCHAR | Service name |
| `description` | VARCHAR | Service description |
| `status` | VARCHAR | `active`, `warning`, `critical`, or `disabled` |
| `created_at` | TIMESTAMP | When the service was created |
| `html_url` | VARCHAR | Link to the service in PagerDuty |
| `escalation_policy_id` | VARCHAR | Escalation policy ID |
| `escalation_policy_name` | VARCHAR | Escalation policy name |

### `users`
| Column | Type | Description |
|--------|------|-------------|
| `id` | VARCHAR | Unique user ID |
| `name` | VARCHAR | Full name |
| `email` | VARCHAR | Email address |
| `role` | VARCHAR | `owner`, `admin`, `user`, `limited_user`, `observer`, `restricted_access` |
| `time_zone` | VARCHAR | User's configured time zone |
| `avatar_url` | VARCHAR | URL to user avatar |

### `oncalls`
| Column | Type | Description |
|--------|------|-------------|
| `user_id` | VARCHAR | On-call user ID |
| `user_name` | VARCHAR | On-call user name |
| `schedule_id` | VARCHAR | Schedule ID (nullable if policy has no schedule) |
| `schedule_name` | VARCHAR | Schedule name |
| `escalation_policy_id` | VARCHAR | Escalation policy ID |
| `escalation_policy_name` | VARCHAR | Escalation policy name |
| `escalation_level` | INT | Level in the escalation policy (1 = primary) |
| `start` | TIMESTAMP | Start of on-call window |
| `end` | TIMESTAMP | End of on-call window |

## Building from Source

The connector compiles against Dremio's internal libraries and must be built inside a running Dremio Docker container.

```bash
# Step 1 — install Dremio internal deps into container's Maven cache
./install_deps_in_container.sh

# Step 2 — build
docker exec dremio-test bash -c "
  cd /tmp/pagerduty-rebuild && mvn package -DskipTests --batch-mode
"

# Step 3 — deploy
docker cp dremio-test:/tmp/pagerduty-rebuild/jars/dremio-pagerduty-connector-1.0.0.jar jars/
docker exec -u root dremio-test cp jars/dremio-pagerduty-connector-1.0.0.jar /opt/dremio/jars/3rdparty/
docker restart dremio-test
```
