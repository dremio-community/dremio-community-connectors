# Dremio Jira Connector

A Dremio storage plugin that exposes [Jira Cloud](https://www.atlassian.com/software/jira) as SQL tables using Dremio's native connector framework. Authenticates via Atlassian API token (Basic auth).

## Tables

| Table | Description |
|-------|-------------|
| `issues` | All issues matching the configured JQL filter — summary, status, type, priority, assignee, dates, labels, components, story points, time tracking |
| `projects` | Project list — key, name, type, visibility |
| `users` | Atlassian user directory — account ID, display name, email, active status |
| `boards` | Scrum and Kanban boards — name, type, linked project |
| `priorities` | Issue priority levels |
| `issue_types` | Issue type definitions (Story, Bug, Task, Epic, etc.) |
| `statuses` | Workflow statuses with status category |
| `fields` | All field definitions including custom fields |
| `components` | Project components (aggregated across all projects) |
| `versions` | Fix versions / releases (aggregated across all projects) |

## Requirements

- Dremio 26.x
- Jira Cloud (REST API v3). Jira Server/Data Center is not supported.

## Install

```bash
./install.sh --docker try-dremio --prebuilt   # Docker, pre-built JAR
./install.sh --docker try-dremio --build      # Docker, build from source
./install.sh --local /opt/dremio --prebuilt   # bare-metal
./install.sh --k8s dremio-0 --prebuilt        # Kubernetes
```

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| Domain | _(required)_ | Your Atlassian subdomain (e.g. `mycompany` for mycompany.atlassian.net) |
| Email | _(required)_ | Atlassian account email |
| API Token | _(required)_ | Generate at id.atlassian.com → Security → API tokens |
| Issue JQL | `project IS NOT EMPTY ORDER BY created ASC` | JQL filter for the `issues` table |
| Page Size | `100` | Records per API call (max 100 for issues) |
| Timeout | `60` | HTTP timeout in seconds |

## Example Queries

```sql
-- Open bugs by priority
SELECT priority, COUNT(*) AS cnt
FROM jira_source.issues
WHERE issue_type = 'Bug' AND status != 'Done'
GROUP BY priority ORDER BY cnt DESC;

-- Sprint velocity: story points completed per project
SELECT project_key, COUNT(*) AS issues_done, SUM(story_points) AS points
FROM jira_source.issues
WHERE status = 'Done' AND story_points IS NOT NULL
GROUP BY project_key ORDER BY points DESC;

-- Issues assigned to each user
SELECT assignee_display_name, COUNT(*) AS open_issues
FROM jira_source.issues
WHERE status != 'Done'
GROUP BY assignee_display_name ORDER BY open_issues DESC;

-- Average resolution time in days by issue type
SELECT issue_type,
       COUNT(*) AS resolved,
       AVG(DATEDIFF(resolved, created)) AS avg_days_to_resolve
FROM jira_source.issues
WHERE resolved IS NOT NULL
GROUP BY issue_type ORDER BY avg_days_to_resolve;

-- Unresolved issues older than 30 days
SELECT key, summary, assignee_display_name, created, status
FROM jira_source.issues
WHERE resolved IS NULL AND status != 'Done'
ORDER BY created;

-- Cross-source: join Jira issues with PagerDuty incidents (if both configured)
SELECT j.key, j.summary, j.priority, p.incident_number, p.urgency
FROM jira_source.issues j
JOIN pagerduty_source.incidents p ON j.key = p.title;

-- Boards with their project
SELECT b.name AS board_name, b.type, b.project_key, p.name AS project_name
FROM jira_source.boards b
JOIN jira_source.projects p ON b.project_key = p.key;

-- Custom JQL: query only a specific project
-- Set Issue JQL Filter to: project = MYPROJ AND status != Done ORDER BY updated DESC
SELECT key, summary, assignee_display_name, updated
FROM jira_source.issues
LIMIT 50;
```

## Building from Source

```bash
./rebuild.sh --docker try-dremio        # Detect Dremio version, rebuild, redeploy
./rebuild.sh --docker try-dremio --dry-run   # Preview version detection only
./rebuild.sh --force                    # Force rebuild even if version matches
```
