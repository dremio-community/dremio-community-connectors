# Dremio HubSpot Connector

Query HubSpot CRM data directly in Dremio SQL — contacts, companies, deals, tickets, engagements, and more. Uses HubSpot Private App token authentication with no OAuth setup required.

## Tables

| Table | Description |
|-------|-------------|
| `contacts` | People in your CRM — name, email, phone, lifecycle stage, owner |
| `companies` | Company records — name, domain, industry, revenue, HubSpot score |
| `deals` | Sales pipeline — deal name, stage, amount, close date, pipeline |
| `tickets` | Support tickets — subject, status, priority, category |
| `products` | Product catalog — name, description, price, SKU |
| `line_items` | Deal line items — product, quantity, unit price, discount |
| `calls` | Logged calls — duration, disposition, recording URL |
| `emails` | Email engagements — subject, direction, status |
| `meetings` | Meeting records — title, start/end time, outcome |
| `notes` | CRM notes — body, associations |
| `tasks` | Tasks — subject, status, priority, due date |
| `owners` | HubSpot users/owners — name, email, userId |

All schemas are derived dynamically from the HubSpot Properties API at metadata refresh time.

## Authentication

Create a **Private App** in HubSpot (Settings → Integrations → Private Apps) with the following scopes:

| Scope | Required for |
|-------|-------------|
| `crm.objects.contacts.read` | contacts |
| `crm.objects.companies.read` | companies |
| `crm.objects.deals.read` | deals |
| `crm.objects.tickets.read` | tickets |
| `crm.objects.owners.read` | owners |
| `crm.objects.products.read` | products, line_items |
| `crm.objects.line_items.read` | line_items |
| `crm.objects.calls.read` | calls |
| `crm.objects.emails.read` | emails |
| `crm.objects.meetings.read` | meetings |
| `crm.objects.notes.read` | notes |
| `crm.objects.tasks.read` | tasks |

## Installation

**First-time install (prebuilt JAR — no Maven required):**

```bash
# Docker
./install.sh --docker try-dremio --prebuilt

# Bare-metal
./install.sh --local /opt/dremio --prebuilt

# Kubernetes
./install.sh --k8s dremio-0 --prebuilt
```

**After a Dremio upgrade (auto-detects version, rebuilds from source):**

```bash
./rebuild.sh --docker try-dremio
./rebuild.sh --local /opt/dremio
```

**Add the source to Dremio:**

```bash
# Interactive (prompts for token and password)
./add-hubspot-source.sh --name hubspot

# Non-interactive
./add-hubspot-source.sh --name hubspot \
  --token pat-na1-xxxxxxxxxxxx \
  --user dremio --password dremio_pass
```

## Usage

```sql
-- Basic CRM queries
SELECT id, firstname, lastname, email, jobtitle, lifecyclestage
FROM hubspot.contacts
LIMIT 100;

SELECT id, name, domain, industry, annualrevenue
FROM hubspot.companies
WHERE annualrevenue > '1000000'
LIMIT 50;

-- Sales pipeline
SELECT id, dealname, dealstage, amount, closedate
FROM hubspot.deals
WHERE dealstage = 'closedwon'
ORDER BY closedate DESC;

-- Join contacts with owners (who owns the most contacts?)
SELECT o.firstname, o.lastname, COUNT(c.id) AS contact_count
FROM hubspot.contacts c
JOIN hubspot.owners o ON c.hubspot_owner_id = o.id
GROUP BY o.firstname, o.lastname
ORDER BY contact_count DESC;

-- Support ticket volume by status
SELECT hs_pipeline_stage, COUNT(*) AS ticket_count
FROM hubspot.tickets
GROUP BY hs_pipeline_stage;

-- Revenue by deal stage
SELECT dealstage,
       COUNT(*) AS deal_count,
       SUM(CAST(amount AS DOUBLE)) AS total_value
FROM hubspot.deals
WHERE amount IS NOT NULL
GROUP BY dealstage;

-- Recent calls
SELECT id, hs_call_title, hs_call_duration, hs_call_disposition, hs_timestamp
FROM hubspot.calls
ORDER BY hs_timestamp DESC
LIMIT 20;

-- All owners
SELECT id, firstname, lastname, email
FROM hubspot.owners;
```

## Schema Details

**Built-in fields** (present on all tables except `owners`):

| Field | Type | Description |
|-------|------|-------------|
| `id` | VARCHAR | HubSpot record ID |
| `createdAt` | TIMESTAMP | Record creation time |
| `updatedAt` | TIMESTAMP | Last modified time |
| `archived` | BOOL | Whether the record is archived |

**Dynamic property fields** are added automatically based on what's configured in your HubSpot portal. Use `SELECT * FROM hubspot.contacts LIMIT 1` to see all available columns.

## Notes

**API limits:** HubSpot allows 100 records per page and 10 requests/second (burst) for Private Apps. The connector fetches pages sequentially with cursor-based pagination.

**Data types:** HubSpot returns all property values as strings internally. The connector maps them to Arrow types based on the property definition (`number` → DOUBLE, `bool` → BOOL, `date` → DATE, `datetime` → TIMESTAMP). If a conversion fails, the value is returned as VARCHAR.

**Archived records:** By default, archived (soft-deleted) records are excluded. Enable with `--include-archived` in `add-hubspot-source.sh` or toggle the option in the Dremio UI.

**Calculated properties:** Calculated/formula properties are included in the schema — they have values only when HubSpot has computed them.
