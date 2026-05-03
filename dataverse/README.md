# Dremio Microsoft Dataverse Connector

[![License](https://img.shields.io/badge/license-Apache--2.0-blue?style=for-the-badge)](LICENSE)

Query Microsoft Dataverse (Dynamics 365) tables directly from Dremio using SQL. No JDBC driver — uses Azure AD OAuth2 client credentials + OData v4 REST API.

## Features

- **Azure AD OAuth2** client credentials flow (app registration — no user password needed)
- **Schema discovery** — automatically lists all queryable Dataverse entities with their field types
- **OData $filter pushdown** — WHERE clauses translate to OData filter expressions, reducing data transfer
- **Pagination** — follows `@odata.nextLink` transparently for large tables
- **Full type mapping** — String, Integer, BigInt, Decimal, Money, Boolean, DateTime, Lookup, Picklist, and more
- Works with **Dynamics 365 Sales, Service, Marketing, Finance**, and any Dataverse environment

## Prerequisites

### 1 — Create an Azure AD App Registration

1. Go to [Azure Portal](https://portal.azure.com) → **Azure Active Directory** → **App registrations** → **New registration**
2. Give it a name (e.g. `Dremio Dataverse Connector`), click **Register**
3. Note the **Application (client) ID** and **Directory (tenant) ID**
4. Go to **Certificates & secrets** → **New client secret** → copy the **Value**
5. Go to **API permissions** → **Add a permission** → **Dynamics CRM** → **Delegated** → `user_impersonation`
   - Or use **Application permissions** if available for your environment
6. Click **Grant admin consent**

### 2 — Add the App User to Dataverse

> **Important:** This step requires **System Administrator** security role in your Dataverse environment. Have your Dynamics 365 admin perform this step if you don't have that role.

In your Power Apps environment:
1. Go to [Power Platform Admin Center](https://admin.powerplatform.microsoft.com) → **Environments** → select your environment → **Settings** → **Users + permissions** → **Application users**
2. Click **+ New app user** → **+ Add an app** → search for your app registration name
3. Set **Business unit** to your org
4. Under **Security roles**, click the pencil → select **System Administrator** (or a custom read-only role)
5. Click **Create**

Alternatively, via classic Dynamics 365 UI:
1. Go to `https://yourorg.crm.dynamics.com/main.aspx?settingsonly=true`
2. **Settings** → **Security** → **Users** → switch view to **"Application Users"** → **New**
3. Enter the **Application ID** from your App Registration and save
4. Click **Manage Roles** → assign **System Administrator**

## Installation

```bash
# Copy the JAR to Dremio's 3rd-party plugins directory
cp jars/dremio-dataverse-connector-1.0.0.jar $DREMIO_HOME/jars/3rdparty/

# Restart Dremio
$DREMIO_HOME/bin/dremio restart
```

Or use the install script:
```bash
chmod +x install.sh
DREMIO_HOME=/opt/dremio ./install.sh
```

## Configuration

In the Dremio UI, add a new source of type **Microsoft Dataverse** and fill in:

| Field | Description | Example |
|---|---|---|
| Organization URL | Your Dataverse environment URL | `https://myorg.api.crm.dynamics.com` |
| Tenant ID | Azure AD Directory (Tenant) ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| Client ID | App Registration Application (Client) ID | `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` |
| Client Secret | App Registration client secret value | `your-secret-value` |
| API Version | Dataverse Web API version | `9.2` |
| Records Per Page | Max records per OData request (max 5000) | `5000` |
| Excluded Entities | Comma-separated entities to hide | `activitypointer,asyncoperation` |

## Usage

Once configured, Dataverse entities appear as tables:

```sql
-- List all accounts
SELECT accountid, name, telephone1, createdon
FROM dataverse.account
LIMIT 100;

-- Filter with pushdown to OData $filter
SELECT name, revenue, industrycode
FROM dataverse.account
WHERE statecode = 0
  AND revenue > 1000000;

-- Join Accounts with Contacts
SELECT a.name AS company, c.fullname AS contact, c.emailaddress1
FROM dataverse.account a
JOIN dataverse.contact c ON a.accountid = c.parentcustomerid
WHERE a.statecode = 0;
```

## Building from Source

```bash
mvn clean package
cp target/dremio-dataverse-connector-1.0.0.jar jars/
```

Requires Java 11+ and Maven 3.6+.

## Common Dataverse Entities

| Entity | Description |
|---|---|
| `account` | Companies / organizations |
| `contact` | People associated with accounts |
| `lead` | Prospective customers |
| `opportunity` | Sales deals |
| `incident` | Support cases (Service) |
| `task`, `phonecall`, `email` | Activity records |
| `systemuser` | CRM users |
| `team` | Teams and business units |

## Type Mapping

| Dataverse Type | Arrow / SQL Type |
|---|---|
| String, Memo | VARCHAR |
| Integer, Picklist, State, Status | INT |
| BigInt | BIGINT |
| Double, Money, Decimal | DOUBLE |
| Boolean | BIT |
| DateTime | TIMESTAMP |
| Lookup, UniqueIdentifier | VARCHAR (GUID) |

## License

Apache 2.0
