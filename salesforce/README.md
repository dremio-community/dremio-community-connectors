# Dremio Salesforce (REST) Connector

*Built by Mark Shainman*

A native Dremio storage plugin that adds **read support for Salesforce SObjects** via the Salesforce REST API and SOQL. No JDBC driver required — pure Java 11 `java.net.http.HttpClient` with OAuth2 password-grant authentication.

Dremio 26.x has no built-in community Salesforce connector. This plugin bridges that gap by implementing Dremio's `ConnectionConf` + `StoragePlugin` interfaces and querying Salesforce directly through the REST API — exposing every accessible SObject as a SQL table in the Dremio catalog.

---

## Features

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ | Full SOQL queries via Salesforce REST API |
| **Auto-discovery** | ✅ | All queryable SObjects appear in the catalog browser |
| **Schema inference** | ✅ | Salesforce field types mapped to Arrow/Dremio types automatically |
| **Projection pushdown** | ✅ | Only requested columns included in SOQL SELECT clause |
| **WHERE clause pushdown** | ✅ | Filter predicates pushed into SOQL WHERE clause |
| **Pagination (queryMore)** | ✅ | Transparent multi-page fetching via Salesforce cursor |
| **Parallel splits** | ✅ | Configurable LIMIT+OFFSET splits for parallel reads |
| **OAuth2 authentication** | ✅ | Password-grant flow with Connected App credentials |
| **Token refresh** | ✅ | Auto-re-authenticates on 401 responses |
| **Sandbox support** | ✅ | Change Login URL to `https://test.salesforce.com` |
| **Object exclusion** | ✅ | Hide noisy or large SObjects from the catalog |
| **Cross-source JOIN** | ✅ | Join Salesforce tables with S3, Iceberg, Cassandra, etc. |
| **CTAS / INSERT INTO** | ❌ | Read-only; write support not planned |

---

## Quick Install

```bash
# Docker — use the included pre-built JAR (no Maven needed)
./install.sh --docker try-dremio --prebuilt

# Bare-metal Dremio
./install.sh --local /opt/dremio --prebuilt

# Kubernetes pod
./install.sh --k8s dremio-0 --prebuilt

# Interactive (prompts for all options)
./install.sh
```

The installer copies the JAR to `jars/3rdparty/` and restarts Dremio. After restart, **Salesforce (REST)** will appear under **Sources → +**.

---

## Adding a Source

### Via Dremio UI

1. Go to **Sources → +** → select **Salesforce (REST)**
2. Fill in Authentication fields (see [Configuration Reference](#configuration-reference))
3. Click **Save**

### Via Command Line

```bash
./add-salesforce-source.sh \
  --name salesforce \
  --sf-user you@example.com \
  --sf-pass yourpassword \
  --sf-token yourSecurityToken \
  --client-id 3MVG9WVXk15qiz1... \
  --client-secret C3CE4B43F03F4602...
```

Sandbox:
```bash
./add-salesforce-source.sh \
  --name sf_sandbox \
  --sf-user you@example.com.sandbox \
  --sf-pass yourpassword \
  --sf-token yourSecurityToken \
  --client-id 3MVG... --client-secret ABC123 \
  --login-url https://test.salesforce.com
```

Non-interactive (CI/CD):
```bash
./add-salesforce-source.sh \
  --name salesforce \
  --sf-user "$SF_USER" --sf-pass "$SF_PASS" --sf-token "$SF_TOKEN" \
  --client-id "$CLIENT_ID" --client-secret "$CLIENT_SECRET" \
  --user dremio_admin --password "$DREMIO_PASS"
```

---

## Setting Up a Salesforce Connected App

The connector uses the OAuth2 password-grant flow, which requires a Salesforce Connected App:

1. In Salesforce **Setup**, navigate to the classic Connected App creation URL:
   ```
   https://YOUR-ORG.my.salesforce.com/app/mgmt/forceconnectedapps/forceAppEdit.apexp
   ```
2. Fill in **Connected App Name**, **Contact Email**, enable **OAuth Settings**
3. Set **Callback URL** to `https://login.salesforce.com/services/oauth2/callback`
4. Add scopes: **Full access (full)** and **Perform requests at any time (refresh_token)**
5. Save → copy the **Consumer Key** and **Consumer Secret**
6. Edit Policies → set **IP Relaxation** to `Relax IP restrictions`
7. In **Setup → OAuth and OpenID Connect Settings**, enable **Allow OAuth Username-Password Flows**

---

## Upgrading Dremio

When you upgrade Dremio, recompile the connector against the new JARs with the one-click rebuild tool:

```bash
# Detect version + rebuild + redeploy — Docker
./rebuild.sh --docker try-dremio

# Force rebuild even if version matches
./rebuild.sh --docker try-dremio --force

# Bare-metal
./rebuild.sh --local /opt/dremio

# Preview changes only
./rebuild.sh --dry-run
```

---

## SQL Usage

```sql
-- List all Accounts
SELECT Id, Name, Industry, AnnualRevenue
FROM salesforce.Account
LIMIT 100;

-- Filter with WHERE pushdown (translated to SOQL)
SELECT Id, Name, Amount, StageName, CloseDate
FROM salesforce.Opportunity
WHERE StageName = 'Closed Won'
  AND CloseDate >= DATE '2026-01-01';

-- Contacts with account name
SELECT Id, FirstName, LastName, Email, Account.Name
FROM salesforce.Contact
WHERE Email IS NOT NULL
LIMIT 500;

-- Aggregation in Dremio after SOQL fetch
SELECT Industry, COUNT(*) as cnt, AVG(AnnualRevenue) as avg_rev
FROM salesforce.Account
WHERE Industry IS NOT NULL
GROUP BY Industry
ORDER BY cnt DESC;

-- Cross-source JOIN: Salesforce + Iceberg
SELECT s.Id, s.Name, s.Amount, i.segment
FROM salesforce.Opportunity s
JOIN iceberg_catalog.crm.customer_segments i
  ON s.AccountId = i.sf_account_id
WHERE s.StageName = 'Closed Won';

-- Cross-source JOIN: Salesforce + DynamoDB
SELECT c.Id, c.Email, d.loyalty_tier
FROM salesforce.Contact c
JOIN dynamodb_source.loyalty_table d
  ON c.Email = d.email;
```

---

## Architecture

```
SQL Query
  └── ScanCrel  (Dremio catalog scan)
        └── SalesforceScanRule        [LOGICAL phase]
              └── SalesforceScanDrel
                    ├── SalesforceFilterRule  [LOGICAL — extracts WHERE pushdown]
                    └── SalesforceScanPrule  [PHYSICAL phase]
                          └── SalesforceScanPrel
                                └── getPhysicalOperator()
                                      └── SalesforceGroupScan
                                            └── getSpecificScan()
                                                  └── SalesforceSubScan
                                                        └── SalesforceScanCreator
                                                              └── SalesforceRecordReader
                                                                    └── SOQL → REST API → Arrow batches
```

### Key Classes

| Class | Role |
|---|---|
| `SalesforceConf` | Source config shown in "Add Source" UI. All 11 configuration fields. |
| `SalesforceStoragePlugin` | Plugin lifecycle, SObject listing, schema inference, split generation. |
| `SalesforceConnection` | OAuth2 authentication, REST API calls, pagination via `queryMore`. |
| `SalesforceScanSpec` | Carries SObject name, SOQL query, and split info through planning. |
| `SalesforceFilterRule` | Planner rule: extracts WHERE predicates and appends to SOQL. |
| `SalesforceGroupScan` | Planning-layer scan; distributes LIMIT+OFFSET splits. |
| `SalesforceSubScan` | JSON-serializable executor work unit for one or more splits. |
| `SalesforceRecordReader` | Executes SOQL, handles pagination, writes Arrow vector batches. |
| `SalesforceTypeConverter` | Maps Salesforce field types to Arrow/Dremio schema types. |
| `SalesforceRulesFactory` | Registers LOGICAL and PHYSICAL planner rule sets. |
| `SalesforceScanRule` | Planner rule: `ScanCrel` → `SalesforceScanDrel` (LOGICAL phase). |
| `SalesforceScanPrule` | Planner rule: `SalesforceScanDrel` → `SalesforceScanPrel` (PHYSICAL phase). |
| `SalesforceScanCreator` | Executor factory: maps `SalesforceSubScan` → `SalesforceRecordReader`. |

---

## Salesforce Type Mapping

| Salesforce Type | Arrow / Dremio Type | Notes |
|---|---|---|
| `string`, `textarea`, `phone`, `url`, `email`, `picklist` | UTF8 | |
| `id`, `reference` | UTF8 | 15/18-char Salesforce IDs |
| `boolean` | BIT | |
| `int` | INT32 | |
| `double`, `currency`, `percent` | FLOAT64 | |
| `date` | DATE_DAY | ISO-8601 date string from API |
| `datetime` | TIMESTAMP_MILLI (UTC) | ISO-8601 datetime; `+0000` → `Z` normalized |
| `time` | TIME_MILLI | `HH:mm:ss.SSS` format |
| `base64` | VARBINARY | Base64-decoded bytes |
| `address`, `location` | UTF8 | JSON-serialized compound field |
| `multipicklist` | UTF8 | Semicolon-separated values |
| Unknown / compound | UTF8 | Fallback serialization |

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| `loginUrl` | `https://login.salesforce.com` | OAuth2 token endpoint base URL. Use `https://test.salesforce.com` for sandboxes. |
| `username` | _(required)_ | Salesforce username (email address) |
| `password` | _(required)_ | Salesforce password (stored encrypted by Dremio) |
| `securityToken` | _(empty)_ | Security token appended to password in OAuth call. Leave blank if your org IP is trusted. |
| `clientId` | _(required)_ | Connected App Consumer Key |
| `clientSecret` | _(required)_ | Connected App Consumer Secret (stored encrypted) |
| `apiVersion` | `59.0` | Salesforce REST API version |
| `recordsPerPage` | `2000` | Records fetched per REST API call (Salesforce max: 2000) |
| `splitParallelism` | `4` | Number of LIMIT+OFFSET splits for parallel reading |
| `queryTimeoutSeconds` | `120` | HTTP connection timeout for Salesforce API calls |
| `excludedObjects` | _(empty)_ | Comma-separated SObject API names to hide from the catalog (e.g. `ContentVersion,FeedItem`) |

---

## Design Notes

**Why REST API and not the Bulk API?**
The Salesforce REST API (SOQL query endpoint) is the most broadly supported and accessible interface. It works with any Connected App and all Salesforce editions. The Bulk API 2.0 offers higher throughput for large datasets but requires additional permissions and a different auth flow. The REST connector covers the primary use case — interactive SQL analytics — and pagination via `queryMore` handles datasets of any size.

**Why OAuth2 password grant?**
The password grant flow (`grant_type=password`) requires only a username, password, security token, and Connected App credentials — no browser redirect needed. This makes it suitable for server-to-server integration (like Dremio running in Docker or Kubernetes). For orgs where the password grant is disabled, use the Login URL override to point to a sandbox where it is enabled.

**How WHERE pushdown works**
`SalesforceFilterRule` runs at logical planning time and extracts simple predicates from `FilterRel` nodes. It appends a `WHERE` clause to the SOQL query stored in `SalesforceScanSpec`. All other predicates (unsupported functions, complex expressions) are left as Dremio residual filters — results are always correct.

**How splits work**
`listPartitionChunks()` estimates the row count via `SELECT COUNT() FROM Object` and generates `splitParallelism` splits, each encoded as `LIMIT N OFFSET M` in the SOQL. This allows multiple Dremio executor fragments to read different slices of the SObject in parallel. On single-node clusters all splits are assigned to one executor.

---

## Requirements

- **Dremio OSS** 26.x (tested on 26.0.5)
- **Salesforce** Professional, Enterprise, Unlimited, or Developer Edition (API access required)
- **Java** 11+ (provided by the Dremio container)
- **Maven** 3.8+ (only required if building from source)

---

## References

- [Salesforce REST API Developer Guide](https://developer.salesforce.com/docs/atlas.en-us.api_rest.meta/api_rest/)
- [SOQL and SOSL Reference](https://developer.salesforce.com/docs/atlas.en-us.soql_sosl.meta/soql_sosl/)
- [Salesforce Connected Apps](https://help.salesforce.com/s/articleView?id=sf.connected_app_overview.htm)
- [Dremio StoragePlugin Interface](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/StoragePlugin.java)
- [Dremio ConnectionConf Base Class](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/catalog/conf/ConnectionConf.java)
