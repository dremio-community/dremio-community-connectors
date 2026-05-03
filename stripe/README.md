# Dremio Stripe Connector

*Built by Mark Shainman*

A native Dremio storage plugin that adds **read support for Stripe billing data** via the Stripe REST API v1. No JDBC driver required — pure Java 11 `java.net.http.HttpClient` with Bearer token authentication.

Dremio 26.x has no built-in Stripe connector. This plugin bridges that gap by implementing Dremio's `ConnectionConf` + `StoragePlugin` interfaces and querying Stripe directly through the REST API — exposing charges, customers, subscriptions, invoices, and more as SQL tables in the Dremio catalog.

---

## Features

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ | Full table scans via Stripe REST API |
| **9 pre-defined tables** | ✅ | charges, customers, subscriptions, invoices, payment_intents, products, prices, refunds, balance_transactions |
| **Cursor-based pagination** | ✅ | Transparent multi-page fetching via `starting_after` cursor |
| **API key authentication** | ✅ | Supports `sk_live_...` and `sk_test_...` keys |
| **Rate limit handling** | ✅ | Auto-retries on HTTP 429 with 1s back-off |
| **Cross-source JOIN** | ✅ | Join Stripe tables with S3, Iceberg, DynamoDB, etc. |
| **Nested field extraction** | ✅ | Dot-path mapping (e.g. `prices.recurring_interval` → `recurring.interval`) |
| **Column projection** | ✅ | Only declared columns are extracted from the JSON response |
| **stripe-mock support** | ✅ | Works with `stripe/stripe-mock` Docker image for testing |
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

The installer copies the JAR to `jars/3rdparty/` and restarts Dremio. After restart, **Stripe** will appear under **Sources → +**.

---

## Adding a Source

### Via Dremio UI

1. Go to **Sources → +** → select **Stripe**
2. Enter your **Secret API Key** (`sk_live_...` or `sk_test_...`)
3. Optionally adjust **Page Size** (default: 100, max: 100) and **Query Timeout** (default: 60s)
4. Click **Save**

### Via Command Line

```bash
./add-stripe-source.sh --name stripe --api-key sk_live_...
```

Test/sandbox key:
```bash
./add-stripe-source.sh --name stripe_test --api-key sk_test_...
```

Non-interactive (CI/CD):
```bash
./add-stripe-source.sh \
  --name stripe \
  --api-key "$STRIPE_SECRET_KEY" \
  --user dremio_admin \
  --password "$DREMIO_PASS"
```

### Finding Your API Key

1. Log into the [Stripe Dashboard](https://dashboard.stripe.com)
2. Navigate to **Developers → API keys**
3. Copy the **Secret key** (`sk_live_...` for production, `sk_test_...` for test mode)

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
-- All charges in the last 30 days (filter in Dremio after full scan)
SELECT id, amount, currency, status, created
FROM stripe.charges
WHERE status = 'succeeded'
LIMIT 100;

-- Revenue by currency
SELECT currency, SUM(amount) AS total_cents, COUNT(*) AS num_charges
FROM stripe.charges
WHERE status = 'succeeded'
GROUP BY currency
ORDER BY total_cents DESC;

-- Active subscriptions
SELECT id, customer, status, created
FROM stripe.subscriptions
WHERE status = 'active';

-- Products and their prices
SELECT p.name AS product_name, pr.currency, pr.unit_amount, pr.type
FROM stripe.products p
JOIN stripe.prices pr ON p.id = pr.product
WHERE p.active = true;

-- Outstanding invoices
SELECT id, currency, amount_due, created
FROM stripe.invoices
WHERE paid = false
ORDER BY amount_due DESC
LIMIT 20;

-- Balance reconciliation: verify net = amount - fee
SELECT id, amount, fee, net,
       amount - fee AS expected_net
FROM stripe.balance_transactions
WHERE type = 'charge';

-- Cross-source JOIN: Stripe + Iceberg customer enrichment
SELECT c.id, c.currency, i.company_name, i.segment
FROM stripe.customers c
JOIN iceberg_catalog.crm.accounts i ON c.id = i.stripe_customer_id;

-- Cross-source JOIN: Stripe charges + DynamoDB order lookup
SELECT ch.id, ch.amount, ch.currency, o.order_ref
FROM stripe.charges ch
JOIN dynamodb_source.orders o ON ch.id = o.stripe_charge_id
WHERE ch.status = 'succeeded';
```

---

## Tables and Schema

All amounts in Stripe are **integers in cents** (e.g. `amount = 1000` → $10.00 USD).

### `charges`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `ch_...` |
| amount | BIGINT | In cents |
| amount_captured | BIGINT | In cents |
| amount_refunded | BIGINT | In cents |
| currency | VARCHAR | ISO 4217 lowercase |
| status | VARCHAR | `succeeded`, `pending`, `failed` |
| paid | BOOLEAN | |
| refunded | BOOLEAN | |
| captured | BOOLEAN | |
| description | VARCHAR | |
| receipt_email | VARCHAR | |
| payment_method_type | VARCHAR | From `payment_method_details.type` |
| customer | VARCHAR | Customer id or null |
| created | BIGINT | Unix timestamp |

### `customers`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `cus_...` |
| email | VARCHAR | |
| name | VARCHAR | |
| phone | VARCHAR | |
| currency | VARCHAR | Default currency |
| balance | BIGINT | In cents |
| delinquent | BOOLEAN | |
| description | VARCHAR | |
| created | BIGINT | Unix timestamp |

### `subscriptions`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `sub_...` |
| customer | VARCHAR | Customer id |
| status | VARCHAR | `active`, `canceled`, `past_due`, etc. |
| cancel_at_period_end | BOOLEAN | |
| current_period_start | BIGINT | Unix timestamp |
| current_period_end | BIGINT | Unix timestamp |
| created | BIGINT | Unix timestamp |

### `invoices`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `in_...` |
| customer | VARCHAR | Customer id |
| subscription | VARCHAR | Subscription id or null |
| currency | VARCHAR | |
| amount_due | BIGINT | In cents |
| amount_paid | BIGINT | In cents |
| amount_remaining | BIGINT | In cents |
| paid | BOOLEAN | |
| status | VARCHAR | `draft`, `open`, `paid`, `uncollectible`, `void` |
| created | BIGINT | Unix timestamp |

### `payment_intents`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `pi_...` |
| amount | BIGINT | In cents |
| amount_received | BIGINT | In cents |
| currency | VARCHAR | |
| status | VARCHAR | `succeeded`, `requires_payment_method`, etc. |
| customer | VARCHAR | Customer id or null |
| description | VARCHAR | |
| created | BIGINT | Unix timestamp |

### `products`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `prod_...` |
| name | VARCHAR | |
| active | BOOLEAN | |
| description | VARCHAR | |
| created | BIGINT | Unix timestamp |

### `prices`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `price_...` |
| product | VARCHAR | Product id |
| currency | VARCHAR | |
| unit_amount | BIGINT | In cents |
| type | VARCHAR | `one_time` or `recurring` |
| active | BOOLEAN | |
| recurring_interval | VARCHAR | `day`, `week`, `month`, `year` (recurring only) |
| recurring_interval_count | BIGINT | (recurring only) |
| created | BIGINT | Unix timestamp |

### `refunds`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `re_...` |
| charge | VARCHAR | Charge id |
| amount | BIGINT | In cents |
| currency | VARCHAR | |
| status | VARCHAR | `succeeded`, `pending`, `failed` |
| reason | VARCHAR | `duplicate`, `fraudulent`, `requested_by_customer` |
| created | BIGINT | Unix timestamp |

### `balance_transactions`
| Column | Type | Notes |
|--------|------|-------|
| id | VARCHAR | `txn_...` |
| amount | BIGINT | In cents |
| currency | VARCHAR | |
| fee | BIGINT | In cents |
| net | BIGINT | `amount - fee` in cents |
| type | VARCHAR | `charge`, `refund`, `payout`, etc. |
| source | VARCHAR | Source object id |
| description | VARCHAR | |
| created | BIGINT | Unix timestamp |

---

## Architecture

```
SQL Query
  └── ScanCrel  (Dremio catalog scan)
        └── StripeScanRule            [LOGICAL phase]
              └── StripeScanDrel
                    └── StripeScanPrule  [PHYSICAL phase]
                          └── StripeScanPrel
                                └── getPhysicalOperator()
                                      └── StripeGroupScan
                                            └── getSpecificScan()
                                                  └── StripeSubScan
                                                        └── StripeScanCreator
                                                              └── StripeRecordReader
                                                                    └── REST API → cursor pagination → Arrow batches
```

### Key Classes

| Class | Role |
|---|---|
| `StripeConf` | Source config: API key, base URL, page size, timeout. Shown in "Add Source" UI. |
| `StripeStoragePlugin` | Plugin lifecycle, table listing, schema metadata, split generation. |
| `StripeConnection` | HTTP client, Bearer auth, cursor-based pagination, rate limit retry. |
| `StripeTypeConverter` | Per-table column definitions; dot-path nested field extraction. |
| `StripeSubScan` | JSON-serializable executor work unit. |
| `StripeRecordReader` | Fetches pages, writes Arrow vector batches (BigInt, Boolean, Utf8). |
| `StripeScanCreator` | Executor factory: maps `StripeSubScan` → `StripeRecordReader`. |
| `StripeRulesFactory` | Registers planner rule sets with Dremio's optimizer. |

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| `apiKey` | _(required)_ | Stripe secret API key (`sk_live_...` or `sk_test_...`). Stored encrypted by Dremio. |
| `baseUrl` | `https://api.stripe.com` | Override for testing (e.g. `http://stripe-mock:12111` with stripe-mock). |
| `pageSize` | `100` | Records fetched per Stripe API call (max: 100 per Stripe's limit). |
| `queryTimeoutSeconds` | `60` | HTTP connection and request timeout. |

---

## Testing with stripe-mock

The connector works with [stripe/stripe-mock](https://github.com/stripe/stripe-mock), which implements the full Stripe API locally using OpenAPI fixture data.

```bash
# Start stripe-mock on the same Docker network as Dremio
docker run -d --name stripe-mock --network dremio-net \
  -p 12111:12111 stripe/stripe-mock:latest

# Add a test source pointing at stripe-mock
./add-stripe-source.sh \
  --name stripe_test \
  --api-key sk_test_mock \
  --base-url http://stripe-mock:12111
```

Then run the smoke tests:
```bash
cd /path/to/dremio-connector-tests
python3 -m pytest connectors/test_stripe.py -v
```

stripe-mock returns exactly 1 fixture record per table with consistent values (`currency=usd`, `status=succeeded`, etc.), making assertions deterministic.

> **Note:** Before running tests, ensure `exec.queue.enable = false` is set in Dremio (required for cross-table JOINs):
> ```sql
> ALTER SYSTEM SET "exec.queue.enable" = false
> ```

---

## Design Notes

**Why a fixed 9-table schema instead of dynamic discovery?**
Stripe's API does not expose a metadata endpoint that lists tables and their fields. The 9 tables here cover the core billing primitives that the vast majority of Stripe users need for analytics. New tables can be added by updating `StripeConnection.TABLES` and `StripeTypeConverter.columnsFor()`.

**Why integers for monetary amounts?**
Stripe stores all amounts as integers in the smallest currency unit (cents for USD). Using `BIGINT` preserves exact values without floating-point rounding. In SQL, divide by 100 to convert to dollars: `amount / 100.0`.

**How pagination works**
Stripe's list endpoints return a page of up to 100 objects and a `has_more` boolean. The cursor for the next page is the `id` of the last item in the current page, passed as `starting_after`. `StripeRecordReader.next()` fetches pages until `has_more = false`.

**Rate limiting**
On HTTP 429 (Too Many Requests), the connector sleeps 1 second and retries once. If the retry also returns 429, the error is propagated. For production workloads with large datasets, consider lowering `pageSize` or using the configurable `queryTimeoutSeconds`.

---

## Requirements

- **Dremio OSS** 26.x (tested on 26.0.5)
- **Stripe** account with a secret API key (any plan)
- **Java** 11+ (provided by the Dremio container)
- **Maven** 3.8+ (only required if building from source)

---

## References

- [Stripe API Reference](https://docs.stripe.com/api)
- [Stripe API Keys](https://docs.stripe.com/keys)
- [stripe-mock (test server)](https://github.com/stripe/stripe-mock)
- [Dremio StoragePlugin Interface](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/store/StoragePlugin.java)
- [Dremio ConnectionConf Base Class](https://github.com/dremio/dremio-oss/blob/master/sabot/kernel/src/main/java/com/dremio/exec/catalog/conf/ConnectionConf.java)
