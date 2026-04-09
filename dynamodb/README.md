# Dremio Amazon DynamoDB Connector

*Built by Mark Shainman*

A native Dremio storage plugin that adds full **read support for Amazon DynamoDB** tables,
including partition key Query mode, sort key range pushdown, FilterExpression pushdown,
projection pushdown, parallel segment scans, and native collection type support.

Dremio 26.x ships with no built-in DynamoDB connector. This plugin bridges that gap by
implementing Dremio's `ConnectionConf` + `StoragePlugin` interfaces and using the
**AWS SDK for Java v2** to access DynamoDB — no JDBC, no Spark, no ORM.

---

## Features

| Feature | Status | Notes |
|---|---|---|
| **SELECT (read)** | ✅ | Full table scans via DynamoDB Parallel Scan |
| **Auto-discovery** | ✅ | All DynamoDB tables appear in catalog browser |
| **Schema inference** | ✅ | Types inferred by sampling N items per table (configurable) |
| **Partition key Query mode** | ✅ | `WHERE pk = val` uses DynamoDB Query API (much cheaper than Scan) |
| **Sort key range pushdown** | ✅ | `WHERE pk = val AND sk > val` pushed as KeyConditionExpression |
| **FilterExpression pushdown** | ✅ | Non-key predicates pushed as DynamoDB FilterExpression |
| **Projection pushdown** | ✅ | Only requested columns included in DynamoDB request (saves bandwidth) |
| **LIMIT pushdown** | ✅ | `LIMIT N` reduces rows fetched from DynamoDB |
| **Parallel splits** | ✅ | DynamoDB Parallel Scan segments (configurable, default 4) |
| **String Set (SS)** | ✅ | SS → Arrow `ListVector<VarChar>` |
| **Number Set (NS)** | ✅ | NS → Arrow `ListVector<Float8>` |
| **Integer N detection** | ✅ | N columns with all-integer samples → Arrow `BigInt` |
| **BOOLEAN type** | ✅ | DynamoDB BOOL → Arrow Bit |
| **Nested types (L/M)** | ✅ | Lists and Maps serialized as JSON VarChar |
| **Schema cache** | ✅ | Configurable TTL to avoid re-sampling on every refresh |
| **pk/sk metadata propagation** | ✅ | Key names stored in namespace; available to planner without plugin lookup |
| **Row count estimation** | ✅ | Uses `DescribeTable` ApproximateItemCount for planner statistics |
| **Cross-source JOIN** | ✅ | Joins DynamoDB tables with Hudi, Delta, S3, etc. |
| **Auto-reconnect** | ✅ | Recovers from SDK errors without Dremio restart |
| **DynamoDB Local** | ✅ | Works with `amazon/dynamodb-local` for dev/test |
| **IAM / instance profile** | ✅ | Leave credentials blank to use default credential chain |
| **CTAS / INSERT INTO** | ❌ | Read-only; write support not planned |

---

## Quick Install

```bash
# Interactive — prompts for mode and JAR choice
./install.sh

# Docker, use the included pre-built JAR (fastest, no Maven needed)
./install.sh --docker try-dremio --prebuilt

# Docker, build from source inside the container
./install.sh --docker try-dremio --build

# Bare-metal Dremio installation
./install.sh --local /opt/dremio --prebuilt

# Kubernetes pod
./install.sh --k8s dremio-0 --prebuilt
```

A pre-built JAR is included in `jars/` so Maven is not required for a standard install.

See [INSTALL.md](INSTALL.md) for full configuration reference.

---

## Adding a Source

After installation, go to **Sources → + → Amazon DynamoDB** in the Dremio UI.

For DynamoDB Local (Docker):
```
Region:            us-east-1
Endpoint Override: http://dynamodb-local:8000
Access Key ID:     fakeKey
Secret Access Key: fakeSecret
```

For AWS DynamoDB:
```
Region:            us-east-1   (or your region)
Endpoint Override: (leave blank)
Access Key ID:     (leave blank to use IAM / instance profile)
Secret Access Key: (leave blank to use IAM / instance profile)
```

Or use the setup script:

```bash
# DynamoDB Local
./add-dynamodb-source.sh \
  --name dynamodb_local \
  --region us-east-1 \
  --endpoint http://dynamodb-local:8000 \
  --access-key fakeKey \
  --secret-key fakeSecret

# AWS DynamoDB (using IAM role)
./add-dynamodb-source.sh \
  --name dynamodb_prod \
  --region us-east-1
```

---

## Querying

```sql
-- Full table scan
SELECT * FROM dynamodb_source.users LIMIT 100;

-- Partition key EQ → DynamoDB Query API (efficient point lookup)
SELECT * FROM dynamodb_source.users WHERE user_id = 'u1';

-- Partition key + sort key range → KeyConditionExpression
SELECT * FROM dynamodb_source.orders WHERE order_id = 'o1' AND user_id >= 'u1';

-- Non-key filter → FilterExpression
SELECT * FROM dynamodb_source.users WHERE country = 'US';

-- Aggregation (computed by Dremio after fetching)
SELECT country, COUNT(*), AVG(score) FROM dynamodb_source.users GROUP BY country;

-- Cross-source JOIN
SELECT u.name, o.total
FROM dynamodb_source.users u
JOIN dynamodb_source.orders o ON u.user_id = o.user_id;
```

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| **AWS Region** | `us-east-1` | AWS region where your DynamoDB tables live |
| **Endpoint Override** | _(blank)_ | Override endpoint URL — use `http://localhost:8000` for DynamoDB Local; leave blank for AWS |
| **Access Key ID** | _(blank)_ | AWS access key; leave blank to use IAM / instance profile / environment variables |
| **Secret Access Key** | _(blank)_ | AWS secret key; leave blank to use default credential chain |
| **Schema Sample Size** | `100` | Items sampled per table for schema inference; larger = more accurate, slower refresh |
| **Split Parallelism** | `4` | DynamoDB Parallel Scan segments per table; higher = more throughput on large tables |
| **Read Timeout (seconds)** | `30` | AWS SDK call timeout |
| **Page Size** | `1000` | Max items per DynamoDB Scan/Query page |
| **Schema Cache TTL (seconds)** | `60` | How long to cache inferred schema before re-sampling; `0` = always re-sample |

---

## Test Results

27/27 smoke tests passing. See [TEST_RESULTS.md](TEST_RESULTS.md).

---

## Upgrading Dremio

Use `rebuild.sh` when you upgrade Dremio to a new version:

```bash
./rebuild.sh --docker try-dremio
```

See [INSTALL.md — Upgrading](INSTALL.md#upgrading-dremio) for details.

---

## Kubernetes

See [k8s/KUBERNETES.md](k8s/KUBERNETES.md) for Helm-based deployment instructions
using either a custom Dremio image or an init container.

---

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
