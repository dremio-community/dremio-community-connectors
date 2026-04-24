# Dremio Community Connectors

Community-built storage plugins for [Dremio](https://www.dremio.com/) — adding read support for data sources not covered by Dremio's official connector library.

Each connector is a self-contained Dremio storage plugin that installs as a JAR into `jars/3rdparty/` and exposes its data source as SQL tables in the Dremio catalog.

> **Looking for UDF libraries?** Vector, geospatial, and other SQL function extensions live in the companion repo: [dremio-community/dremio-community-udfs](https://github.com/dremio-community/dremio-community-udfs)

---

## Connectors

| Connector | Source | Auth | Status |
|-----------|--------|------|--------|
| [ClickHouse](clickhouse/) | ClickHouse (HTTP/HTTPS) | Username/password, SSL, ClickHouse Cloud | ✅ 32/32 tests passing |
| [Apache Kafka](kafka/) | Kafka topics | SASL (PLAIN, SCRAM-SHA-256/512), TLS, mTLS | ✅ 27/27 tests passing |
| [Apache Cassandra](cassandra/) | Cassandra CQL | Username/password, SSL | ✅ Tests passing |
| [Apache Hudi](hudi/) | Hudi tables on S3/HDFS | IAM, service account | ✅ Tests passing |
| [Delta Lake](delta/) | Delta tables on S3/HDFS | IAM, service account | ✅ Tests passing |
| [Apache Pinot](pinot/) | Pinot real-time tables | Username/password, TLS | ✅ 10/10 tests passing |
| [Amazon DynamoDB](dynamodb/) | DynamoDB tables (any region) | IAM role, instance profile, static keys | ✅ 27/27 tests passing |
| [Splunk](splunk/) | Splunk indexes (on-prem + Cloud) | Username/password, bearer token | ✅ 20/20 tests passing |
| [Salesforce](salesforce/) | Salesforce SObjects (REST API) | OAuth2 Connected App | ✅ Working |
| [Excel / CSV Importer](excel-importer/) | `.xlsx`, `.csv`, Google Sheets | Dremio REST API (user/password) | ✅ Working |

---

## Quick Install

Each connector has its own `install.sh` that handles build, deploy, and restart for Docker, bare-metal, and Kubernetes.

```bash
# ClickHouse
cd clickhouse
./install.sh --docker try-dremio --prebuilt

# Apache Kafka
cd kafka
./install.sh --docker try-dremio --prebuilt

# Apache Cassandra
cd cassandra
./install.sh --docker try-dremio --prebuilt

# Apache Hudi
cd hudi
./install.sh --docker try-dremio --prebuilt

# Delta Lake
cd delta
./install.sh --docker try-dremio --prebuilt

# Apache Pinot
cd pinot
./install.sh --docker try-dremio

# Amazon DynamoDB
cd dynamodb
./install.sh --docker try-dremio --prebuilt

# Salesforce (REST)
cd salesforce
./install.sh --docker try-dremio --prebuilt
```

After installing, restart Dremio. The new source type will appear under **Sources → +**.

---

## Connector Overview

### [ClickHouse](clickhouse/)

SQL-native connector using Dremio's ARP (Advanced Relational Pushdown) framework. Full predicate, aggregation, sort, limit, and join pushdown via the official ClickHouse JDBC driver. Supports ClickHouse Cloud.

```bash
./add-clickhouse-source.sh --name clickhouse --host my-server.example.com
./add-clickhouse-source.sh --name ch_cloud --host abc123.us-east-1.aws.clickhouse.cloud --cloud
```

**Key features:** 60+ pushed-down functions · ClickHouse Cloud · SSL/TLS · compression · full date/type support

---

### [Apache Kafka](kafka/)

Exposes Kafka topics as bounded SQL tables. Offsets are frozen at plan time for deterministic, repeatable results. Supports JSON schema inference, AVRO decoding (with Confluent Schema Registry), RAW mode, and filter pushdown on partition/offset/timestamp columns.

```bash
./add-kafka-source.sh --name kafka --brokers localhost:9092
./add-kafka-source.sh --name kafka_prod --brokers broker1:9092,broker2:9092 \
  --security SASL_SSL --sasl-mech SCRAM-SHA-512 \
  --sasl-user myuser --sasl-pass mypassword
```

**Key features:** JSON · AVRO · RAW modes · filter pushdown · CTAS to Iceberg · SASL auth · mTLS

---

### [Apache Cassandra](cassandra/)

Native CQL connector with custom Calcite planning rules for predicate, projection, limit, and sort pushdown directly to Cassandra. Uses the DataStax Java Driver.

**Key features:** Predicate pushdown · partition key routing · clustering key range scans · token-aware queries

---

### [Apache Hudi](hudi/)

Reads Hudi Copy-on-Write (COW) and Merge-on-Read (MOR) tables directly from S3, HDFS, or local filesystem. Understands the Hudi timeline and always returns the latest committed snapshot.

**Key features:** COW + MOR support · Hudi metadata · Parquet/Avro decoding · S3/HDFS

---

### [Delta Lake](delta/)

Reads Delta Lake tables using `delta-standalone` (no Spark required). Resolves the latest checkpoint and log to present a consistent snapshot of the table as a Dremio virtual dataset.

**Key features:** No Spark dependency · checkpoint + log resolution · schema evolution · S3/HDFS

---

### [Apache Pinot](pinot/)

ARP/JDBC connector that exposes Apache Pinot real-time tables as Dremio SQL tables. Full predicate, aggregation, ORDER BY, and LIMIT pushdown via the Pinot JDBC driver. Suitable for time-series analytics and low-latency queries over streaming data.

```bash
./add-pinot-source.sh --name pinot --controller localhost
./add-pinot-source.sh --name pinot_prod --controller pinot.example.com \
  --port 9000 --user myuser --password mypassword
```

```sql
SELECT eventType, COUNT(*) AS cnt, AVG(totalAmount) AS avg_amount
FROM pinot.transactions
WHERE eventTime >= 1700000000000
GROUP BY eventType
ORDER BY cnt DESC
LIMIT 20;
```

**Key features:** Full SQL pushdown · real-time tables · schema discovery · TLS · username/password auth · external query

---

### [Amazon DynamoDB](dynamodb/)

Native connector that queries DynamoDB tables directly from Dremio SQL. Uses the AWS SDK v2 with partition key equality routing to the DynamoDB Query API for efficient single-partition lookups, with Parallel Scan for full-table reads. Schema is inferred by sampling items.

```bash
# DynamoDB Local (development)
./add-dynamodb-source.sh --name dynamodb_local \
  --endpoint http://dynamodb-local:8000 \
  --access-key fakeKey --secret-key fakeSecret

# AWS DynamoDB (IAM role / instance profile)
./add-dynamodb-source.sh --name dynamodb_prod --region us-east-1
```

```sql
-- Partition key EQ → DynamoDB Query API (efficient, reads one partition)
SELECT * FROM dynamodb_prod.orders WHERE order_id = 'o42';

-- Sort key range → KeyConditionExpression
SELECT * FROM dynamodb_prod.orders
WHERE order_id = 'o1' AND created_at >= '2024-01-01';

-- Full table → Parallel Scan
SELECT country, COUNT(*) FROM dynamodb_prod.users GROUP BY country;
```

**Key features:** Query API routing · sort key range pushdown · FilterExpression pushdown · projection pushdown · Parallel Scan · SS/NS list types · IAM / instance profile / static key auth

---

### [Splunk](splunk/)

Queries Splunk indexes as SQL tables using the Splunk REST API. Translates SQL to SPL with time-range pushdown (`WHERE _time >= ...` → `earliest_time` parameter) and field-equality pushdown (`WHERE field = 'value'` → SPL filter clause). Supports Splunk on-prem and Splunk Cloud (JWT bearer tokens). Schema is inferred by sampling recent events per index.

```bash
./add-splunk-source.sh --name splunk --host splunk.example.com \
  --splunk-user admin --splunk-pass changeme
```

```sql
SELECT _time, _host, _sourcetype, status, clientip
FROM splunk.web_logs
WHERE _time >= NOW() - INTERVAL '1' HOUR
  AND status = '404'
LIMIT 500;
```

**Key features:** SPL time pushdown · field-equality pushdown · LIMIT pushdown · schema inference · Splunk Cloud · bearer token auth · job cleanup on cancel

---

### [Salesforce (REST)](salesforce/)

Native connector that queries Salesforce SObjects as SQL tables using the REST API and SOQL. OAuth2 password-grant authentication with Connected App credentials. Auto-discovers all accessible SObjects, pushes WHERE clauses as SOQL filters, and reads data via paginated queryMore cursors. No JDBC driver required.

```bash
./add-salesforce-source.sh --name salesforce \
  --sf-user you@example.com --sf-pass yourpassword \
  --sf-token yourSecurityToken \
  --client-id 3MVG... --client-secret ABC123
```

```sql
SELECT Id, Name, Amount, StageName, CloseDate
FROM salesforce.Opportunity
WHERE StageName = 'Closed Won'
  AND CloseDate >= DATE '2026-01-01';

-- Cross-source JOIN: Salesforce + Iceberg
SELECT s.Id, s.Name, i.segment
FROM salesforce.Account s
JOIN iceberg_catalog.crm.segments i ON s.Id = i.sf_account_id;
```

**Key features:** SOQL WHERE pushdown · parallel LIMIT+OFFSET splits · auto-discovery · OAuth2 token refresh · sandbox support · object exclusion · full type mapping



---

### [Excel / CSV Importer](excel-importer/)

Imports `.xlsx` spreadsheets, `.csv` files, and Google Sheets directly into Dremio Iceberg tables via the REST API. Includes a web UI with live progress streaming, schema preview, per-column type overrides, multi-sheet import, and connection profiles. No JDBC driver needed.

```bash
# Web UI (easiest)
python3 excel-importer/importer-ui.py

# CLI
java -jar excel-importer/jars/dremio-excel-importer.jar \
  --file report.xlsx \
  --dest "iceberg_minio.my-bucket.my_table" \
  --user admin --password secret --yes
```

**Key features:** Web UI · CSV + XLSX + Google Sheets · schema preview · multi-sheet import · append mode · Docker/Kubernetes support

---

## Requirements

| Requirement | Details |
|-------------|---------|
| Dremio OSS or Enterprise | 26.x (connectors are version-pinned; use `rebuild.sh` for other versions) |
| Java 11+ | Required on the build machine if building from source |
| Maven 3.8+ | Required for source builds only |
| `curl` | Required by `add-*-source.sh` and `test-connection.sh` |
| `python3` | Required by `add-*-source.sh` for JSON payload building |

---

## Keeping Up with Dremio Upgrades

Each connector includes a `rebuild.sh` script that detects the running Dremio version, updates `pom.xml`, rebuilds the JAR, and redeploys — all in one command.

```bash
cd clickhouse && ./rebuild.sh               # Docker (default: try-dremio)
cd kafka     && ./rebuild.sh --k8s pod-0    # Kubernetes
cd cassandra && ./rebuild.sh --dry-run      # Preview only
```

---

## Repository Structure

```
dremio-community-connectors/
├── clickhouse/      — ClickHouse connector (ARP/JDBC)
├── kafka/           — Apache Kafka connector
├── cassandra/       — Apache Cassandra connector
├── hudi/            — Apache Hudi connector
├── delta/           — Delta Lake connector
├── pinot/           — Apache Pinot connector (ARP/JDBC)
├── dynamodb/        — Amazon DynamoDB connector (native)
├── splunk/          — Splunk connector (REST API / SPL)
├── salesforce/      — Salesforce connector (REST API / SOQL)
├── excel-importer/  — Excel / CSV / Google Sheets importer
└── .github/
    ├── workflows/   — Per-connector CI (builds on every push/PR)
    └── ISSUE_TEMPLATE/
```

Each connector subdirectory is self-contained: source, scripts, docs, and pre-built JARs.

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for how to report bugs, request features, and submit pull requests.

---

## License

Apache License 2.0 — see [LICENSE](clickhouse/LICENSE).

*Built by Mark Shainman*
