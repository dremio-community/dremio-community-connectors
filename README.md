# Dremio Community Connectors

Community-built storage plugins for [Dremio](https://www.dremio.com/) — adding read support for data sources not covered by Dremio's official connector library.

Each connector is a self-contained Dremio storage plugin that installs as a JAR into `jars/3rdparty/` and exposes its data source as SQL tables in the Dremio catalog.

---

## Connectors

| Connector | Source | Auth | Status |
|-----------|--------|------|--------|
| [ClickHouse](clickhouse/) | ClickHouse (HTTP/HTTPS) | Username/password, SSL, ClickHouse Cloud | ✅ 32/32 tests passing |
| [Apache Kafka](kafka/) | Kafka topics | SASL (PLAIN, SCRAM-SHA-256/512), TLS, mTLS | ✅ 27/27 tests passing |
| [Apache Cassandra](cassandra/) | Cassandra CQL | Username/password, SSL | ✅ Tests passing |
| [Apache Hudi](hudi/) | Hudi tables on S3/HDFS | IAM, service account | ✅ Tests passing |
| [Delta Lake](delta/) | Delta tables on S3/HDFS | IAM, service account | ✅ Tests passing |
| [Excel / CSV Importer](excel-importer/) | `.xlsx`, `.csv`, Google Sheets | Dremio REST API (user/password) | ✅ Working |
| [Splunk](splunk/) | Splunk indexes (on-prem + Cloud) | Username/password, bearer token | ✅ 20/20 tests passing |

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
├── splunk/          — Splunk connector (REST API / SPL)
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
