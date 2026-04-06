# Contributing to Dremio Community Connectors

Thank you for your interest in contributing! This guide covers bug reports, feature requests, and pull requests.

---

## Reporting Bugs

1. Search [existing issues](../../issues) first — your bug may already be reported.
2. Open a new issue using the **Bug report** template.
3. Include:
   - Dremio version (`docker exec try-dremio cat /opt/dremio/VERSION`)
   - Connector version (the JAR filename, e.g. `dremio-clickhouse-connector-1.0.0-SNAPSHOT-plugin.jar`)
   - The source system version (ClickHouse 24.x, Kafka 3.7, etc.)
   - Exact error message from Dremio logs
   - The query that triggered the issue (or the `add-*-source.sh` command)
   - What you expected vs. what happened

---

## Requesting Features

Open a **Feature request** issue. Describe:
- The use case (what you're trying to accomplish)
- The current workaround (if any)
- Which connector it applies to

---

## Submitting Pull Requests

### Setup

```bash
git clone https://github.com/<org>/dremio-community-connectors.git
cd dremio-community-connectors

# Work on a specific connector
cd clickhouse    # or kafka, cassandra, hudi, delta

# Build from source (requires a running Dremio instance for JAR installation)
./install.sh --docker try-dremio --build
```

### Process

1. **Fork** the repository and create a feature branch: `git checkout -b feat/clickhouse-array-pushdown`
2. **Make your changes** — one logical change per PR is easier to review.
3. **Test locally:**
   ```bash
   # Unit tests
   mvn test

   # Integration smoke test (requires running Dremio + source)
   ./test-connection.sh
   ```
4. **Update docs** — if you change behavior, update the relevant `README.md`, `INSTALL.md`, or `USER_GUIDE.md`.
5. **Open the PR** against `main` and fill in the PR template.

### Code Style

- Java: standard Google Java style (4-space indent, 120-char line limit)
- Shell scripts: `bash`, `set -euo pipefail`, quote all variables
- YAML: 2-space indent

### What Makes a Good PR

- Focused: one feature or fix per PR
- Tested: includes test coverage for new behavior or a test case that reproduces the fixed bug
- Documented: `BUILD_JOURNAL.md` entry describing the problem and solution
- Clean: no debug prints, no commented-out code

---

## Local Development Environment

### ClickHouse

```bash
# Start ClickHouse
docker run -d --name try-clickhouse \
  -p 8123:8123 -p 9000:9000 \
  clickhouse/clickhouse-server:latest

# Start Dremio
docker run -d --name try-dremio \
  -p 9047:9047 -p 31010:31010 \
  dremio/dremio-oss:26.0

# Connect them
docker network create dremio-net
docker network connect dremio-net try-dremio
docker network connect dremio-net try-clickhouse
```

### Kafka (docker-compose stack)

```bash
cd kafka
docker compose up -d
# Wait ~60s, then:
./install.sh --docker dremio
docker restart dremio
./add-kafka-source.sh --name kafka_test --brokers localhost:9092
./test-connection.sh --source kafka_test --topic orders
```

---

## Questions

Open a [Discussion](../../discussions) if you have questions about the codebase or want to propose a larger design change before investing time in a PR.
