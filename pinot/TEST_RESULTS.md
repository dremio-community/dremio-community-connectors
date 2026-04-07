# Test Results — Dremio Apache Pinot Connector

## Build Status

| Item | Result |
|---|---|
| Maven compilation | ✅ PASS |
| Shaded JAR created | ✅ PASS (`dremio-pinot-connector-1.0.0-SNAPSHOT-plugin.jar`, ~96 MB) |
| Dremio 26.x compatibility | ✅ Compiles against Dremio 26.0.5 JARs |

## Notes

The connector JAR is ~96 MB because the Apache Pinot JDBC client (`pinot-jdbc-client:1.4.0`)
bundles a significant amount of Pinot infrastructure code. Libraries already provided by
Dremio (Jackson, SLF4J, Guava, Arrow, Netty) are excluded from the shaded JAR.

## Live Integration Tests

Live end-to-end tests require a running Apache Pinot cluster (controller on port 9000).

**To run against a local Pinot instance:**

```bash
# Start Pinot with Docker
docker run -p 9000:9000 apachepinot/pinot:latest StartController

# Build and install the connector
./install.sh --docker try-dremio

# In Dremio UI: Add Source → Apache Pinot
# Host: localhost, Port: 9000

# Run smoke tests
./test-connection.sh --url http://localhost:9047 --user mark --password critter77 --source pinot
```

## Recommended Test Queries

Once connected to a Pinot cluster with data:

```sql
-- 1. Basic connectivity
SELECT 1;

-- 2. Table discovery
SHOW TABLES IN pinot_source;

-- 3. Simple scan
SELECT * FROM pinot_source.<table> LIMIT 10;

-- 4. Aggregation pushdown
SELECT col, COUNT(*), SUM(metric) FROM pinot_source.<table>
GROUP BY col ORDER BY 2 DESC LIMIT 20;

-- 5. Filter pushdown
SELECT * FROM pinot_source.<table>
WHERE string_col = 'value' AND int_col > 100
LIMIT 100;

-- 6. EXPLAIN to verify pushdown
EXPLAIN PLAN FOR
SELECT region, COUNT(*) FROM pinot_source.<table> GROUP BY region;
```

## Build Environment

| Component | Version |
|---|---|
| Dremio | 26.0.5-202509091642240013-f5051a07 |
| Apache Pinot JDBC client | 1.4.0 |
| Java | 11 |
| Maven | 3.x |
| Build date | 2026-04-07 |
