# Dremio Neo4j Connector

*Built by Mark Shainman*

A Dremio storage plugin that adds **read support for Neo4j graph databases** via the Bolt protocol. Each Neo4j node label is exposed as a SQL table with properties dynamically inferred by sampling nodes.

---

## Architecture

```
SQL:  SELECT name, born FROM neo4j_source.Person
      WHERE born > 1970 ORDER BY born DESC LIMIT 10

Dremio Planner
  └── Neo4jScanRule / Neo4jScanPrule
        └── Neo4jGroupScan  (one split per label)
              └── Neo4jRecordReader
                    ├── CALL db.labels()  → discover node labels
                    ├── MATCH (n:Label) RETURN n LIMIT sampleSize  → infer schema
                    └── MATCH (n:Label) RETURN n SKIP offset LIMIT batchSize  → paginate

Neo4jConnection (Bolt driver 4.4.14)
  └── bolt://host:port  with username/password auth
        └── Neo4jTypeConverter  → Arrow field types

Neo4jTypeConverter
  └── Neo4j type → Dremio type:
        INTEGER/FLOAT → Float8, BOOLEAN → Bit,
        STRING → Utf8, LIST → Utf8 (JSON), NULL → Utf8
```

### Key Classes

| Class | Role |
|---|---|
| `Neo4jConf` | Source config: Bolt URI, username, password, database, sample size, fetch batch size, timeout |
| `Neo4jConnection` | Bolt session management; label discovery; schema sampling |
| `Neo4jRecordReader` | Paginates `MATCH … SKIP … LIMIT` to stream all nodes; writes Arrow fields |
| `Neo4jTypeConverter` | Maps Neo4j value types to Dremio/Arrow types |
| `Neo4jDatasetMetadata` | Schema descriptor per label (field names + types) |
| `Neo4jGroupScan` | One `Neo4jScanSpec` per label |
| `Neo4jStoragePlugin` | Registers the connector; returns `Neo4jConnection` |

### Driver Version

Uses **Neo4j Java Driver 4.4.14** (not 5.x) for Java 11 compatibility — Dremio 26 runs on JVM 11. The driver is bundled and shaded (relocated to `com.dremio.plugins.neo4j.shaded.*`) to avoid classpath conflicts with Netty and Reactor.

---

## What's Implemented

| Feature | Status | Notes |
|---|---|---|
| **Label discovery** | ✅ Working | `CALL db.labels()` returns all node labels |
| **Schema inference** | ✅ Working | Samples up to 50 nodes per label; infers Float8, Bit, Utf8 |
| **Property reading** | ✅ Working | All node properties mapped to Arrow fields |
| **Pagination** | ✅ Working | `SKIP` / `LIMIT` Cypher paging |
| **Multi-database** | ✅ Working | Configurable database (default: `neo4j`) |
| **Encrypted (TLS)** | ✅ Working | Bolt+s:// URI for encrypted connections |
| **WHERE / filter pushdown** | ❌ Not supported | Dremio evaluates filters post-fetch |
| **Relationship traversal** | ❌ Not supported | Only node labels exposed as tables |
| **INSERT / UPDATE / DELETE** | ❌ Not supported | Read-only connector |

---

## Quick Start

### 1. Install the connector

```bash
# Docker (default container: try-dremio)
./install.sh --docker try-dremio --prebuilt

# Bare-metal
./install.sh --local /opt/dremio --prebuilt

# Kubernetes
./install.sh --k8s dremio-master-0
```

### 2. Add a Neo4j source in Dremio

Open `http://localhost:9047` → **Sources → + → Neo4j** and fill in:

| Field | Value |
|---|---|
| Bolt URI | `bolt://localhost:7687` |
| Username | `neo4j` |
| Password | your password |
| Database | `neo4j` (default) |

### 3. Query

```sql
-- Each node label is a table
SELECT * FROM neo4j_source.Person LIMIT 10;

-- Filter and sort on node properties
SELECT name, born FROM neo4j_source.Person
WHERE born > 1970 ORDER BY born DESC;

-- Aggregations
SELECT COUNT(*) FROM neo4j_source.Movie;
SELECT AVG(born) AS avg_born FROM neo4j_source.Person WHERE born IS NOT NULL;

-- GROUP BY
SELECT released, COUNT(*) AS cnt
FROM neo4j_source.Movie
GROUP BY released ORDER BY released DESC;

-- Boolean properties
SELECT name, active FROM neo4j_source.Person WHERE active = true;

-- Cross-source JOIN: Neo4j + Iceberg
SELECT p.name, p.born, i.segment
FROM neo4j_source.Person p
JOIN iceberg_minio.analytics.segments i ON p.person_id = i.external_id;
```

---

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| Bolt URI | `bolt://localhost:7687` | Neo4j Bolt endpoint. Use `bolt+s://` for TLS. |
| Username | `neo4j` | Neo4j username |
| Password | — | Neo4j password (stored as secret) |
| Database | `neo4j` | Target database name |
| Schema Sample Size | `50` | Nodes sampled per label for type inference |
| Fetch Batch Size | `500` | Nodes fetched per Cypher `LIMIT` page |
| Connection Timeout (seconds) | `30` | Bolt connection timeout |

---

## Building

```bash
mvn clean package -DskipTests
# Deployable JAR:
jars/dremio-neo4j-connector-1.0.0.jar
```

---

## Smoke Tests

28 tests (standard battery + Neo4j-specific, 1 intentional skip) in the companion test harness:

```bash
cd dremio-connector-tests
python3 -m pytest connectors/test_neo4j.py -v
```

**Result: 28/28 tests passing, 1 skip (no natural JOIN key between Person and Movie).**

Test coverage includes:
- Person and Movie label tables
- Born year lookup, boolean `active` filter
- AVG/MAX aggregations, ORDER BY, GROUP BY
- Exact row counts, column counts

---

## References

- [Neo4j Java Driver 4.x Documentation](https://neo4j.com/docs/java-manual/4.4/)
- [Neo4j Cypher Reference](https://neo4j.com/docs/cypher-manual/current/)
- [Bolt Protocol](https://neo4j.com/docs/bolt/current/)
