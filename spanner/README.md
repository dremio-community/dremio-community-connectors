# Dremio Google Cloud Spanner Connector

A community Dremio storage plugin that lets you query **Google Cloud Spanner** tables using standard SQL through Dremio, without any ETL.

## Features

- Browse and query any Spanner database table from Dremio's UI or REST API
- Full type mapping: BOOL, INT64, FLOAT32, FLOAT64, STRING, BYTES, DATE, TIMESTAMP, NUMERIC, JSON, ARRAY
- Parallel table reads using MOD-based segmentation (configurable split count)
- Filter and LIMIT pushdown to Spanner (reduces data transferred)
- Schema caching with configurable TTL (avoids repeated INFORMATION_SCHEMA queries)
- Flexible auth: service account key file, ADC, Workload Identity, or emulator

## Prerequisites

- Dremio 25.x or later (community or enterprise)
- Java 11+, Maven 3.8+ (to build from source)
- A Google Cloud project with Cloud Spanner enabled
- A GCP service account with `roles/spanner.databaseReader` (or ADC with equivalent permissions)

## Quick Start

### 1. Build

```bash
cd spanner/
mvn package -DskipTests
# JAR produced at: target/dremio-spanner-plugin-<version>-shaded.jar
```

### 2. Install

```bash
# Local Dremio (set DREMIO_HOME or JARS_DIR env vars if non-default)
./install.sh

# Docker Dremio (set DREMIO_CONTAINER if container name differs from 'dremio')
DREMIO_CONTAINER=dremio ./install.sh
```

Or copy manually:

```bash
cp target/dremio-spanner-plugin-*-shaded.jar /opt/dremio/jars/3rdparty/
# restart Dremio
```

### 3. Configure in Dremio

1. Open Dremio UI → **Sources** → **+ Add Source**
2. Select **Google Cloud Spanner**
3. Fill in:
   - **GCP Project ID** — your Google Cloud project (e.g. `my-project`)
   - **Spanner Instance ID** — your Spanner instance (e.g. `my-instance`)
   - **Database** — database name (e.g. `my-db`)
   - **Service Account Key File** — path to a JSON key file on the Dremio server, or leave blank for ADC

4. Click **Save**. Dremio will discover all tables automatically.

### 4. Query

```sql
SELECT * FROM spanner_source.my_table LIMIT 100;
SELECT name, salary FROM spanner_source.employees WHERE department = 'Engineering';
```

## Configuration Reference

| Field | Default | Description |
|---|---|---|
| `project` | *(required)* | GCP project ID |
| `instance` | *(required)* | Spanner instance ID |
| `database` | *(required)* | Spanner database name |
| `credentialsFile` | *(blank)* | Path to service account JSON key; blank = ADC |
| `splitParallelism` | `4` | Number of parallel read segments per table |
| `queryTimeoutSeconds` | `300` | Spanner query timeout in seconds |
| `metadataCacheTtlSeconds` | `300` | Schema cache TTL in seconds (0 = disabled) |

## Type Mapping

| Spanner Type | Dremio / Arrow Type |
|---|---|
| BOOL | BIT |
| INT64 | BIGINT |
| FLOAT32 | FLOAT |
| FLOAT64 | DOUBLE |
| STRING(N/MAX) | VARCHAR |
| BYTES(N/MAX) | VARBINARY |
| DATE | DATE |
| TIMESTAMP | TIMESTAMP (UTC) |
| NUMERIC | VARCHAR (decimal string) |
| JSON | VARCHAR |
| ARRAY\<T\> | VARCHAR (JSON representation) |

## Using the Spanner Emulator

For local development with the [Cloud Spanner Emulator](https://cloud.google.com/spanner/docs/emulator):

1. Set the `SPANNER_EMULATOR_HOST` environment variable on your Dremio server before starting it:
   ```bash
   export SPANNER_EMULATOR_HOST=localhost:9010
   ```
2. Leave `credentialsFile` blank — the emulator accepts any credentials.
3. Set `project`, `instance`, and `database` to match your emulator configuration.

## Architecture

```
ScanCrel
  └─ SpannerScanRule (LOGICAL)
       └─ SpannerScanDrel
            └─ SpannerScanPrule (PHYSICAL)
                 └─ SpannerScanPrel
                      └─ SpannerGroupScan.getSpecificScan()
                           └─ SpannerSubScan  (serialized to executor fragment)
                                └─ SpannerScanCreator
                                     └─ SpannerRecordReader
                                          └─ SpannerConnection → Spanner ResultSet
```

Each executor fragment receives one or more `SpannerScanSpec` objects. The RecordReader executes a SQL query per spec, using `MOD(ABS(FARM_FINGERPRINT(...)), totalSegments) = segment` to partition the data across parallel fragments.

## License

Apache 2.0
