# Dremio Talend Connector

A native, high-performance Talend Studio connector for **full ELT with Dremio** — extract data from Dremio, load data into Dremio, and trigger SQL transformations inside Dremio, all from a single Talend job.

The read component uses **Apache Arrow Flight RPC** for maximum throughput, bypassing JDBC entirely and streaming partitions in parallel directly from Dremio's executor nodes. The write and SQL components use the **Dremio REST API** for broad compatibility across all Dremio editions.

## Components

| Component | Palette Name | Role |
|---|---|---|
| `DremioInputMapper` | `tDremioInput` | **Extract** — reads any Dremio SQL query via Arrow Flight |
| `DremioOutputSink` | `tDremioOutput` | **Load** — writes Talend records into a Dremio Iceberg table |
| `DremioExecuteSQLMapper` | `tDremioExecuteSQL` | **Transform** — submits any SQL (DDL/DML) and waits for completion |

### ELT Pattern

```
[Source] → tDremioOutput → (data lands in Dremio staging table)
                              ↓
                    tDremioExecuteSQL: INSERT INTO target SELECT ... FROM staging
                              ↓
                    tDremioInput → [Downstream]
```

## Features

- **Arrow Flight Protocol**: Sub-second latency extraction by avoiding JDBC serialization bottlenecks.
- **Parallel Partitioning**: `@Split` logic maps to Dremio `FlightEndpoint` tickets for parallel reads.
- **Batch INSERT Writes**: Records are buffered and flushed as batched `INSERT INTO ... VALUES (...)` statements.
- **APPEND / OVERWRITE modes**: `tDremioOutput` can append to or fully replace a table's contents.
- **SQL Job Polling**: `tDremioExecuteSQL` submits SQL, polls until the job reaches a terminal state, and emits a result record with `job_id`, `job_state`, and `error_message`.
- **Dremio Cloud Support**: SSL/TLS toggle for Cloud endpoints on both Flight and REST.
- **Health Checks**: Live Studio "Test Connection" button powered by TCK `@HealthCheck`.

## Prerequisites

- Talend Studio (v8.x+)
- Dremio OSS / Software / Cloud (v20.x+)
- Personal Access Token (PAT) for Dremio Software/Cloud authentication (or username for OSS)

## Installation

### Building the Component Archive (.car)

```bash
./build.sh
```

This runs `./mvnw clean install` and produces a `.car` archive in `target/`.

### Deploying to Talend Studio

1. Navigate to `target/` and locate `dremio-talend-connector-1.0.0-SNAPSHOT.car`.
2. Open Talend Studio.
3. Drag and drop the `.car` file into the Talend Studio workspace, or install via the Component Manager.
4. All three components (`tDremioInput`, `tDremioOutput`, `tDremioExecuteSQL`) appear in the **Dremio** family in your Palette.

## Configuration

### Dremio Connection (DataStore) — shared by all three components

| Field | Description | Default |
|---|---|---|
| Host | Dremio coordinator host | `localhost` |
| Port | Arrow Flight port | `32010` |
| Enable SSL/TLS | Required for Dremio Cloud | `false` |
| Username | Leave blank when using a PAT | — |
| Personal Access Token | PAT or password | — |

### tDremioInput

| Field | Description |
|---|---|
| SQL Query | Any Dremio SQL query |

### tDremioOutput

| Field | Description | Default |
|---|---|---|
| Target Table Path | Fully-qualified table (e.g. `my_catalog.my_schema.my_table`) | — |
| Write Mode | `APPEND` or `OVERWRITE` | `APPEND` |
| Batch Size | Records per INSERT statement | `500` |
| REST API Port | Dremio REST port (`9047` HTTP, `443` Cloud) | `9047` |

### tDremioExecuteSQL

| Field | Description | Default |
|---|---|---|
| SQL Statement | Any DDL or DML (CTAS, INSERT, DROP, etc.) | — |
| REST API Port | Dremio REST port | `9047` |
| Poll Interval (ms) | How often to check job status | `500` |
| Timeout (seconds) | Max wait before timeout error | `300` |

The `tDremioExecuteSQL` emitter produces one record per execution:

| Field | Type | Description |
|---|---|---|
| `job_id` | String | Dremio job ID |
| `job_state` | String | `COMPLETED`, `FAILED`, or `CANCELED` |
| `sql_statement` | String | The SQL that was submitted |
| `error_message` | String | Error detail if the job failed |

## Supported Arrow Data Types (tDremioInput)

| Arrow Vector | Talend Type | Notes |
|---|---|---|
| `VarCharVector` | `String` | UTF-8 decoded |
| `IntVector` | `Int` | 32-bit |
| `SmallIntVector` | `Int` | widened to 32-bit |
| `TinyIntVector` | `Int` | widened to 32-bit |
| `BigIntVector` | `Long` | 64-bit |
| `Float4Vector` | `Float` | 32-bit |
| `Float8Vector` | `Double` | 64-bit |
| `DecimalVector` | `Double` | precision may be lost for very large decimals |
| `BitVector` | `Boolean` | |
| `TimeStampVector` | `DateTime` | normalized to UTC |
| `DateDayVector` | `DateTime` | normalized to UTC |
| `DateMilliVector` | `DateTime` | normalized to UTC |
| All others | `String` | `toString()` fallback |

## Compatibility Notes

- **Java 8**: Compiled targeting Java 8 for maximum Talend Studio compatibility. Arrow Flight 11.0.0 is the most recent version stable on Java 8. If your Talend environment runs Java 11+, you may upgrade `arrow.version` in `pom.xml`.
- **REST API port**: `tDremioOutput` and `tDremioExecuteSQL` use the Dremio REST API (default port `9047`), not the Arrow Flight port (`32010`). Set `restPort` to `443` for Dremio Cloud.

## Programmatic Testing

```bash
./mvnw test
```

Tests use environment variables with safe defaults so they pass without a live Dremio instance:

| Variable | Used by | Default |
|---|---|---|
| `DREMIO_HOST` | all | `localhost` |
| `DREMIO_PORT` | tDremioInput | `32010` |
| `DREMIO_USER` | tDremioInput | `dremio` |
| `DREMIO_PAT` | all | `dremio123` |
| `DREMIO_REST_PORT` | tDremioOutput, tDremioExecuteSQL | `9047` |
| `DREMIO_TABLE` | tDremioOutput | `my_catalog.my_schema.test_output` |
| `DREMIO_SQL` | tDremioExecuteSQL | `SELECT 1` |
