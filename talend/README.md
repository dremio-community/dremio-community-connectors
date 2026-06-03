# Dremio Talend Connector

A native, high-performance Talend Studio component for extracting data from Dremio (OSS, Enterprise Software, and Dremio Cloud). 

Unlike standard generic JDBC drivers, this native component utilizes **Apache Arrow Flight RPC**, providing extreme performance by bypassing the coordinator and streaming partitions in parallel directly from Dremio's executor nodes.

## Features

- **Arrow Flight Protocol**: Sub-second latency data extraction by avoiding JDBC serialization bottlenecks.
- **Parallel Partitioning**: Native Talend `@Split` logic seamlessly maps to Dremio `FlightEndpoint` tickets.
- **Strict Data Type Mapping**: Dremio Arrow Vectors (`Float8Vector`, `TimeStampVector`, etc.) are mapped exactly to native Talend Studio types, preventing downstream casting errors.
- **Dremio Cloud Support**: Native SSL/TLS toggle (`Location.forGrpcTls`) for secure Cloud endpoints.
- **Health Checks**: Live Studio "Test Connection" button powered by TCK `@HealthCheck`.

## Prerequisites

- Talend Studio (v8.x+)
- Dremio OSS / Software / Cloud (v20.x+)
- Personal Access Token (PAT) for Dremio Software/Cloud authentication (or standard username for OSS).

## Installation

### Building the Component Archive (.car)

This project uses the Talend Component Kit (TCK) and the Maven Wrapper, meaning you do not need Maven installed globally to build it.

```bash
# Compile and build the Talend component archive (.car)
./build.sh
```

This script runs `./mvnw clean install` and packages the project.

### Deploying to Talend Studio

1. Navigate to the `target/` directory.
2. Locate the generated `.car` file (e.g., `dremio-talend-connector-1.0.0-SNAPSHOT.car`).
3. Open Talend Studio.
4. Drag and drop the `.car` file directly into the Talend Studio workspace, or install it via the Component Manager.
5. The `tDremioInput` component will now be available in your Palette!

## Configuration

### Dremio Connection (DataStore)

Configure your connection parameters in the Talend metadata or directly on the component:

- **Host**: Dremio Coordinator Host (e.g., `localhost`, `dremio.company.com`, `data.dremio.cloud`).
- **Port**: Arrow Flight Port (Default is `32010` for OSS/Software, `443` for Cloud).
- **Enable SSL/TLS**: **Must be checked** if connecting to Dremio Cloud or a secure enterprise deployment.
- **Username**: Only required for older OSS versions; leave blank if using a PAT.
- **Personal Access Token**: Your secure Dremio PAT.

### Query Execution (DataSet)

Provide the SQL query to execute:

- **SQL Query**: Standard Dremio SQL (e.g., `SELECT * FROM "Samples"."samples.dremio.com"."NYC-taxi-trips"`).

## Supported Arrow Data Types

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

- **Java 8**: This connector is compiled targeting Java 8 for maximum Talend Studio compatibility. Arrow Flight 11.0.0 is used as the most recent version that is stable under a Java 8 runtime. If your Talend environment runs Java 11+, you may upgrade the `arrow.version` in `pom.xml` for additional features.

## Programmatic Testing

You can run the end-to-end integration test locally without Talend Studio. The TCK test harness will execute the pipeline, perform the secure Arrow Flight handshake, and validate the extraction mapping.

```bash
# Run the integration test suite
./mvnw test
```

*Note: The `DremioCloudTest` uses a dummy PAT and is expected to safely fail at the authentication step. You can edit the test locally to inject your real PAT if you wish to see live data extraction.*
