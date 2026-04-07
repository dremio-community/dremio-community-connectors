# Dremio Apache Pinot Connector

A community connector that adds Apache Pinot as a data source in Dremio, enabling SQL queries across Pinot real-time tables alongside your data lake and other sources.

## Features

- **Full SQL pushdown** — aggregations, filters, projections, ORDER BY, and LIMIT are executed inside Pinot
- **JOINs across sources** — join Pinot tables with Iceberg, Parquet, Kafka, or any other Dremio source
- **Schema discovery** — Pinot tables appear automatically in the Dremio catalog
- **Authentication support** — username/password for secured Pinot clusters
- **TLS support** — encrypted connections via `jdbc:pinot+ssl://`
- **External query** — run raw Pinot SQL via Dremio's external query interface

## Requirements

- Dremio 26.x (community or enterprise)
- Apache Pinot 0.12+ (tested against 1.x)
- Java 11

## Quick Start

```bash
# 1. Build and install
./install.sh --docker <dremio-container>

# 2. In Dremio UI: Add Source → Apache Pinot
#    Set Controller Host and Port (default: 9000)
```

## Documentation

- [INSTALL.md](INSTALL.md) — full installation guide
- [USER_GUIDE.md](USER_GUIDE.md) — configuration reference and SQL examples
- [k8s/KUBERNETES.md](k8s/KUBERNETES.md) — Kubernetes deployment guide
