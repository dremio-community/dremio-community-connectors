# Dremio Prometheus Connector

A Dremio storage plugin that exposes [Prometheus](https://prometheus.io/) as SQL tables using the Prometheus HTTP API v1. Supports open Prometheus, Basic auth, and Bearer token auth (Grafana Cloud).

## Tables

| Table | Description |
|-------|-------------|
| `metrics` | All metric names with type, help text, and unit — from `/api/v1/metadata` |
| `targets` | Active scrape targets — job, instance, health, last scrape time, error |
| `alerts` | Currently firing alerts — alert_name, state, metric_value, labels, annotations, active_at |
| `rules` | Alerting and recording rules — group_name, rule_name, rule_type, query, health, evaluation_time_seconds |
| `labels` | All label names known to Prometheus |
| `samples` | Time series data points — metric_name, labels, sample_time, sample_value. Fetches the configured PromQL expression over the lookback window. |

## Requirements

- Dremio 26.x
- Prometheus 2.x (HTTP API v1)

## Install

```bash
./install.sh --docker try-dremio --prebuilt   # Docker, pre-built JAR
./install.sh --docker try-dremio --build      # Docker, build from source
./install.sh --local /opt/dremio --prebuilt   # bare-metal
./install.sh --k8s dremio-0 --prebuilt        # Kubernetes
```

## Configuration

| Field | Default | Description |
|-------|---------|-------------|
| Prometheus URL | `http://localhost:9090` | Base URL of the Prometheus server |
| Username | _(optional)_ | Username for Basic auth |
| Password | _(optional)_ | Password for Basic auth |
| Bearer Token | _(optional)_ | Token for Grafana Cloud or token-protected Prometheus (takes precedence over Basic auth) |
| PromQL Query | `up` | PromQL expression for the `samples` table. Be specific to avoid fetching large data sets. |
| Lookback Hours | `1` | Hours of history to fetch for the `samples` table |
| Step (seconds) | `60` | Range query resolution — smaller values return more rows |
| Timeout | `30` | HTTP timeout in seconds |

## Example Queries

```sql
-- Target health overview
SELECT job, instance, health, last_scrape, last_error
FROM prometheus_source.targets
ORDER BY health DESC, job;

-- Count metrics by type
SELECT type, COUNT(*) AS cnt
FROM prometheus_source.metrics
GROUP BY type ORDER BY cnt DESC;

-- Find all counter metrics
SELECT metric_name, help
FROM prometheus_source.metrics
WHERE type = 'counter'
ORDER BY metric_name;

-- Firing alerts
SELECT alert_name, state, labels, annotations, active_at
FROM prometheus_source.alerts
WHERE state = 'firing'
ORDER BY active_at;

-- All labels
SELECT label_name FROM prometheus_source.labels ORDER BY label_name;

-- Recent metric samples (uses configured PromQL + lookback window)
SELECT metric_name, labels, sample_time, sample_value
FROM prometheus_source.samples
ORDER BY sample_time DESC
LIMIT 100;

-- Average value per metric over the window
SELECT metric_name, AVG(sample_value) AS avg_val, MIN(sample_value) AS min_val, MAX(sample_value) AS max_val
FROM prometheus_source.samples
GROUP BY metric_name ORDER BY avg_val DESC;

-- Join samples with metric metadata
SELECT s.metric_name, m.type, m.help, AVG(s.sample_value) AS avg_value
FROM prometheus_source.samples s
JOIN prometheus_source.metrics m ON s.metric_name = m.metric_name
GROUP BY s.metric_name, m.type, m.help
ORDER BY avg_value DESC;

-- Alerting rules with slow evaluation
SELECT group_name, rule_name, query, evaluation_time_seconds
FROM prometheus_source.rules
WHERE rule_type = 'alerting'
ORDER BY evaluation_time_seconds DESC;

-- Cross-source: correlate Prometheus alerts with Jira incidents
SELECT a.alert_name, a.labels, j.key, j.summary, j.status
FROM prometheus_source.alerts a
JOIN jira_source.issues j ON j.labels LIKE CONCAT('%', a.alert_name, '%')
WHERE a.state = 'firing';
```

## Samples Table Tips

The `samples` table runs a range query at query time using your configured PromQL expression. To keep queries fast:

- Use a specific metric name: `samplesQuery = "node_cpu_seconds_total"`
- Filter by label: `samplesQuery = "up{job=\"kubernetes-nodes\"}"`
- Use a broad selector for multi-metric analysis: `samplesQuery = "{__name__=~\"node_.*\"}"`
- Increase `stepSeconds` (e.g. 300) to reduce row count for long lookback windows

## Building from Source

```bash
./rebuild.sh --docker try-dremio        # Detect Dremio version, rebuild, redeploy
./rebuild.sh --docker try-dremio --dry-run   # Preview version detection only
./rebuild.sh --force                    # Force rebuild even if version matches
```
