---
name: Bug report
about: Report a problem with a connector
title: '[connector-name] Brief description of the bug'
labels: bug
assignees: ''
---

## Connector

Which connector is affected?
- [ ] ClickHouse
- [ ] Apache Kafka
- [ ] Apache Cassandra
- [ ] Apache Hudi
- [ ] Delta Lake

## Environment

| Item | Value |
|------|-------|
| Dremio version | e.g. 26.0.5 |
| Connector JAR | e.g. `dremio-clickhouse-connector-1.0.0-SNAPSHOT-plugin.jar` |
| Source version | e.g. ClickHouse 24.3, Kafka 3.7 |
| Deployment | Docker / bare-metal / Kubernetes |

## What happened?

<!-- Describe the bug clearly -->

## What did you expect?

<!-- What should have happened instead? -->

## Steps to reproduce

```bash
# Commands or queries that trigger the bug
```

## Error message / logs

```
# Paste relevant Dremio log output here
# docker logs try-dremio | grep -i error | tail -50
```

## Additional context

<!-- Any other relevant information: schema, data samples, network topology -->
