# jars/

Pre-built JARs go here after running `./install.sh` or `mvn package -DskipTests`.

The Pinot connector requires two JARs deployed to `/opt/dremio/jars/3rdparty/`:

1. `dremio-pinot-connector-1.0.0-SNAPSHOT-plugin.jar` — the connector plugin
2. `pinot-jdbc-client-1.4.0.jar` — the Apache Pinot JDBC driver (no-spi variant)

`install.sh` builds both and deploys them automatically.
