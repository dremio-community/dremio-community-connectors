# =============================================================================
# Dremio ClickHouse Connector — Custom Image Build
#
# Multi-stage build:
#   Stage 1 (dremio-base):  Pulls the Dremio Enterprise image to extract JARs
#   Stage 2 (builder):      Maven build using those JARs as compile-time deps
#   Stage 3 (final):        Clean Dremio Enterprise image + the plugin JARs added
#
# ClickHouse requires TWO JARs in /opt/dremio/jars/3rdparty/:
#   1. dremio-clickhouse-connector-*-plugin.jar  (the connector plugin)
#   2. clickhouse-jdbc-0.4.6-all.jar             (the JDBC driver, NOT shaded in)
#
# Build:
#   docker build \
#     --build-arg DREMIO_IMAGE=dremio/dremio-ee \
#     --build-arg DREMIO_TAG=26.0.5 \
#     -t your-registry/dremio-with-clickhouse:26.0.5 .
#
# If your Enterprise image is in a private registry, log in first:
#   docker login your-registry.example.com
# =============================================================================

ARG DREMIO_IMAGE=dremio/dremio-ee
ARG DREMIO_TAG=26.0.5

# ---------------------------------------------------------------------------
# Stage 1: Dremio base — used only to copy JARs into the Maven build stage
# ---------------------------------------------------------------------------
FROM ${DREMIO_IMAGE}:${DREMIO_TAG} AS dremio-base

# ---------------------------------------------------------------------------
# Stage 2: Maven build
# ---------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-11 AS builder

# Version strings — update these if upgrading Dremio
ARG DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARG ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
ARG CALCITE_VER=1.22.0-202501311721330426-8b30dab9

# Copy the Dremio JARs needed for compilation
COPY --from=dremio-base /opt/dremio/jars/ /opt/dremio/jars/

# Install all required JARs into the Maven local repository.
# ClickHouse connector uses the JDBC plugin API and requires several extra JARs
# (dremio-services-options, dremio-ce-jdbc-plugin, dremio-ce-jdbc-fetcher-api)
# in addition to the standard plugin API JARs.
RUN set -e; \
    install_jar() { \
      mvn install:install-file \
        -Dfile="$1" -DgroupId="$2" -DartifactId="$3" -Dversion="$4" \
        -Dpackaging=jar -q && echo "  Installed $2:$3:$4"; \
    }; \
    JARS=/opt/dremio/jars; \
    TP=$JARS/3rdparty; \
    install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}.jar                  com.dremio          dremio-sabot-kernel          $DREMIO_VER; \
    install_jar $JARS/dremio-common-${DREMIO_VER}.jar                        com.dremio          dremio-common                $DREMIO_VER; \
    install_jar $JARS/dremio-plugin-common-${DREMIO_VER}.jar                 com.dremio.plugin   dremio-plugin-common         $DREMIO_VER; \
    install_jar $JARS/dremio-sabot-kernel-${DREMIO_VER}-proto.jar            com.dremio          dremio-sabot-kernel-proto    $DREMIO_VER; \
    install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar            com.dremio          dremio-sabot-vector-tools    $DREMIO_VER; \
    install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar            com.dremio          dremio-services-namespace    $DREMIO_VER; \
    install_jar $JARS/dremio-connector-${DREMIO_VER}.jar                     com.dremio          dremio-connector             $DREMIO_VER; \
    install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar                 com.dremio          dremio-sabot-logical         $DREMIO_VER; \
    install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar                   com.dremio          dremio-common-core           $DREMIO_VER; \
    install_jar $JARS/dremio-services-credentials-${DREMIO_VER}.jar          com.dremio.services dremio-services-credentials  $DREMIO_VER; \
    install_jar $JARS/dremio-services-datastore-${DREMIO_VER}.jar            com.dremio          dremio-services-datastore    $DREMIO_VER; \
    install_jar $JARS/dremio-services-options-${DREMIO_VER}.jar              com.dremio          dremio-services-options      $DREMIO_VER; \
    install_jar $JARS/dremio-ce-jdbc-plugin-${DREMIO_VER}.jar                com.dremio.plugins  dremio-ce-jdbc-plugin        $DREMIO_VER; \
    install_jar $JARS/dremio-ce-jdbc-fetcher-api-${DREMIO_VER}.jar           com.dremio.plugins  dremio-ce-jdbc-fetcher-api   $DREMIO_VER; \
    install_jar $TP/arrow-vector-${ARROW_VER}.jar                            org.apache.arrow    arrow-vector                 $ARROW_VER; \
    install_jar $TP/arrow-memory-core-${ARROW_VER}.jar                       org.apache.arrow    arrow-memory-core            $ARROW_VER; \
    install_jar $TP/calcite-core-${CALCITE_VER}.jar                          org.apache.calcite  calcite-core                 $CALCITE_VER; \
    install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar                        org.apache.calcite  calcite-linq4j               $CALCITE_VER; \
    install_jar $TP/guava-33.4.0-jre.jar                                     com.google.guava    guava                        33.4.0-jre; \
    install_jar $TP/javax.inject-1.jar                                       javax.inject        javax.inject                 1; \
    echo "All JARs installed."

# Copy source and build the plugin.
# After packaging, copy the ClickHouse JDBC driver JAR into target/ so that
# Dockerfile.init and Stage 3 can access both JARs from a single location.
WORKDIR /build
COPY . .
RUN mvn package -DskipTests -q && \
    cp ~/.m2/repository/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar target/ && \
    echo "Build complete:" && \
    ls -lh target/dremio-clickhouse-connector-*-plugin.jar target/clickhouse-jdbc-*-all.jar

# ---------------------------------------------------------------------------
# Stage 3: Final image — clean Dremio Enterprise + both plugin JARs
# ---------------------------------------------------------------------------
FROM ${DREMIO_IMAGE}:${DREMIO_TAG}

# Copy the plugin JAR and the ClickHouse JDBC driver (deployed separately —
# NOT shaded into the plugin JAR).
COPY --from=builder /build/target/dremio-clickhouse-connector-*-plugin.jar /opt/dremio/jars/3rdparty/
COPY --from=builder /root/.m2/repository/com/clickhouse/clickhouse-jdbc/0.4.6/clickhouse-jdbc-0.4.6-all.jar /opt/dremio/jars/3rdparty/

# Verify both JARs landed correctly
RUN ls -lh /opt/dremio/jars/3rdparty/dremio-clickhouse-connector-*.jar \
           /opt/dremio/jars/3rdparty/clickhouse-jdbc-*-all.jar
