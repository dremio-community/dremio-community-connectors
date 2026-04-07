# =============================================================================
# Dremio + Apache Pinot Connector — Custom Image
# Multi-stage build:
#   Stage 1: install Maven deps from Dremio image
#   Stage 2: build the connector JAR
#   Stage 3: create the final Dremio image with the JAR pre-installed
# =============================================================================

ARG DREMIO_IMAGE=dremio/dremio-oss
ARG DREMIO_TAG=26.0.5

# -- Stage 1: Extract Dremio JARs needed for compilation ---------------------
FROM ${DREMIO_IMAGE}:${DREMIO_TAG} AS dremio-base

# -- Stage 2: Build the connector ---------------------------------------------
FROM maven:3.9-eclipse-temurin-11 AS builder

WORKDIR /build
COPY . .

# Install Dremio JARs into local Maven repo
COPY --from=dremio-base /opt/dremio/jars/dremio-sabot-kernel-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-common-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-services-namespace-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-connector-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-common-core-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-sabot-logical-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-sabot-kernel-proto-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-sabot-vector-tools-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-services-datastore-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-services-options-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-plugin-common-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-services-credentials-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-ce-jdbc-plugin-*.jar /tmp/dremio-jars/
COPY --from=dremio-base /opt/dremio/jars/dremio-ce-jdbc-fetcher-api-*.jar /tmp/dremio-jars/

RUN /build/k8s/install-jars.sh /tmp/dremio-jars && \
    mvn package -q -DskipTests

# -- Stage 3: Final Dremio image with connector installed --------------------
FROM ${DREMIO_IMAGE}:${DREMIO_TAG}

COPY --from=builder /build/target/dremio-pinot-connector-*-plugin.jar \
     /opt/dremio/jars/3rdparty/

USER root
RUN chown dremio:dremio /opt/dremio/jars/3rdparty/dremio-pinot-connector-*-plugin.jar
USER dremio
