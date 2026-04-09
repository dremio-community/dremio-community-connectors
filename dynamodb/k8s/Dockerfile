# =============================================================================
# Dremio Amazon DynamoDB Connector — Custom Image Build
#
# Multi-stage build:
#   Stage 1 (dremio-base):  Pulls the Dremio Enterprise image to extract JARs
#   Stage 2 (builder):      Maven build using those JARs as compile-time deps
#   Stage 3 (final):        Clean Dremio Enterprise image + the plugin JAR added
#
# The AWS SDK v2 is downloaded from Maven Central automatically during
# `mvn package` — no extra install_jar step needed.
#
# Build:
#   docker build \
#     --build-arg DREMIO_IMAGE=dremio/dremio-ee \
#     --build-arg DREMIO_TAG=26.0.5 \
#     -t your-registry/dremio-with-dynamodb:26.0.5 .
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

ARG DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARG ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
ARG CALCITE_VER=1.22.0-202501311721330426-8b30dab9

COPY --from=dremio-base /opt/dremio/jars/ /opt/dremio/jars/

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
    install_jar $JARS/dremio-connector-${DREMIO_VER}.jar                     com.dremio          dremio-connector             $DREMIO_VER; \
    install_jar $JARS/dremio-sabot-vector-tools-${DREMIO_VER}.jar            com.dremio          dremio-sabot-vector-tools    $DREMIO_VER; \
    install_jar $JARS/dremio-services-namespace-${DREMIO_VER}.jar            com.dremio          dremio-services-namespace    $DREMIO_VER; \
    install_jar $JARS/dremio-sabot-logical-${DREMIO_VER}.jar                 com.dremio          dremio-sabot-logical         $DREMIO_VER; \
    install_jar $JARS/dremio-common-core-${DREMIO_VER}.jar                   com.dremio          dremio-common-core           $DREMIO_VER; \
    install_jar $JARS/dremio-services-datastore-${DREMIO_VER}.jar            com.dremio          dremio-services-datastore    $DREMIO_VER; \
    install_jar $TP/arrow-vector-${ARROW_VER}.jar                            org.apache.arrow    arrow-vector                 $ARROW_VER; \
    install_jar $TP/arrow-memory-core-${ARROW_VER}.jar                       org.apache.arrow    arrow-memory-core            $ARROW_VER; \
    install_jar $TP/calcite-core-${CALCITE_VER}.jar                          org.apache.calcite  calcite-core                 $CALCITE_VER; \
    install_jar $TP/calcite-linq4j-${CALCITE_VER}.jar                        org.apache.calcite  calcite-linq4j               $CALCITE_VER; \
    install_jar $TP/guava-33.4.0-jre.jar                                     com.google.guava    guava                        33.4.0-jre; \
    install_jar $TP/javax.inject-1.jar                                       javax.inject        javax.inject                 1; \
    echo "All JARs installed."

WORKDIR /build
COPY . .
RUN mvn package -DskipTests -q && \
    echo "Build complete:" && \
    ls -lh target/dremio-dynamodb-connector-*-plugin.jar

# ---------------------------------------------------------------------------
# Stage 3: Final image — clean Dremio Enterprise + our plugin JAR
# ---------------------------------------------------------------------------
FROM ${DREMIO_IMAGE}:${DREMIO_TAG}

# The AWS SDK is shaded/relocated inside the fat plugin JAR — no second JAR needed.
COPY --from=builder /build/target/dremio-dynamodb-connector-*-plugin.jar \
     /opt/dremio/jars/3rdparty/

RUN ls -lh /opt/dremio/jars/3rdparty/dremio-dynamodb-connector-*.jar
