# Deploying the Dremio Apache Pinot Connector on Kubernetes

## Overview

Two deployment patterns are supported:

| Pattern | When to use |
|---|---|
| **Custom image** | You control the Docker registry; want the connector baked into the image |
| **Init container** | You use the stock Dremio image; want to inject the connector at pod start |

## Prerequisites

- Kubernetes cluster with Dremio installed via Helm
- Docker registry accessible from your cluster
- `kubectl`, `helm`, `docker`, and `mvn` on your workstation

## Pattern 1: Custom Dremio Image

### Build and push

```bash
./k8s/build.sh \
  --registry myregistry.io/myorg \
  --dremio-tag 26.0.5 \
  --push
```

### Deploy

```bash
helm upgrade --install dremio dremio/dremio \
  -f k8s/helm-values-custom-image.yaml
```

Edit `helm-values-custom-image.yaml` to set your registry and tag first.

## Pattern 2: Init Container

### Build and push the init image only

```bash
cd ..
mvn package -DskipTests
docker build \
  -f k8s/Dockerfile.init \
  -t myregistry.io/myorg/dremio-pinot-connector-init:1.0.0-SNAPSHOT \
  .
docker push myregistry.io/myorg/dremio-pinot-connector-init:1.0.0-SNAPSHOT
```

### Deploy

Edit `helm-values-init-container.yaml` to set your registry and image name, then:

```bash
helm upgrade --install dremio dremio/dremio \
  -f helm-values-init-container.yaml
```

## Verifying the Deployment

```bash
# Check the connector JAR is present
kubectl exec -n <namespace> <dremio-coordinator-pod> -- \
  ls /opt/dremio/jars/3rdparty/ | grep pinot

# Check Dremio logs for successful plugin registration
kubectl logs -n <namespace> <dremio-coordinator-pod> | grep -i pinot
```

## Upgrading the Connector

```bash
# Rebuild
mvn package -DskipTests

# Rebuild and push images
./k8s/build.sh --registry myregistry.io/myorg --push

# Rolling restart to pick up new image
kubectl rollout restart deployment/dremio-coordinator -n <namespace>
kubectl rollout restart statefulset/dremio-executor -n <namespace>
```
