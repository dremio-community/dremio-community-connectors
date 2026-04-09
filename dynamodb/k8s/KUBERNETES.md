# Dremio DynamoDB Connector — Kubernetes Install Guide

This guide covers deploying the DynamoDB connector to **Dremio Enterprise** running on
Kubernetes, using the official `dremio_v2` Helm chart from
[dremio/dremio-cloud-tools](https://github.com/dremio/dremio-cloud-tools).

Two deployment approaches are provided. Choose one:

| Approach | When to use |
|---|---|
| **A — Custom Image** | Recommended for production. Simple, reliable, no pod startup overhead. |
| **B — Init Container** | Prefer this if your team's policy prohibits maintaining a custom Dremio image. |

---

## Prerequisites

- `docker` CLI, logged into your container registry
- `helm` 3.x
- `kubectl` pointing at your cluster
- Dremio Enterprise image accessible from your cluster (private registry or Dremio's registry)
- Your base Dremio Helm values file

---

## Approach A — Custom Dremio Image (Recommended)

The plugin JAR is baked directly into a custom image that extends Dremio Enterprise.
The multi-stage `Dockerfile` handles the entire build: it extracts Dremio's internal
JARs for compilation, runs Maven (which downloads the DataStax driver from Maven Central
automatically), then layers the single fat plugin JAR onto a clean Dremio image.

### Step 1: Build and push the image

From the `dremio-dynamodb-connector/` directory:

```bash
chmod +x k8s/build.sh

k8s/build.sh \
  --registry your-registry.example.com \
  --dremio-image dremio/dremio-ee \
  --dremio-tag 26.0.5 \
  --push
```

This builds `your-registry.example.com/dremio-with-dynamodb:26.0.5` and pushes it.

### Step 2: Deploy with Helm

```bash
helm upgrade --install dremio charts/dremio_v2 \
  -f your-base-values.yaml \
  -f k8s/helm-values-custom-image.yaml \
  -n dremio
```

Edit `helm-values-custom-image.yaml` first to set your registry and image name.

---

## Approach B — Init Container

The standard Dremio Enterprise image is used unchanged. A small init container image
(based on `alpine`) holds only the plugin JAR. Two init containers run at pod startup:
one to seed an `emptyDir` volume with the existing Dremio 3rdparty JARs, and one to
add the DynamoDB plugin JAR into that volume.

### Step 1: Build just the init container image

```bash
# First build the plugin JAR (requires Dremio JARs in local Maven repo — see INSTALL.md)
mvn package -DskipTests -q

# Then build the init container image
k8s/build.sh \
  --registry your-registry.example.com \
  --init-only \
  --push
```

This builds `your-registry.example.com/dremio-dynamodb-plugin-init:1.0.0` and pushes it.

### Step 2: Deploy with Helm

Edit `k8s/helm-values-init-container.yaml` and replace the two placeholder image names:
- `copy-3rdparty` init container: set to your actual Dremio Enterprise image
- `dynamodb-plugin` init container: set to the init image you just pushed

```bash
helm upgrade --install dremio charts/dremio_v2 \
  -f your-base-values.yaml \
  -f k8s/helm-values-init-container.yaml \
  -n dremio
```

---

## Registering the DynamoDB Source

After Dremio is running with the plugin installed, register a DynamoDB source via the
Dremio REST API (or the UI under **Sources → + → DynamoDB**):

```bash
# Get an auth token
TOKEN=$(curl -s -X POST https://dremio.example.com/apiv2/login \
  -H 'Content-Type: application/json' \
  -d '{"userName":"admin","password":"YOUR_PASSWORD"}' \
  | jq -r '.token')

# Create the DynamoDB source
curl -s -X POST https://dremio.example.com/api/v3/catalog \
  -H "Authorization: _dremio${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{
    "entityType": "source",
    "name": "dynamodb",
    "type": "DYNAMODB",
    "config": {
      "contactPoints": "dynamodb.example.com",
      "port": 9042,
      "username": "dynamodb",
      "password": "dynamodb-password",
      "keyspace": "mykeyspace",
      "localDatacenter": "datacenter1"
    }
  }'
```

---

## Verifying the Install

```bash
# Check that the plugin JAR is present in all pods
kubectl get pods -n dremio -l role=dremio-cluster-pod \
  -o name | xargs -I{} kubectl exec -n dremio {} -- \
  ls /opt/dremio/jars/3rdparty/dremio-dynamodb-connector-*.jar

# Run a quick smoke test
curl -s -X POST https://dremio.example.com/api/v3/sql \
  -H "Authorization: _dremio${TOKEN}" \
  -H 'Content-Type: application/json' \
  -d '{"sql":"SELECT * FROM dynamodb.mykeyspace.mytable LIMIT 5"}' \
  | jq '.id'
```

---

## Upgrading Dremio

When upgrading Dremio Enterprise, rebuild the plugin against the new version:

```bash
# Update DREMIO_VER in k8s/Dockerfile to the new version string, then:
k8s/build.sh \
  --registry your-registry.example.com \
  --dremio-tag NEW_VERSION \
  --push

helm upgrade dremio charts/dremio_v2 \
  -f your-base-values.yaml \
  -f k8s/helm-values-custom-image.yaml \
  -n dremio
```

Or use `rebuild.sh` for Docker/Kubernetes in-place upgrades (see README.md).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Plugin JAR not found in pod | Init container failed or image not pulled | `kubectl logs -n dremio <pod> -c dynamodb-plugin` |
| `ClassNotFoundException` for DynamoDB classes | JAR deployed to wrong path | Verify JAR is in `/opt/dremio/jars/3rdparty/`, not `/opt/dremio/jars/` |
| Connection refused to DynamoDB | Wrong contact point or port | Verify `contactPoints` and `port` (default 9042) in source config |
| Source shows as unhealthy after deploy | Plugin loaded but DynamoDB unreachable | Check network policy allows egress to DynamoDB port 9042 |
| `UnsupportedOperationException` on timestamp | Wrong Dremio version JAR compiled against | Rebuild with `DREMIO_VER` matching your cluster's exact version string |
| Authentication failure | Wrong credentials or auth not enabled | Verify `username`/`password` match DynamoDB's authenticator config |
