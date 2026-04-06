# Dremio Delta Lake Connector — Kubernetes Install Guide

This guide covers deploying the Delta Lake connector to **Dremio Enterprise** running on
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
JARs for compilation, runs Maven, then layers the plugin JAR onto a clean Dremio image.

### Step 1: Build and push the image

From the `dremio-delta-connector/` directory:

```bash
chmod +x k8s/build.sh

k8s/build.sh \
  --registry your-registry.example.com \
  --dremio-image dremio/dremio-ee \
  --dremio-tag 26.0.5 \
  --full-only \
  --push
```

This produces and pushes: `your-registry.example.com/dremio-with-delta:26.0.5`

> If your Dremio Enterprise image is in a private registry, `docker login` to it first.

### Step 2: Configure Helm values

Edit `k8s/helm-values-custom-image.yaml` and set your image name and registry secret
if applicable, then apply:

```bash
helm upgrade --install dremio charts/dremio_v2 \
  -f your-base-values.yaml \
  -f k8s/helm-values-custom-image.yaml \
  -n dremio
```

### Step 3: Verify

```bash
# Check all pods come up healthy
kubectl get pods -n dremio

# Confirm the JAR is present in a coordinator pod
kubectl exec -n dremio deployment/dremio-coordinator -- \
  ls /opt/dremio/jars/3rdparty/dremio-delta-connector-*.jar

# Check Dremio logs for classpath scanning
kubectl logs -n dremio deployment/dremio-coordinator \
  | grep -i "delta\|sabot-module"
```

Open the Dremio UI → Sources → Add Source → look for **Delta Lake**.

### Upgrading Dremio

When upgrading Dremio to a new version, rebuild the image with the new tag:

```bash
k8s/build.sh \
  --registry your-registry.example.com \
  --dremio-tag 26.1.0 \    # new version
  --full-only \
  --push
```

Then update `imageTag` in your Helm values and `helm upgrade`.

---

## Approach B — Init Container

No custom Dremio image is built. Instead, two init containers run before Dremio starts:

1. `copy-3rdparty` — copies the existing Dremio 3rdparty JARs into an emptyDir volume
2. `delta-plugin` — copies the Delta plugin JAR into the same volume

The volume is then mounted over `/opt/dremio/jars/3rdparty/` so Dremio sees both
the original JARs and the new plugin.

### Step 1: Build and push the plugin-only init image

The init image is a small Alpine image (~5 MB + the JAR) containing only the plugin.
You need to build the JAR locally first.

#### 1a. Install Dremio JARs into your local Maven repo

Extract the JARs from a running Dremio Enterprise container or installation:

```bash
DREMIO_VER=26.0.5-202509091642240013-f5051a07
ARROW_VER=18.1.1-20250709131625-66bbaf1fd7-dremio
HADOOP_VER=3.3.6-dremio-202507241551560856-75923ad5

# Example: pull from a pod in your cluster
kubectl cp -n dremio \
  dremio-coordinator-0:/opt/dremio/jars/dremio-sabot-kernel-${DREMIO_VER}.jar \
  ./dremio-sabot-kernel-${DREMIO_VER}.jar

# Install each JAR (repeat for all JARs listed in INSTALL.md Step 1)
mvn install:install-file \
  -Dfile=dremio-sabot-kernel-${DREMIO_VER}.jar \
  -DgroupId=com.dremio \
  -DartifactId=dremio-sabot-kernel \
  -Dversion=${DREMIO_VER} \
  -Dpackaging=jar
```

#### 1b. Build the plugin JAR

```bash
cd dremio-delta-connector
mvn package -DskipTests
```

#### 1c. Build and push the init image

```bash
k8s/build.sh \
  --registry your-registry.example.com \
  --plugin-version 1.0.0 \
  --init-only \
  --push
```

This produces and pushes: `your-registry.example.com/dremio-delta-plugin-init:1.0.0`

### Step 2: Configure Helm values

Edit `k8s/helm-values-init-container.yaml`:

- Replace `your-registry.example.com/dremio-delta-plugin-init:1.0.0` with your actual image
- Replace `dremio/dremio-ee:26.0.5` in the `copy-3rdparty` init containers with your actual Enterprise image

Then apply:

```bash
helm upgrade --install dremio charts/dremio_v2 \
  -f your-base-values.yaml \
  -f k8s/helm-values-init-container.yaml \
  -n dremio
```

### Step 3: Verify

```bash
# Watch init containers complete during pod startup
kubectl get pods -n dremio -w

# Check init container logs
kubectl logs -n dremio dremio-coordinator-0 -c copy-3rdparty
kubectl logs -n dremio dremio-coordinator-0 -c delta-plugin

# Confirm the JAR landed correctly
kubectl exec -n dremio dremio-coordinator-0 -- \
  ls /opt/dremio/jars/3rdparty/dremio-delta-connector-*.jar
```

### Upgrading the plugin

Rebuild the init image with a new tag and update the image reference in your Helm values:

```bash
k8s/build.sh --registry your-registry.example.com --plugin-version 1.0.1 --init-only --push
# Update helm-values-init-container.yaml: image tag 1.0.0 → 1.0.1
helm upgrade dremio charts/dremio_v2 -f your-base-values.yaml -f k8s/helm-values-init-container.yaml -n dremio
```

---

## Enterprise Plugin Enablement

Depending on your Dremio Enterprise version, you may need to explicitly allow
third-party plugins. Add this to your `dremio.conf` ConfigMap if the source
doesn't appear after restart:

```hocon
plugins.enabled = true
```

Check with Dremio support if this setting is required for your specific version.

---

## Troubleshooting

**Pods stuck in `Init:0/2`**
- Check init container logs: `kubectl logs -n dremio <pod> -c copy-3rdparty`
- Verify the init images are accessible from the cluster: `kubectl describe pod -n dremio <pod>`

**Source doesn't appear in Add Source after restart**
- Confirm the JAR is present: `kubectl exec -n dremio <pod> -- ls /opt/dremio/jars/3rdparty/ | grep delta`
- Check Dremio server logs: `kubectl logs -n dremio deployment/dremio-coordinator | grep -i delta`
- Check if Enterprise plugin loading needs to be enabled (see above)

**Init container image pull error**
- Ensure the registry secret is referenced in your Helm values via `imagePullSecrets`
- Verify the image was successfully pushed: `docker manifest inspect your-registry.../dremio-delta-plugin-init:1.0.0`

**`ClassNotFoundException` for Delta classes at runtime**
- The coordinator may have the JAR but executors may not. Confirm the init containers
  run on executor pods too (both `coordinator.extraInitContainers` and
  `executor.extraInitContainers` must be set).

**delta-standalone API errors**
- This connector uses delta-standalone 0.6.0. The `Metadata` constructor takes `StructType`
  as the last argument. Use `DeltaScan` (not `Scan`) and note that `getChanges()` returns
  a plain `java.util.Iterator` (not `AutoCloseable`).
