# Installation Guide — Dremio Vector UDF

## Requirements

- Dremio 24.x or later
- Java 11+ (for building)
- Maven 3.8+ (for building)
- Docker (for the quick-install path)

## Option 1: Docker Quick Install

```bash
./install.sh --container try-dremio
```

This builds the JAR, copies it into the container, and restarts Dremio.

## Option 2: Manual Install

1. **Build the JAR:**
   ```bash
   mvn package -DskipTests
   ```
   Output: `target/dremio-vector-udf-1.0.0-SNAPSHOT.jar`

2. **Copy to Dremio:**
   ```bash
   # Docker
   docker cp target/dremio-vector-udf-1.0.0-SNAPSHOT.jar \
     try-dremio:/opt/dremio/jars/3rdparty/

   # Or for a bare-metal / VM install
   cp target/dremio-vector-udf-1.0.0-SNAPSHOT.jar \
     /opt/dremio/jars/3rdparty/
   ```

3. **Restart Dremio:**
   ```bash
   docker restart try-dremio
   # or: systemctl restart dremio
   ```

4. **Verify:**
   ```sql
   SELECT COSINE_SIMILARITY('[1.0, 0.0]', '[0.0, 1.0]')
   -- Expected: 0.0  (orthogonal vectors)

   SELECT L2_DISTANCE('[0.0, 0.0]', '[3.0, 4.0]')
   -- Expected: 5.0

   SELECT VECTOR_DIMS('[0.1, 0.2, 0.3]')
   -- Expected: 3
   ```

## Option 3: Kubernetes

See [k8s/](k8s/) for Helm init-container and custom image deployment patterns (same as other community connectors).

## Troubleshooting

**Function not found after restart:**
- Confirm the JAR is in `/opt/dremio/jars/3rdparty/` (not `/opt/dremio/jars/`)
- Check Dremio logs: `docker logs try-dremio | grep -i "vector\|udf"`
- Ensure `sabot-module.conf` is inside the JAR: `jar tf dremio-vector-udf-*.jar | grep sabot`

**Dimension mismatch error:**
- Both vectors must have the same number of elements
- Use `VECTOR_DIMS(v)` to inspect dimension counts

**Slow performance on large tables:**
- Use `L2_DISTANCE_SQUARED` instead of `L2_DISTANCE` for ranking (avoids `sqrt`)
- For cosine similarity on pre-normalized vectors, `DOT_PRODUCT` is fastest
- Pre-filter rows with a `WHERE` clause before computing distances
