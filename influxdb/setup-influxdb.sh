#!/bin/bash
# Sets up a local InfluxDB 3 Core container with test data.
# Run this once after: docker compose up -d influxdb
set -e

CONTAINER="${INFLUXDB_CONTAINER:-influxdb3}"
HOST="http://localhost:8181"
TOKEN_FILE=".influxdb-token"

echo "=== InfluxDB 3 Setup ==="

# Wait for InfluxDB to be ready
echo "Waiting for InfluxDB to start..."
for i in $(seq 1 30); do
  if curl -sf "$HOST/health" > /dev/null 2>&1 || \
     curl -sf "$HOST/api/v3/query_sql" > /dev/null 2>&1 || \
     docker exec "$CONTAINER" influxdb3 --help > /dev/null 2>&1; then
    echo "InfluxDB is up."
    break
  fi
  sleep 2
done

# Create admin token (only needed on first run)
if [ ! -f "$TOKEN_FILE" ]; then
  echo "Creating admin token..."
  TOKEN=$(docker exec "$CONTAINER" influxdb3 create token --admin 2>&1 | grep -oE '[A-Za-z0-9_\-\.]{40,}' | head -1)
  if [ -z "$TOKEN" ]; then
    echo "Could not extract token. Check: docker exec $CONTAINER influxdb3 create token --admin"
    exit 1
  fi
  echo "$TOKEN" > "$TOKEN_FILE"
  echo "Token saved to $TOKEN_FILE"
else
  TOKEN=$(cat "$TOKEN_FILE")
  echo "Using existing token from $TOKEN_FILE"
fi

echo "Token: $TOKEN"

# Create databases
for DB in sensors metrics; do
  echo "Creating database '$DB'..."
  docker exec "$CONTAINER" influxdb3 create database "$DB" --token "$TOKEN" 2>/dev/null || true
done

# Write test data: sensors database
echo "Writing sensor data..."
NOW_S=$(date +%s)
curl -sf -X POST "$HOST/api/v3/write_lp?db=sensors" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "
temperature,location=server_room,sensor_id=S001 value=22.4,humidity=45.2 $((NOW_S - 300))000000000
temperature,location=server_room,sensor_id=S001 value=22.6,humidity=45.0 $((NOW_S - 240))000000000
temperature,location=server_room,sensor_id=S001 value=22.9,humidity=44.8 $((NOW_S - 180))000000000
temperature,location=server_room,sensor_id=S001 value=23.1,humidity=44.5 $((NOW_S - 120))000000000
temperature,location=server_room,sensor_id=S001 value=22.8,humidity=44.9 $((NOW_S - 60))000000000
temperature,location=office,sensor_id=S002 value=21.0,humidity=50.1 $((NOW_S - 300))000000000
temperature,location=office,sensor_id=S002 value=21.2,humidity=49.8 $((NOW_S - 240))000000000
temperature,location=office,sensor_id=S002 value=21.5,humidity=49.5 $((NOW_S - 180))000000000
temperature,location=office,sensor_id=S002 value=21.3,humidity=49.9 $((NOW_S - 120))000000000
temperature,location=office,sensor_id=S002 value=21.1,humidity=50.3 $((NOW_S - 60))000000000
pressure,location=server_room,sensor_id=P001 value=1013.2 $((NOW_S - 300))000000000
pressure,location=server_room,sensor_id=P001 value=1013.5 $((NOW_S - 180))000000000
pressure,location=server_room,sensor_id=P001 value=1013.1 $((NOW_S - 60))000000000
"
echo "Sensor data written."

# Write test data: metrics database
echo "Writing metrics data..."
curl -sf -X POST "$HOST/api/v3/write_lp?db=metrics" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: text/plain; charset=utf-8" \
  --data-binary "
cpu,host=web01,region=us-east usage_user=12.5,usage_system=2.1,usage_idle=85.4 $((NOW_S - 300))000000000
cpu,host=web01,region=us-east usage_user=14.2,usage_system=2.3,usage_idle=83.5 $((NOW_S - 240))000000000
cpu,host=web01,region=us-east usage_user=18.7,usage_system=3.1,usage_idle=78.2 $((NOW_S - 180))000000000
cpu,host=web01,region=us-east usage_user=15.3,usage_system=2.8,usage_idle=81.9 $((NOW_S - 120))000000000
cpu,host=web01,region=us-east usage_user=13.1,usage_system=2.0,usage_idle=84.9 $((NOW_S - 60))000000000
cpu,host=db01,region=us-west usage_user=45.2,usage_system=8.3,usage_idle=46.5 $((NOW_S - 300))000000000
cpu,host=db01,region=us-west usage_user=52.1,usage_system=9.1,usage_idle=38.8 $((NOW_S - 180))000000000
cpu,host=db01,region=us-west usage_user=48.7,usage_system=8.9,usage_idle=42.4 $((NOW_S - 60))000000000
memory,host=web01,region=us-east used_bytes=2147483648i,total_bytes=8589934592i $((NOW_S - 300))000000000
memory,host=web01,region=us-east used_bytes=2306867200i,total_bytes=8589934592i $((NOW_S - 180))000000000
memory,host=web01,region=us-east used_bytes=2415919104i,total_bytes=8589934592i $((NOW_S - 60))000000000
"
echo "Metrics data written."

echo ""
echo "=== Setup complete ==="
echo "Token: $TOKEN"
echo ""
echo "Verify with:"
echo "  curl -X POST $HOST/api/v3/query_sql \\"
echo "    -H 'Authorization: Bearer $TOKEN' \\"
echo "    -H 'Content-Type: application/json' \\"
echo "    -d '{\"db\":\"sensors\",\"q\":\"SELECT * FROM temperature LIMIT 5\",\"format\":\"json\"}'"
echo ""
echo "Add to Dremio:"
echo "  Host: $HOST"
echo "  Database: sensors  (or metrics)"
echo "  Token: $TOKEN"
