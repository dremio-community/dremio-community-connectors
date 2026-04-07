#!/usr/bin/env bash
set -euo pipefail
# Smoke test: generate sample data, import both sheets into Dremio, verify row counts.

cd "$(dirname "$0")"

JAR="jars/dremio-excel-importer.jar"
HOST="${DREMIO_HOST:-localhost}"
PORT="${DREMIO_PORT:-9047}"
USER="${DREMIO_USER:-mark}"
PASS="${DREMIO_PASS:-your_password}"
SAMPLE="sample-data/sample.xlsx"
DEST="${DREMIO_DEST:-iceberg_minio.dremio-test}"  # Dremio destination (must be an Iceberg-compatible source, not a Space)

# ---- 0. Build if JAR is missing ----
if [ ! -f "$JAR" ]; then
  echo "JAR not found — building..."
  bash build.sh
fi

# ---- 1. Generate sample data ----
echo ""
echo "Step 1: Generating sample data..."
java -cp "$JAR" com.dremio.community.excel.util.SampleDataGenerator "$SAMPLE"

# ---- 2. List sheets ----
echo ""
echo "Step 2: List sheets..."
java -jar "$JAR" --file "$SAMPLE" --list-sheets

# ---- 3. Import Sales Orders ----
echo ""
echo "Step 3: Import 'Sales Orders' sheet..."
java -jar "$JAR" \
  --file "$SAMPLE" \
  --sheet "Sales Orders" \
  --dest "${DEST}.excel_test_sales_orders" \
  --host "$HOST" --port "$PORT" \
  --user "$USER" --password "$PASS" \
  --overwrite --yes

# ---- 4. Import Employees ----
echo ""
echo "Step 4: Import 'Employees' sheet..."
java -jar "$JAR" \
  --file "$SAMPLE" \
  --sheet "Employees" \
  --dest "${DEST}.excel_test_employees" \
  --host "$HOST" --port "$PORT" \
  --user "$USER" --password "$PASS" \
  --overwrite --yes

# ---- 5. Verify via REST API ----
echo ""
echo "Step 5: Verifying row counts via Dremio SQL API..."
TOKEN=$(curl -s -X POST "http://${HOST}:${PORT}/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\":\"${USER}\",\"password\":\"${PASS}\"}" | \
  python3 -c "import sys,json; print(json.load(sys.stdin)['token'])")

run_sql() {
  local SQL="$1"
  local JOB_ID
  JOB_ID=$(curl -s -X POST "http://${HOST}:${PORT}/api/v3/sql" \
    -H "Content-Type: application/json" \
    -H "Authorization: _dremio${TOKEN}" \
    -d "{\"sql\":\"${SQL}\"}" | \
    python3 -c "import sys,json; print(json.load(sys.stdin)['id'])")

  for i in $(seq 1 20); do
    sleep 1
    STATE=$(curl -s "http://${HOST}:${PORT}/api/v3/job/${JOB_ID}" \
      -H "Authorization: _dremio${TOKEN}" | \
      python3 -c "import sys,json; print(json.load(sys.stdin)['jobState'])")
    [ "$STATE" = "COMPLETED" ] && break
    [ "$STATE" = "FAILED" ] && echo "Query FAILED: $SQL" && exit 1
  done

  curl -s "http://${HOST}:${PORT}/api/v3/job/${JOB_ID}/results" \
    -H "Authorization: _dremio${TOKEN}" | \
    python3 -c "import sys,json; rows=json.load(sys.stdin)['rows']; print(rows[0][list(rows[0].keys())[0]] if rows else 'no rows')"
}

SALES_COUNT=$(run_sql "SELECT COUNT(*) FROM ${DEST}.excel_test_sales_orders")
EMP_COUNT=$(run_sql "SELECT COUNT(*) FROM ${DEST}.excel_test_employees")

echo ""
echo "Results:"
echo "  Sales Orders: ${SALES_COUNT} rows (expected: 10)"
echo "  Employees:    ${EMP_COUNT} rows (expected: 5)"

if [ "$SALES_COUNT" = "10" ] && [ "$EMP_COUNT" = "5" ]; then
  echo ""
  echo "All tests PASSED ✓"
else
  echo ""
  echo "FAILED — row counts do not match expected values"
  exit 1
fi
