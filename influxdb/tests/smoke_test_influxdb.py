#!/usr/bin/env python3
"""
Smoke tests for the Dremio InfluxDB connector.

Prerequisites:
  1. InfluxDB 3 running: docker compose up -d influxdb
  2. Test data loaded:    ./setup-influxdb.sh
  3. Connector JAR deployed and Dremio restarted: ./install.sh
  4. InfluxDB sources added in Dremio UI:
       - Source name: influxdb_sensors  (database: sensors)
       - Source name: influxdb_metrics  (database: metrics)
  5. Metadata refreshed on both sources

Usage:
  DREMIO_TOKEN=<token> python tests/smoke_test_influxdb.py
  # or with password auth:
  python tests/smoke_test_influxdb.py
"""

import os
import sys
import json
import time
import requests

# ── Config ────────────────────────────────────────────────────────────────────
DREMIO_HOST  = os.environ.get("DREMIO_HOST", "http://localhost:9047")
DREMIO_USER  = os.environ.get("DREMIO_USER", "mark")
DREMIO_PASS  = os.environ.get("DREMIO_PASS", "Hoyasaxa7788&&**")
DREMIO_TOKEN = os.environ.get("DREMIO_TOKEN", "")

SOURCE_SENSORS = "influxdb_sensors"
SOURCE_METRICS = "influxdb_metrics"

PASS_COUNT = 0
FAIL_COUNT = 0

# ── Dremio REST helpers ───────────────────────────────────────────────────────

def get_token():
    if DREMIO_TOKEN:
        return DREMIO_TOKEN
    r = requests.post(f"{DREMIO_HOST}/apiv2/login",
                      json={"userName": DREMIO_USER, "password": DREMIO_PASS})
    r.raise_for_status()
    return r.json()["token"]

TOKEN = get_token()
HEADERS = {"Authorization": f"_dremio{TOKEN}", "Content-Type": "application/json"}


def run_query(sql, timeout=60):
    job = requests.post(f"{DREMIO_HOST}/api/v3/sql",
                        headers=HEADERS, json={"sql": sql})
    job.raise_for_status()
    job_id = job.json()["id"]

    for _ in range(timeout):
        status = requests.get(f"{DREMIO_HOST}/api/v3/job/{job_id}", headers=HEADERS)
        state = status.json().get("jobState", "")
        if state == "COMPLETED":
            rows = requests.get(f"{DREMIO_HOST}/api/v3/job/{job_id}/results?offset=0&limit=500",
                                headers=HEADERS)
            return rows.json().get("rows", [])
        if state in ("FAILED", "CANCELED"):
            raise RuntimeError(f"Query failed: {status.json().get('errorMessage', state)}")
        time.sleep(1)
    raise TimeoutError(f"Query timed out after {timeout}s: {sql}")


def check(name, condition, detail=""):
    global PASS_COUNT, FAIL_COUNT
    if condition:
        print(f"  PASS  {name}")
        PASS_COUNT += 1
    else:
        print(f"  FAIL  {name}" + (f" — {detail}" if detail else ""))
        FAIL_COUNT += 1


# ── Tests: sensors database ───────────────────────────────────────────────────

def test_sensors_source():
    print("\n[sensors] Basic table listing")
    rows = run_query(f'SELECT * FROM "{SOURCE_SENSORS}".temperature LIMIT 5')
    check("temperature table returns rows", len(rows) > 0, f"got {len(rows)}")
    if rows:
        cols = list(rows[0].keys())
        check("time column present", "time" in cols, f"cols={cols}")
        check("value column present", "value" in cols, f"cols={cols}")
        check("location tag present", "location" in cols, f"cols={cols}")
        check("sensor_id tag present", "sensor_id" in cols, f"cols={cols}")
        check("humidity field present", "humidity" in cols, f"cols={cols}")

def test_sensors_count():
    print("\n[sensors] Row counts")
    rows = run_query(f'SELECT COUNT(*) AS cnt FROM "{SOURCE_SENSORS}".temperature')
    cnt = int(rows[0]["cnt"]) if rows else 0
    check("temperature has >= 10 rows", cnt >= 10, f"cnt={cnt}")

    rows = run_query(f'SELECT COUNT(*) AS cnt FROM "{SOURCE_SENSORS}".pressure')
    cnt = int(rows[0]["cnt"]) if rows else 0
    check("pressure has >= 3 rows", cnt >= 3, f"cnt={cnt}")

def test_sensors_filter():
    print("\n[sensors] WHERE filter on tag")
    rows = run_query(
        f'SELECT * FROM "{SOURCE_SENSORS}".temperature WHERE location = \'server_room\' LIMIT 10')
    check("filter on location=server_room works", len(rows) > 0, f"got {len(rows)}")
    if rows:
        locs = {r.get("location") for r in rows}
        check("all rows have location=server_room", locs == {"server_room"}, f"locs={locs}")

def test_sensors_aggregation():
    print("\n[sensors] Aggregation")
    rows = run_query(
        f'SELECT location, AVG("value") AS avg_temp FROM "{SOURCE_SENSORS}".temperature GROUP BY location ORDER BY location')
    check("group by location returns 2 groups", len(rows) == 2, f"got {len(rows)}")
    if rows:
        locs = {r.get("location") for r in rows}
        check("both locations present", {"office", "server_room"} == locs, f"locs={locs}")

def test_sensors_order():
    print("\n[sensors] ORDER BY time")
    rows = run_query(
        f'SELECT "time", "value" FROM "{SOURCE_SENSORS}".temperature ORDER BY "time" ASC LIMIT 5')
    check("time-ordered query returns rows", len(rows) > 0, f"got {len(rows)}")

def test_sensors_types():
    print("\n[sensors] Data types")
    rows = run_query(f'SELECT * FROM "{SOURCE_SENSORS}".temperature LIMIT 1')
    if rows:
        row = rows[0]
        check("value is numeric", isinstance(row.get("value"), (int, float)), f"type={type(row.get('value'))}")
        check("humidity is numeric", isinstance(row.get("humidity"), (int, float)), f"type={type(row.get('humidity'))}")
        check("time is a string (timestamp)", isinstance(row.get("time"), str), f"type={type(row.get('time'))}")

# ── Tests: metrics database ───────────────────────────────────────────────────

def test_metrics_source():
    print("\n[metrics] Basic table listing")
    rows = run_query(f'SELECT * FROM "{SOURCE_METRICS}".cpu LIMIT 5')
    check("cpu table returns rows", len(rows) > 0, f"got {len(rows)}")
    if rows:
        cols = list(rows[0].keys())
        check("host tag present", "host" in cols, f"cols={cols}")
        check("region tag present", "region" in cols, f"cols={cols}")
        check("usage_user field present", "usage_user" in cols, f"cols={cols}")

def test_metrics_multi_table():
    print("\n[metrics] Multiple measurements")
    rows = run_query(f'SELECT COUNT(*) AS cnt FROM "{SOURCE_METRICS}".memory')
    cnt = int(rows[0]["cnt"]) if rows else 0
    check("memory table has rows", cnt >= 3, f"cnt={cnt}")

def test_metrics_filter_and_agg():
    print("\n[metrics] Filter + aggregation")
    rows = run_query(
        f'SELECT host, AVG(usage_user) AS avg_user FROM "{SOURCE_METRICS}".cpu '
        f'WHERE region = \'us-east\' GROUP BY host')
    check("filtered aggregation on cpu returns rows", len(rows) > 0, f"got {len(rows)}")

def test_cross_measurement_join():
    print("\n[sensors] Cross-measurement join (same source)")
    rows = run_query(
        f'SELECT t.location, t."value" AS temp, p."value" AS pressure '
        f'FROM "{SOURCE_SENSORS}".temperature t '
        f'JOIN "{SOURCE_SENSORS}".pressure p ON t.location = p.location '
        f'LIMIT 5')
    check("cross-measurement join executes", isinstance(rows, list), f"got {len(rows)} rows")

# ── Main ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("Dremio InfluxDB Connector — Smoke Tests")
    print("=" * 60)

    tests = [
        test_sensors_source,
        test_sensors_count,
        test_sensors_filter,
        test_sensors_aggregation,
        test_sensors_order,
        test_sensors_types,
        test_metrics_source,
        test_metrics_multi_table,
        test_metrics_filter_and_agg,
        test_cross_measurement_join,
    ]

    for t in tests:
        try:
            t()
        except Exception as e:
            print(f"  ERROR  {t.__name__}: {e}")
            FAIL_COUNT += 1

    print("\n" + "=" * 60)
    print(f"Results: {PASS_COUNT} passed, {FAIL_COUNT} failed out of {PASS_COUNT + FAIL_COUNT} checks")
    print("=" * 60)
    sys.exit(0 if FAIL_COUNT == 0 else 1)
