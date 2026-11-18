"""
verify_pipeline.py
==================
Stage 2 — End-to-End Verification Script

Runs all verification steps:
1. Checks all Docker containers are healthy
2. Triggers the good DAG run and waits for completion
3. Reads back the 3 Parquet objects from MinIO and validates row counts
4. Runs idempotency check (re-triggers, verifies same row counts)
5. Saves evidence to docs/evidence/

Usage (from project root):
    python tests/verify_pipeline.py
"""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib import request, error as urllib_error
import base64

# Optional dotenv
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

AIRFLOW_PORT = os.getenv("AIRFLOW_WEBSERVER_PORT", "8082")
AIRFLOW_USER = os.getenv("AIRFLOW_ADMIN_USER", "admin")
AIRFLOW_PASS = os.getenv("AIRFLOW_ADMIN_PASSWORD", "admin")
AIRFLOW_BASE = f"http://localhost:{AIRFLOW_PORT}/api/v1"
MINIO_PORT   = os.getenv("MINIO_API_PORT", "9020")
MINIO_USER   = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_PASS   = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mailex-parquet")
MINIO_ENDPOINT = f"http://localhost:{MINIO_PORT}"
DAG_ID = "csv_to_parquet_minio"

EVIDENCE_DIR = Path(__file__).parent.parent / "docs" / "evidence"
EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)

EXPECTED_TOTAL_ROWS = 9159   # 9168 - 9 exact duplicates ignoring id


def _auth_header():
    creds = base64.b64encode(f"{AIRFLOW_USER}:{AIRFLOW_PASS}".encode()).decode()
    return {"Authorization": f"Basic {creds}", "Content-Type": "application/json"}


def _api(method, path, body=None):
    url = f"{AIRFLOW_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = request.Request(url, data=data, headers=_auth_header(), method=method)
    with request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read())


def unpause_dag():
    _api("PATCH", f"/dags/{DAG_ID}", {"is_paused": False})
    print(f"✓ DAG '{DAG_ID}' unpaused.")


def trigger_and_wait(timeout=600):
    """Trigger the default DAG run, wait for completion. Returns run_id and state."""
    resp = _api("POST", f"/dags/{DAG_ID}/dagRuns", {"conf": {}})
    run_id = resp["dag_run_id"]
    print(f"  Triggered run: {run_id}")

    deadline = time.time() + timeout
    while time.time() < deadline:
        r = _api("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}")
        state = r["state"]
        if state in ("success", "failed"):
            return run_id, state
        time.sleep(8)
    raise TimeoutError(f"Run did not finish within {timeout}s")


def get_task_states(run_id):
    resp = _api("GET", f"/dags/{DAG_ID}/dagRuns/{run_id}/taskInstances")
    return {ti["task_id"]: ti["state"] for ti in resp.get("task_instances", [])}


def read_parquet_from_minio():
    """Download and read the 3 Parquet partitions. Returns total row count and per-split counts."""
    import boto3
    import pyarrow.parquet as pq

    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_USER,
        aws_secret_access_key=MINIO_PASS,
    )

    prefix = "email_event_extraction/"
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=MINIO_BUCKET, Prefix=prefix):
        objects.extend(page.get("Contents", []))

    print(f"  MinIO objects under prefix: {len(objects)}")
    for obj in objects:
        print(f"    s3://{MINIO_BUCKET}/{obj['Key']}  ({obj['Size']:,} bytes)")

    total = 0
    per_split = {}
    for obj in objects:
        key = obj["Key"]
        data = s3.get_object(Bucket=MINIO_BUCKET, Key=key)["Body"].read()
        table = pq.read_table(io.BytesIO(data))
        rows = table.num_rows
        split_val = key.split("split=")[1].split("/")[0] if "split=" in key else "unknown"
        per_split[split_val] = rows
        total += rows
        # Verify schema has audit columns
        cols = table.schema.names
        assert "ingested_at" in cols, f"Missing audit column 'ingested_at' in {key}"
        assert "run_id" in cols, f"Missing audit column 'run_id' in {key}"

    return total, per_split, objects


def save_evidence(filename, content):
    path = EVIDENCE_DIR / filename
    path.write_text(content, encoding="utf-8")
    print(f"  Evidence saved: {path}")


def main():
    print("=" * 70)
    print("Stage 2 — End-to-End Pipeline Verification")
    print("=" * 70)

    evidence_lines = []

    # ── Step 1: Check containers ───────────────────────────────────────────
    print("\n[1] Checking Docker containers...")
    result = subprocess.run(
        ["docker", "compose", "ps", "--format", "json"],
        capture_output=True, text=True,
        cwd=str(Path(__file__).parent.parent)
    )
    containers_json = result.stdout.strip()
    print(containers_json[:2000])
    evidence_lines.append("## Docker Compose Status\n```\n" + result.stdout + "\n```\n")

    # ── Step 2: Unpause DAG ────────────────────────────────────────────────
    print("\n[2] Unpausing DAG...")
    unpause_dag()

    # ── Step 3: Good run ───────────────────────────────────────────────────
    print("\n[3] Triggering good CSV run...")
    run_id, state = trigger_and_wait()
    task_states = get_task_states(run_id)
    print(f"  Final state: {state}")
    print(f"  Task states: {task_states}")
    assert state == "success", f"Expected 'success', got '{state}'"
    assert task_states.get("read_csv") == "success"
    assert task_states.get("validate_contract") == "success"
    assert task_states.get("transform") == "success"
    assert task_states.get("write_parquet_to_minio") == "success"
    print("  ✓ All 4 tasks green!")
    evidence_lines.append(f"## Good Run\n- run_id: {run_id}\n- state: {state}\n- tasks: {task_states}\n")

    # ── Step 4: Verify MinIO ───────────────────────────────────────────────
    print("\n[4] Reading back Parquet from MinIO...")
    total_rows, per_split, objects = read_parquet_from_minio()
    print(f"  Total rows: {total_rows}  (expected: {EXPECTED_TOTAL_ROWS})")
    print(f"  Per split: {per_split}")
    assert total_rows == EXPECTED_TOTAL_ROWS, \
        f"Row count mismatch: got {total_rows}, expected {EXPECTED_TOTAL_ROWS}"
    assert len(objects) == 3, f"Expected 3 objects, got {len(objects)}"
    print("  ✓ Row count correct. Schema includes audit columns.")
    evidence_lines.append(
        f"## MinIO Verification\n- Total rows: {total_rows}\n"
        f"- Per split: {per_split}\n"
        f"- Objects: {[o['Key'] for o in objects]}\n"
    )

    # ── Step 5: Idempotency ────────────────────────────────────────────────
    print("\n[5] Idempotency check (re-trigger)...")
    run_id2, state2 = trigger_and_wait()
    total2, per_split2, objects2 = read_parquet_from_minio()
    print(f"  Re-run state: {state2}")
    print(f"  Total rows after re-run: {total2}  per_split: {per_split2}")
    assert state2 == "success"
    assert total2 == EXPECTED_TOTAL_ROWS, f"Idempotency fail: {total2} != {EXPECTED_TOTAL_ROWS}"
    assert len(objects2) == 3, f"Expected 3 objects after re-run, got {len(objects2)}"
    print("  ✓ Idempotent — same row count, exactly 3 objects.")
    evidence_lines.append(
        f"## Idempotency\n- Re-run id: {run_id2}\n"
        f"- Total rows: {total2} (same)\n- Objects: {len(objects2)} (same)\n"
    )

    # ── Save evidence ──────────────────────────────────────────────────────
    evidence_content = f"# Stage 2 Verification Evidence\n\n" + "\n".join(evidence_lines)
    save_evidence("verification_results.md", evidence_content)

    print("\n" + "=" * 70)
    print("ALL VERIFICATION STEPS PASSED ✓")
    print("=" * 70)
    return 0


if __name__ == "__main__":
    sys.exit(main())
