# DHAP-42 — Real MinIO End-to-End Verification Log

Generated: 2026-11-03  
Context: Replacing moto mock with real MinIO (RELEASE.2025-09-07T16-13-09Z)

---

## 1. MinIO Build

```
Dockerfile.minio strategy:
  - Base image : debian:12-slim (official library — always pullable)
  - Install    : curl + ca-certificates via apt-get
  - Resolution : GitHub Releases API → latest release with binary assets
  - Asset      : minio_20250907161309.0.0_amd64.deb (dpkg -i)
  - Verified   : minio version RELEASE.2025-09-07T16-13-09Z  Runtime: go1.24.6 linux/amd64
```

Build command: `docker compose build minio`  
Build exit: 0

---

## 2. Stack Startup

```
docker compose up -d
```

| Container | Image | Status | Ports |
|-----------|-------|--------|-------|
| dhap42_postgres_meta | postgres:15 | healthy | 5432/tcp (internal) |
| dhap42_minio | project-dhap-42-...-minio | **healthy** | 0.0.0.0:9020→9000 (API), 0.0.0.0:9021→9001 (console) |
| dhap42_airflow_webserver | project-dhap-42-...-airflow-webserver | healthy | 0.0.0.0:8082→8080 |
| dhap42_airflow_scheduler | project-dhap-42-...-airflow-scheduler | healthy | 8080/tcp (internal) |

**mc-init log:**
```
=== [mc-init] Creating S3 bucket via boto3 ===
Waiting for S3 server at http://minio:9000...
S3 server is ready.
Bucket 'mailex-parquet' created.
Buckets: ['mailex-parquet']
=== [mc-init] Done. ===
```

---

## 3. MinIO Console & API Accessibility

| Endpoint | URL | Result |
|----------|-----|--------|
| S3 API health | http://localhost:9020/minio/health/live | **HTTP 200** ✅ |
| Web console | http://localhost:9021 | **HTTP 200** ✅ |

Console login: http://localhost:9021  
Credentials: minioadmin / minioadmin123  
Bucket visible: `mailex-parquet` (empty before first run)

---

## 4. Pre-Run State

```
Buckets: ['mailex-parquet']
Objects in mailex-parquet: 0  ← empty before first DAG run ✅
```

---

## 5. DAG Run 1 — Happy Path

**Trigger:** `airflow dags trigger csv_to_parquet_minio`  
**Run ID:** `manual__2026-11-03T14:39:19+00:00`  
**State:** `success`  
**Duration:** ~14 seconds (14:39:19 → 14:39:33 UTC)

### S3 Objects After Run 1

```
Total objects: 3
  email_event_extraction/split=dev/part-0.parquet    (1,016 rows, 114,041 bytes)
  email_event_extraction/split=test/part-0.parquet   (  944 rows, 108,770 bytes)
  email_event_extraction/split=train/part-0.parquet  (7,199 rows, 737,653 bytes)
Total rows: 9,159
```

Row count check: 9,168 raw − 9 exact duplicates = **9,159** ✅  
Partitions: dev + test + train = **3** ✅

---

## 6. DAG Run 2 — Idempotency

**Trigger:** `airflow dags trigger csv_to_parquet_minio`  
**Run ID:** `manual__2026-11-03T15:39:57+00:00`  
**State:** `success`  
**Duration:** ~9 seconds

### S3 Objects After Run 2

```
Total objects: 3   ← still 3, not 6 ✅ (idempotent overwrite confirmed)
  email_event_extraction/split=dev/part-0.parquet    (1,016 rows, 114,041 bytes)
  email_event_extraction/split=test/part-0.parquet   (  944 rows, 108,770 bytes)
  email_event_extraction/split=train/part-0.parquet  (7,199 rows, 737,653 bytes)
Total rows: 9,159
```

**Idempotency: PASS** — second run overwrote existing objects, no duplicates ✅

---

## 7. Negative Test Suite (9/9)

```
========================================================================
DHAP-42 — Negative Test Runner (direct validation)
========================================================================
Contract : /opt/airflow/config/schema_contract.yaml
Fixtures : /opt/airflow/tests/fixtures

  [a] Missing event_type column + bad num_arguments dtype          ✓
  [b] Renamed column (split → dataset_split)                      ✓
  [c] Extra column 'foo' (strict_columns=true)                    ✓
  [d] Wrong dtype in num_arguments ('NOT_AN_INTEGER')             ✓
  [e] Null in non-nullable thread_file                            ✓
  [f] Invalid event_type value ('UNKNOWN_EVENT')                  ✓
  [g] Invalid split value ('validation')                          ✓
  [h] Malformed trigger_indices (regex mismatch)                  ✓
  [i] Empty file / header only (row_count < min_rows)             ✓

SUMMARY: 9/9 negative tests passed.
ALL NEGATIVE TESTS PASSED ✓
========================================================================
```

---

## 8. Summary — All Acceptance Criteria Met

| Story | Criterion | Result |
|-------|-----------|--------|
| S2 | `docker compose up -d` brings up Airflow + real MinIO | ✅ |
| S2 | MinIO web console accessible at :9021 | ✅ HTTP 200 |
| S2 | Target bucket created on startup (idempotent) | ✅ |
| S2 | Parquet data persists in `minio_data` Docker volume | ✅ |
| S3 | DAG succeeds against real MinIO | ✅ success in 14s |
| S3 | 3 partitioned Parquet files (dev/test/train) | ✅ |
| S3 | Correct row count after deduplication | ✅ 9,159 rows |
| S3 | Idempotent re-run (3 objects, not 6) | ✅ |
| S1 | 9/9 negative tests pass against config/schema_contract.yaml | ✅ |
