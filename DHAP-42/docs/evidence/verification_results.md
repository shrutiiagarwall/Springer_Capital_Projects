# Stage 2 — End-to-End Verification Evidence

**Generated:** 2026-11-18  
**Stack:** `docker.io/pgsty/silo:latest` (real MinIO fork) + Airflow 2.9.3  
**Host:** Windows 11, Docker Desktop (desktop-linux context)

---

## 1. Stack Status (`docker compose ps`)

```
NAME                       IMAGE                                    STATUS
dhap42_airflow_scheduler   stage-2-csv-to-minio-airflow-scheduler   Up (unhealthy→healthy)   :8080
dhap42_airflow_webserver   stage-2-csv-to-minio-airflow-webserver   Up (healthy)             0.0.0.0:8082->8080/tcp
dhap42_minio               docker.io/pgsty/silo:latest              Up (healthy)             0.0.0.0:9020->9000/tcp, 0.0.0.0:9021->9001/tcp
dhap42_postgres_meta       postgres:15                              Up (healthy)             :5432
```

---

## 2. MinIO Console — HTTP 200

```
curl.exe -s -o NUL -w "%{http_code}" http://localhost:9021
200
```

MinIO web console (pgsty/silo) is reachable at **http://localhost:9021**.  
Login: `minioadmin` / `minioadmin123`

---

## 3. Bucket Creation (mc-init log)

```
=== [mc-init] Creating S3 bucket via boto3 ===
Waiting for S3 server at http://minio:9000...
S3 server is ready.
Bucket 'mailex-parquet' created.
Buckets: ['mailex-parquet']
=== [mc-init] Done. ===
```

Bucket `mailex-parquet` exists and is **empty** before the first DAG run.

---

## 4. DAG Run 1 — Good CSV (Happy Path)

**run_id:** `manual__2026-11-18T18:19:39+00:00`  
**state:** `success`  
**duration:** ~10 seconds

### verify_s3.py output

```
=== Listing objects in s3://mailex-parquet/email_event_extraction/ ===
Found 3 object(s):
  email_event_extraction/split=dev/part-0.parquet   (114,041 bytes)
  email_event_extraction/split=test/part-0.parquet  (108,770 bytes)
  email_event_extraction/split=train/part-0.parquet (737,653 bytes)
Total: 960,464 bytes

=== Reading each partition and validating =====
  email_event_extraction/split=dev/part-0.parquet:   1,016 rows, splits=['dev'],   cols=[...audit cols included...]
  email_event_extraction/split=test/part-0.parquet:    944 rows, splits=['test'],  cols=[...audit cols included...]
  email_event_extraction/split=train/part-0.parquet: 7,199 rows, splits=['train'], cols=[...audit cols included...]

Splits seen: ['dev', 'test', 'train']
Total rows across all partitions: 9,159

=== ALL ASSERTIONS PASSED ===
✓ 3 Parquet partition files (train / dev / test)
✓ 9,159 total rows (matches source CSV)
✓ Each file contains exactly one split value
```

---

## 5. DAG Run 2 — Idempotency Check

**run_id:** `manual__2026-11-18T18:22:22+00:00`  
**state:** `success`

### idempotency_check.py output

```
Object count after 2 successful DAG runs: 3
  email_event_extraction/split=dev/part-0.parquet   (114,041 bytes)
  email_event_extraction/split=test/part-0.parquet  (108,770 bytes)
  email_event_extraction/split=train/part-0.parquet (737,653 bytes)

IDEMPOTENCY PASSED — exactly 3 partition files, no accumulation across runs.
```

---

## 6. DAG Run 3 — Bad CSV (`bad_missing_column.csv`)

**run_id:** `manual__2026-11-18T18:24:42+00:00`  
**state:** `failed`  
**Failed task:** `validate_contract`  
**Downstream tasks:** `transform`, `write_parquet_to_minio` → `upstream_failed` (never ran)

### validate_contract task log (attempt=1)

```
[2026-11-18T18:24:47.071+0000] {taskinstance.py:2905} ERROR - Task failed with exception
    raise AirflowFailException(
airflow.exceptions.AirflowFailException:
SCHEMA CONTRACT VALIDATION FAILED — 3 violation(s):
======================================================================
  [1] MISSING COLUMNS: ['event_type']
  [2] WRONG COLUMN ORDER.
      Expected : ['id', 'thread_file', 'split', 'turn_id', 'event_type', 'trigger_text', 'trigger_indices', 'extras', 'sentence_text', 'num_arguments']
      Got      : ['id', 'thread_file', 'split', 'turn_id', 'trigger_text', 'trigger_indices', 'extras', 'sentence_text', 'num_arguments']
  [3] TYPE VIOLATION: 'num_arguments' must be integer but contains non-integer values
      (first 5): ['NOT_AN_INTEGER', 'NOT_AN_INTEGER', 'NOT_AN_INTEGER', 'NOT_AN_INTEGER', 'NOT_AN_INTEGER']
======================================================================
Fix the CSV or update schema_contract.yaml and re-trigger the DAG.
NO DATA was written to MinIO.
```

**Result:** 0 objects written to MinIO during the failed run. Object count remains 3 (from run 1 & 2).

---

## 7. Negative Tests — 9/9 PASSED

```
========================================================================
Stage 2 — Negative Test Runner (direct validation)
========================================================================
Contract : /opt/airflow/mailex_email_event_extraction/schema_contract.yaml
Fixtures : /opt/airflow/tests/fixtures

  [a] Missing event_type column + bad num_arguments dtype
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [b] Renamed column (split → dataset_split)
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [c] Extra column 'foo' (strict_columns=true)
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [d] Wrong dtype in num_arguments ('NOT_AN_INTEGER')
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [e] Null in non-nullable thread_file
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [f] Invalid event_type value ('UNKNOWN_EVENT')
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [g] Invalid split value ('validation')
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [h] Malformed trigger_indices (regex mismatch)
    ✓  Correctly REJECTED — validate_contract raised (expected)
  [i] Empty file / header only (row_count < min_rows)
    ✓  Correctly REJECTED — validate_contract raised (expected)

========================================================================
SUMMARY
========================================================================
  9/9 negative tests passed.

ALL NEGATIVE TESTS PASSED ✓
```

---

## 8. Summary

| Check | Result |
|-------|--------|
| MinIO console HTTP 200 at :9021 | ✅ |
| Bucket `mailex-parquet` created empty | ✅ |
| Run 1 (good CSV) → 3 objects, 9,159 rows | ✅ |
| Run 2 (re-trigger) → still 3 objects (idempotent) | ✅ |
| Run 3 (bad CSV) → `validate_contract` fails, 0 objects written | ✅ |
| Negative tests 9/9 pass | ✅ |
