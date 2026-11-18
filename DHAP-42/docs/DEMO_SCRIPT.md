# DEMO SCRIPT — Stage 2: Local CSV → MinIO Parquet Ingestion Pipeline

**Target length:** 5–7 minutes  
**Format:** Screen-share demo with spoken commentary  
**Audience:** Mentor / technical reviewer

---

## Overview (read before starting)

You will walk through the pipeline in this order:

1. Quick code walkthrough (DAG tasks, schema contract, folder layout) — ~2 min
2. `docker compose up` → confirm empty bucket in MinIO console — ~1 min
3. Trigger DAG with the good CSV → 3 Parquet objects appear in console — ~1.5 min
4. Re-trigger to prove idempotency — ~30 s
5. Trigger with the bad CSV → `validate_contract` fails, nothing written — ~45 s
6. Run negative tests → 9/9 pass — ~30 s

Open two terminal windows and the browser before you start.

---

## 1. Code Walkthrough (~2 minutes)

### 1a. Folder layout

> *"Let me show you the project structure."*

Point to these in your file explorer or terminal:

```
stage-2-csv-to-minio/
├── mailex_email_event_extraction/
│   ├── manifest.yaml          ← dataset metadata
│   └── schema_contract.yaml   ← validation rules
├── dags/
│   ├── csv_to_parquet_minio.py  ← the DAG
│   └── extraction/sample.csv   ← the MailEx dataset (9,168 rows)
├── tests/                      ← negative tests + verification scripts
├── docker-compose.yaml
└── Dockerfile
```

> *"Everything the pipeline needs lives here. The contract and manifest are in `mailex_email_event_extraction/` so they're co-located with the dataset metadata."*

---

### 1b. DAG task graph

Open `dags/csv_to_parquet_minio.py` and scroll to the bottom task-wiring block:

```python
raw_dir       = read_csv()
validated_dir = validate_contract(raw_dir)
cleaned_dir   = transform(validated_dir)
                write_parquet_to_minio(cleaned_dir)
```

> *"Four tasks in a linear chain. Tasks communicate via file paths in `/tmp/<run_id>/` — not XCom — because the CSV is 5.6 MB, well over Airflow's 48 KB XCom limit."*

---

### 1c. Task: `read_csv`

Scroll to the `read_csv` function:

```python
@task
def read_csv(**context):
    csv_path = context["dag_run"].conf.get("csv_path", _DEFAULT_CSV_PATH)
    df = pd.read_csv(csv_path)
    ...
    df.to_parquet(raw_parquet, index=False)
    return str(tmp_dir)
```

> *"Reads the CSV, writes a raw Parquet snapshot to a temp dir, and passes the dir path to the next task. The CSV path is overridable via DAG conf — that's how we inject the bad CSV for negative tests."*

---

### 1d. Task: `validate_contract`

Scroll to `validate_contract`:

> *"This is the enforcement layer. It reads `schema_contract.yaml` and runs every check before raising — it never fail-fast on the first violation. It checks: empty file, extra columns, missing columns, column order, per-column type/nullability/allowed-values/regex/min-value, and id uniqueness."*

Point to the `AirflowFailException` raise at the end of the validation block:

```python
raise AirflowFailException(
    f"Schema contract violated ({len(errors)} error(s)):\n" + "\n".join(errors)
)
```

> *"If any check fails, the task raises `AirflowFailException`, which marks the task `failed` and immediately marks all downstream tasks `upstream_failed`. Nothing reaches MinIO."*

---

### 1e. Task: `transform`

> *"Three clean-up steps: strip whitespace, drop 9 exact duplicates that exist in the raw dataset (ignoring the `id` column), and add two audit columns: `ingested_at` (UTC ISO-8601 timestamp) and `run_id` (Airflow run ID). No coercion of valid nulls — the contract already validates nullability."*

---

### 1f. Task: `write_parquet_to_minio`

Scroll to the idempotency block:

```python
# Idempotency: delete existing objects under prefix
for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
    for obj in page.get("Contents", []):
        objects_to_delete.append({"Key": obj["Key"]})
if objects_to_delete:
    s3.delete_objects(...)
```

> *"Before writing, the task deletes every existing object under the prefix. This means re-running the DAG always yields exactly 3 objects — no accumulation. After deletion it writes one Snappy-compressed Parquet per split value using Hive-style keys:"*

```
mailex-parquet/email_event_extraction/split=train/part-0.parquet
mailex-parquet/email_event_extraction/split=dev/part-0.parquet
mailex-parquet/email_event_extraction/split=test/part-0.parquet
```

---

## 2. `docker compose up` → Empty Bucket (~1 minute)

Switch to Terminal 1. Navigate to `stage-2-csv-to-minio/`.

```bash
docker compose up -d --build
```

> *"Building the Airflow image — it installs pandas, pyarrow, PyYAML, and boto3. The MinIO service uses `pgsty/silo`, a maintained drop-in fork of MinIO pulled directly from Docker Hub — no custom Dockerfile needed."*

After ~90 seconds:

```bash
docker compose ps
```

> *"All services healthy. `mc-init` has already created the `mailex-parquet` bucket."*

Switch to browser → **http://localhost:9021**  
Login: `minioadmin` / `minioadmin123`

> *"This is the real MinIO web console. Click on Buckets — you can see `mailex-parquet` already exists and is empty. This satisfies the Story 2 acceptance criterion: the bucket is visible and browsable before the first run."*

---

## 3. Trigger DAG → 3 Parquet Objects (~1.5 minutes)

Switch to browser → **http://localhost:8082**  
Login: `admin` / `admin`

1. Find `csv_to_parquet_minio`, toggle **unpause**
2. Click the **▶ Trigger** button → leave conf blank → **Trigger**

> *"The DAG is running. While we wait, notice the task graph: read_csv → validate_contract → transform → write_parquet_to_minio."*

Watch the graph view. After ~30–60 seconds all 4 boxes turn green.

Switch to Terminal 2:

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/verify_s3.py
```

Expected output (show on screen):

```
=== Listing objects in s3://mailex-parquet/email_event_extraction/ ===
Found 3 object(s):
  email_event_extraction/split=dev/part-0.parquet      (...) 
  email_event_extraction/split=test/part-0.parquet     (...)
  email_event_extraction/split=train/part-0.parquet    (...)
...
=== ALL ASSERTIONS PASSED ===
✓ 3 Parquet partition files (train / dev / test)
✓ 9,159 total rows
✓ Each file contains exactly one split value
```

Switch to MinIO console → refresh `mailex-parquet` → browse to `email_event_extraction/`.

> *"Three partition folders, each with a `part-0.parquet` file. 9,159 rows total — the 9 known exact duplicates were dropped by the transform task."*

---

## 4. Re-trigger → Idempotency (~30 seconds)

```bash
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio
```

After it completes:

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/idempotency_check.py
```

Expected:

```
Object count after 2 successful DAG runs: 3
  email_event_extraction/split=dev/part-0.parquet   (...)
  email_event_extraction/split=test/part-0.parquet  (...)
  email_event_extraction/split=train/part-0.parquet (...)
IDEMPOTENCY PASSED — exactly 3 partition files, no accumulation across runs.
```

> *"Still exactly 3 objects. The delete-before-write pattern guarantees idempotency — no matter how many times you trigger the DAG, you always get a clean, non-duplicated dataset."*

---

## 5. Bad CSV → `validate_contract` Fails (~45 seconds)

```bash
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/tests/bad_missing_column.csv"}'
```

Switch to Airflow UI → watch the graph. `validate_contract` turns red; `transform` and `write_parquet_to_minio` turn grey (`upstream_failed`).

```bash
# Get the failed run_id
docker exec dhap42_airflow_scheduler \
  airflow dags list-runs -d csv_to_parquet_minio

# Show the error (replace <run_id> with actual value)
docker exec dhap42_airflow_scheduler \
  airflow tasks logs csv_to_parquet_minio validate_contract <run_id>
```

> *"The log shows: `SchemaError: missing required columns: ['event_type']`. The exception type is `AirflowFailException` with `retries=0`, so Airflow doesn't retry. Nothing was written to MinIO."*

Verify MinIO still has only 3 objects (from the previous good runs):

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/verify_s3.py
```

---

## 6. Negative Tests → 9/9 Pass (~30 seconds)

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/run_negative_tests.py
```

Expected output:

```
Running 9 negative-test cases...
  [PASS] a_missing_column       — SchemaError: missing required columns...
  [PASS] b_renamed_column       — SchemaError: missing required columns...
  [PASS] c_extra_column         — SchemaError: extra columns not allowed...
  [PASS] d_wrong_dtype          — SchemaError: ...
  [PASS] e_null_nonnullable     — SchemaError: ...
  [PASS] f_invalid_event_type   — SchemaError: ...
  [PASS] g_invalid_split        — SchemaError: ...
  [PASS] h_bad_trigger_indices  — SchemaError: ...
  [PASS] i_empty_file           — SchemaError: file has 0 rows...
9 / 9 passed.
```

> *"All 9 contract violations are caught and rejected. The pipeline is robust against malformed inputs."*

---

## Closing (~15 seconds)

> *"To summarize: Stage 2 is a fully containerized Airflow + MinIO pipeline that ingests the MailEx dataset, validates it against a schema contract, transforms it, and writes Snappy-compressed Parquet files partitioned by split. It's idempotent, schema-enforced, and testable. Thank you."*

---

## Quick Reference — Commands Used

```bash
# Start stack
docker compose up -d --build

# Check status
docker compose ps

# Trigger DAG (good CSV)
docker exec dhap42_airflow_scheduler airflow dags trigger csv_to_parquet_minio

# Trigger DAG (bad CSV)
docker exec dhap42_airflow_scheduler airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/tests/bad_missing_column.csv"}'

# List runs
docker exec dhap42_airflow_scheduler airflow dags list-runs -d csv_to_parquet_minio

# Verify S3 objects
docker exec dhap42_airflow_scheduler python3 /opt/airflow/tests/verify_s3.py

# Idempotency check
docker exec dhap42_airflow_scheduler python3 /opt/airflow/tests/idempotency_check.py

# Negative tests
docker exec dhap42_airflow_scheduler python3 /opt/airflow/tests/run_negative_tests.py

# Stop everything (keeps volumes)
docker compose down

# Stop everything + wipe data
docker compose down -v
```

## URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8082 | admin / admin |
| MinIO Console | http://localhost:9021 | minioadmin / minioadmin123 |
| MinIO S3 API | http://localhost:9020 | (use boto3 / mc client) |
