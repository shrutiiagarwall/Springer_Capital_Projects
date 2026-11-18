# Stage 2 — Local CSV → MinIO Parquet Ingestion Pipeline

A self-contained Airflow + MinIO pipeline that ingests the
[MailEx Email-Event-Extraction](https://github.com/salokr/Email-Event-Extraction) dataset
(CSV) into an S3-compatible object store as Snappy-compressed Parquet files,
partitioned by `split` (train / dev / test).

> **Stage 1** (Postgres ingestion pipeline) lives in a separate folder in this repository.

---

## Table of Contents

1. [Architecture](#architecture)
2. [Prerequisites](#prerequisites)
3. [Setup](#setup)
4. [Starting the Stack](#starting-the-stack)
5. [Airflow UI](#airflow-ui)
6. [MinIO Console](#minio-console)
7. [Triggering the DAG](#triggering-the-dag)
8. [Verifying Objects in MinIO](#verifying-objects-in-minio)
9. [Negative Tests](#negative-tests)
10. [Idempotency](#idempotency)
11. [Folder Layout](#folder-layout)
12. [Troubleshooting](#troubleshooting)

---

## Architecture

```
                        ┌──────────────────────────────────────────┐
                        │           Docker Compose Stack            │
                        │                                           │
  host :8082 ──────────►│  airflow-webserver  (Airflow 2.x UI)     │
                        │          │                                │
                        │  airflow-scheduler  (LocalExecutor)       │
                        │          │                                │
                        │          ▼                                │
                        │   ┌─────────────────────────────┐        │
                        │   │   DAG: csv_to_parquet_minio  │        │
                        │   │                              │        │
                        │   │  read_csv                    │        │
                        │   │     │                        │        │
                        │   │  validate_contract           │        │
                        │   │     │                        │        │
                        │   │  transform                   │        │
                        │   │     │                        │        │
                        │   │  write_parquet_to_minio ─────┼───────►│  minio (pgsty/silo)
                        │   └─────────────────────────────┘        │  API  :9020
                        │                                           │  UI   :9021
                        │  postgres-meta  (Airflow metadata DB)     │
                        └──────────────────────────────────────────┘
```

### DAG Task Graph

```
read_csv ──► validate_contract ──► transform ──► write_parquet_to_minio
```

| Task | What it does |
|------|-------------|
| `read_csv` | Reads the CSV from disk, writes raw Parquet to `/tmp/<run_id>/raw.parquet` |
| `validate_contract` | Checks all schema rules in `mailex_email_event_extraction/schema_contract.yaml`; fails fast on any violation |
| `transform` | Strips whitespace, drops 9 exact duplicates, adds `ingested_at` + `run_id` audit columns |
| `write_parquet_to_minio` | Deletes existing objects under prefix (idempotency), writes 3 Hive-partitioned Parquet files |

### S3 Key Layout

```
mailex-parquet/
  email_event_extraction/
    split=train/part-0.parquet   (7,207 rows)
    split=dev/part-0.parquet     (1,007 rows)
    split=test/part-0.parquet      (945 rows)
```

---

## Prerequisites

| Requirement | Version tested |
|------------|---------------|
| Docker Desktop | 4.x |
| Docker Compose v2 | bundled with Docker Desktop |
| (optional) Python 3.11+ | for running test scripts on the host |

No other local dependencies are required — the Dockerfile installs everything inside the container.

---

## Setup

```bash
# 1. Clone / navigate to this folder
cd stage-2-csv-to-minio

# 2. Copy the example env file (defaults work out of the box)
cp .env.example .env
```

The `.env` file contains:

| Variable | Default | Purpose |
|----------|---------|---------|
| `MINIO_ROOT_USER` | `minioadmin` | MinIO / Silo root username |
| `MINIO_ROOT_PASSWORD` | `minioadmin123` | MinIO / Silo root password |
| `MINIO_BUCKET` | `mailex-parquet` | S3 bucket name |
| `AIRFLOW_ADMIN_USER` | `admin` | Airflow UI username |
| `AIRFLOW_ADMIN_PASSWORD` | `admin` | Airflow UI password |
| `POSTGRES_USER` | `airflow` | Airflow metadata DB user |
| `POSTGRES_PASSWORD` | `airflow_dhap42_pw` | Airflow metadata DB password |

> **Never commit a populated `.env` with real secrets.** `.gitignore` already excludes `.env`.

---

## Starting the Stack

```bash
# Build images and start all services in the background
docker compose up -d --build

# Watch logs (Ctrl-C to exit)
docker compose logs -f
```

Startup order (enforced by `depends_on` + healthchecks):

1. `postgres-meta` → healthy
2. `minio` → healthy  ← S3 API + web console
3. `mc-init` → runs once, creates the `mailex-parquet` bucket
4. `airflow-init` → runs once, migrates DB + creates admin user
5. `airflow-webserver` + `airflow-scheduler` → start

Allow ~90 seconds on first run for all services to become healthy.

```bash
# Check status
docker compose ps
```

All services should show `healthy` or `exited (0)` for the one-shot containers.

---

## Airflow UI

Open **http://localhost:8082** in your browser.

- **Username:** `admin`  
- **Password:** `admin`

The DAG `csv_to_parquet_minio` appears in the list. It is paused by default; unpause it before triggering.

---

## MinIO Console

Open **http://localhost:9021** in your browser.

- **Username:** `minioadmin`  
- **Password:** `minioadmin123`

After `mc-init` runs, you will see the `mailex-parquet` bucket listed under **Buckets**. It is empty until you trigger the DAG.

> The MinIO service uses **pgsty/silo** (`docker.io/pgsty/silo`), a maintained drop-in fork of MinIO that keeps the same environment variables, the same S3 API, and the same web console.

---

## Triggering the DAG

### Option A — Airflow UI

1. Go to **http://localhost:8082**
2. Find `csv_to_parquet_minio`, click the **▶ Trigger** button
3. Leave the JSON config blank (uses `dags/extraction/sample.csv`)
4. Click **Trigger**

### Option B — CLI

```bash
# Happy path (uses sample.csv by default)
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio

# Trigger with a specific CSV (replace path as needed)
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/dags/extraction/sample.csv"}'

# Trigger with the BAD CSV (validate_contract should fail)
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/tests/bad_missing_column.csv"}'
```

### Watch task progress

```bash
# Poll DAG run status
docker exec dhap42_airflow_scheduler \
  airflow dags list-runs -d csv_to_parquet_minio
```

A successful run shows all 4 tasks as `success`. A bad-CSV run shows `validate_contract` as `failed` and downstream tasks as `upstream_failed`.

---

## Verifying Objects in MinIO

### In the console

1. Open **http://localhost:9021** → **Buckets** → `mailex-parquet`
2. Browse to `email_event_extraction/`
3. After a successful run you should see three folders: `split=train/`, `split=dev/`, `split=test/`
4. Each folder contains a single `part-0.parquet` file

### Via CLI script (runs inside the scheduler container)

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/verify_s3.py
```

Expected output:

```
=== Listing objects in s3://mailex-parquet/email_event_extraction/ ===
Found 3 object(s):
  email_event_extraction/split=dev/part-0.parquet      (XX,XXX bytes)
  email_event_extraction/split=test/part-0.parquet     (XX,XXX bytes)
  email_event_extraction/split=train/part-0.parquet    (XX,XXX bytes)
...
=== ALL ASSERTIONS PASSED ===
✓ 3 Parquet partition files (train / dev / test)
✓ 9,159 total rows (matches source CSV)
✓ Each file contains exactly one split value
```

---

## Negative Tests

The test suite validates that all 9 schema-contract violations are correctly rejected.

### Step 1 — Regenerate fixtures (once, or after changing the contract)

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/generate_fixtures.py
```

### Step 2 — Run the negative tests

```bash
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/run_negative_tests.py
```

Expected output:

```
Running 9 negative-test cases...
  [PASS] a_missing_column   — SchemaError: missing required columns: ['event_type']
  [PASS] b_renamed_column   — SchemaError: ...
  ...
  [PASS] i_empty_file       — SchemaError: file has 0 rows ...
9 / 9 passed.
```

### Bad-CSV DAG trigger

Trigger the DAG with the bad CSV and inspect the `validate_contract` task log:

```bash
# Trigger
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio \
  --conf '{"csv_path": "/opt/airflow/tests/bad_missing_column.csv"}'

# After the run fails, get the run_id
docker exec dhap42_airflow_scheduler \
  airflow dags list-runs -d csv_to_parquet_minio

# View the validate_contract task logs
docker exec dhap42_airflow_scheduler \
  airflow tasks logs csv_to_parquet_minio validate_contract <run_id>
```

You should see `SchemaError: missing required columns: ['event_type']` and **nothing written to MinIO** (verify with `verify_s3.py` — it will show 0 or 3 objects depending on whether a previous good run existed).

---

## Idempotency

Re-trigger the DAG with the same CSV. The `write_parquet_to_minio` task deletes all existing objects under the prefix before writing, so the bucket always ends up with exactly 3 objects — no accumulation.

```bash
# Trigger a second time
docker exec dhap42_airflow_scheduler \
  airflow dags trigger csv_to_parquet_minio

# After completion, verify still exactly 3 objects
docker exec dhap42_airflow_scheduler \
  python3 /opt/airflow/tests/idempotency_check.py
```

---

## Folder Layout

```
stage-2-csv-to-minio/
├── mailex_email_event_extraction/
│   ├── manifest.yaml            # Dataset metadata (source, schema, owner)
│   └── schema_contract.yaml     # Column rules validated by validate_contract task
├── dags/
│   ├── csv_to_parquet_minio.py  # Airflow DAG (do not modify)
│   └── extraction/
│       └── sample.csv           # MailEx dataset (9,168 rows)
├── tests/
│   ├── bad_missing_column.csv   # Fixture: triggers validate_contract failure
│   ├── fixtures/                # b_renamed_column.csv … i_empty_file.csv
│   ├── generate_fixtures.py     # Generates fixtures from sample.csv
│   ├── run_negative_tests.py    # Runs 9 negative test cases
│   ├── verify_s3.py             # Reads Parquet back from MinIO, asserts counts
│   ├── idempotency_check.py     # Asserts exactly 3 objects after re-run
│   ├── verify_pipeline.py       # Full end-to-end verification script
│   └── inspect_contract.py     # Prints parsed schema_contract.yaml
├── docs/
│   ├── evidence/               # Fresh logs from the verified run
│   └── DEMO_SCRIPT.md          # 5-7 min demo video script
├── docker-compose.yaml
├── Dockerfile                  # Airflow image (pandas, pyarrow, PyYAML, boto3)
├── .env.example
├── .gitignore
└── README.md                   # This file
```

---

## Troubleshooting

### Containers not starting

```bash
docker compose logs postgres-meta
docker compose logs minio
docker compose logs airflow-init
```

### MinIO console not accessible

Verify the `minio` container is healthy:

```bash
docker compose ps minio
docker compose logs minio
```

The console is at **http://localhost:9021**. The API is at **http://localhost:9020**.

### DAG not visible in Airflow UI

The scheduler parses DAGs from `./dags/`. If the file has a syntax error, it won't appear:

```bash
docker exec dhap42_airflow_scheduler \
  airflow dags list
```

Check scheduler logs for parse errors:

```bash
docker compose logs airflow-scheduler | grep -i error
```

### Reset everything (clean slate)

```bash
# Stop all containers AND delete volumes (data is lost)
docker compose down -v

# Rebuild from scratch
docker compose up -d --build
```

### Port conflicts

Default ports:

| Service | Host port |
|---------|----------|
| Airflow UI | 8082 |
| MinIO S3 API | 9020 |
| MinIO console | 9021 |

If these conflict with other services, edit the corresponding `*_PORT` variables in `.env`.
