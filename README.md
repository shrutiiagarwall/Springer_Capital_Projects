# Springer Capital Projects

Internal data engineering and analytics projects for **Glynac AI / Springer Capital**.

## Repository Structure

| Folder | Jira | Description |
|---|---|---|
| [`Email_Event_Extraction/`](./Email_Event_Extraction/) | DHAP-34 | Airflow + PostgreSQL ingestion pipeline for the MailEx email event extraction dataset (Srivastava et al., EMNLP 2023) |
| [`DHAP-42/`](./DHAP-42/) | DHAP-42 | Local CSV → MinIO Parquet ingestion pipeline — same MailEx dataset, Airflow DAG writes partitioned Parquet to a real MinIO S3-compatible store with schema contract validation |

---

## Quick Start — Email Event Extraction (DHAP-34)

```bash
# 1. Generate the flat CSV from raw JSON data (run once on your host)
python Email_Event_Extraction/scripts/flatten_to_csv.py

# 2. Start the Docker stack
cd Email_Event_Extraction/
docker compose up -d

# 3. Open Airflow UI at http://localhost:8080  (admin / admin)
#    Trigger DAG: load_email_event_extraction_to_postgres

# 4. Verify in Postgres
docker compose exec postgres-target \
  psql -U airflow -d mailex_warehouse -c \
  "SELECT event_type, COUNT(*) FROM email_events GROUP BY event_type ORDER BY 2 DESC;"
```

See [`Email_Event_Extraction/README.md`](./Email_Event_Extraction/README.md) for the full runbook.

---

## Quick Start — CSV → MinIO Parquet Pipeline (DHAP-42)

```bash
# 1. Copy the environment file and set credentials
cd DHAP-42/
cp .env.example .env          # edit MINIO_ROOT_PASSWORD / Airflow fernet key if needed

# 2. Build images and start the full stack (Airflow + MinIO + Postgres)
docker compose up -d --build
#    First build: ~5-10 min (pulls debian:12-slim, installs MinIO DEB + pip packages)

# 3. Open the services
#    Airflow UI  → http://localhost:8082   (admin / admin)
#    MinIO UI    → http://localhost:9021   (minioadmin / minioadmin123)

# 4. Trigger the pipeline DAG
#    In the Airflow UI: enable & trigger  csv_to_parquet_minio
#    Or via CLI:
docker compose exec airflow-scheduler airflow dags trigger csv_to_parquet_minio

# 5. Verify output in MinIO
#    Browse http://localhost:9021 → mailex-parquet bucket
#    Expected: 3 partitioned Parquet files (split=train / dev / test), 9,159 rows total

# 6. Run the negative test suite (all 9 schema-contract tests must pass)
docker compose exec airflow-scheduler python3 /opt/airflow/tests/run_negative_tests.py
```

See [`DHAP-42/README.md`](./DHAP-42/README.md) for the full runbook, architecture diagram, and verification checklist.

### DHAP-42 Key Facts

| Item | Detail |
|------|--------|
| Dataset | MailEx Email-Event-Extraction (EMNLP 2023) — 9,168 rows, 10 columns |
| Pipeline | `read_csv → validate_contract → transform → write_parquet_to_minio` |
| Object store | Real MinIO `RELEASE.2025-09-07T16-13-09Z` (web console on :9021) |
| Partitioning | By `split` column → `split=train / dev / test` Parquet files |
| Compression | Snappy |
| Idempotency | Existing partition objects deleted before each write |
| Port scheme | Airflow :8082 · MinIO API :9020 · MinIO console :9021 (no clash with DHAP-34) |

---

## Environment Setup

Each sub-project has its own `.env.example`. Copy it to `.env` and fill in values before running:

```bash
# DHAP-34
cp Email_Event_Extraction/.env.example Email_Event_Extraction/.env

# DHAP-42
cp DHAP-42/.env.example DHAP-42/.env
```

> **Note:** `.env` files are excluded by `.gitignore`. Raw dataset CSVs (large files) are also excluded and must be generated locally via the flatten script.

---

*Owner: Shruti Agarwal | Contact: shrutiagarwaljsr@gmail.com*
