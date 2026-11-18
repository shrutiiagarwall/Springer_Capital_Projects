# Springer Capital Projects

Internal data engineering and analytics projects for **Springer Capital**.

---

## Repository Structure

```
Springer_Capital_Projects/
  Email_Event_Extraction_DHAP_34/   Stage 1 v1  Original pipeline layout
  Stage 1/                          Stage 1 v2  Restructured & verified pipeline (current)
  DHAP-42/                          Stage 2     CSV -> MinIO Parquet ingestion pipeline
```

| Folder | Stage | Version | Description |
|--------|-------|---------|-------------|
| [`Email_Event_Extraction_DHAP_34/`](./Email_Event_Extraction_DHAP_34/) | Stage 1 | v1 | Original Airflow + PostgreSQL ingestion pipeline for the MailEx dataset. First working version; kept for reference. |
| [`Stage 1/`](./Stage%201/) | Stage 1 | v2 (current) | Restructured, fully verified, and production-ready version of the same pipeline. See below for what changed. |
| [`DHAP-42/`](./DHAP-42/) | Stage 2 | current | Airflow DAG that reads the same MailEx CSV and writes partitioned Parquet files to a MinIO (pgsty/silo) S3-compatible object store. |

---

## Stage 1 — What changed between v1 and v2

`Email_Event_Extraction_DHAP_34/` was the first working version submitted for Stage 1.
`Stage 1/` is the **restructured and fully end-to-end verified** successor. It is the
version to use for any new work on this dataset.

| Area | Email_Event_Extraction_DHAP_34 (v1) | Stage 1 (v2 — current) |
|------|--------------------------------------|------------------------|
| **Folder layout** | `config/` holds YAML files; `sql/` holds DDL | Flattened into `email_event_extraction/` subfolder — manifest, schema, and DDL all in one place |
| **Path references** | Hardcoded to old `config/` and `sql/` paths | All paths updated and consistent throughout DAG, docker-compose, and scripts |
| **manifest.yaml status** | `status: validated` | Correctly reflects analysis status: `status: "."`, `status2: "pending"` |
| **DAG status awareness** | No status check | DAG reads `manifest.yaml` at runtime and logs a WARNING when `status2 = pending`; run still continues |
| **Event types** | 10 event types | 11 event types — added `Amend_Action_Data` (3 rows confirmed in data) |
| **DDL manual-run example** | Used `/docker-entrypoint-initdb.d/` path (incorrect) | Corrected to `docker compose exec -T postgres-target psql` via stdin |
| **BOM / encoding** | UTF-8 BOM present in several files | All files verified UTF-8 without BOM |
| **End-to-end verification** | Partial | Full 16-step verification completed: fresh-start test, idempotency test, failure/recovery test — all PASS |
| **Row count confirmed** | Not verified | **9,168 rows**, 12 event types, in `mailex_warehouse.email_events` |

---

## Quick Start — Stage 1 (current)

```bash
# 1. Generate the flat CSV from raw JSON (run once on your machine)
#    Requires: Python 3.9+  |  no extra packages
python "Stage 1/scripts/flatten_to_csv.py"
#    Output: Stage 1/email_event_extraction/data/raw/email_event_extraction.csv

# 2. Copy the env template — edit only if you want non-default ports/passwords
cp "Stage 1/.env.example" "Stage 1/.env"

# 3. Start the Docker stack
cd "Stage 1"
docker compose up -d

# 4. Wait ~60 s for Airflow to become healthy, then open:
#    http://localhost:8080  (login: admin / admin)

# 5. Trigger the DAG
docker compose exec airflow-scheduler \
  airflow dags trigger load_email_event_extraction_to_postgres

# 6. Verify rows in Postgres
docker compose exec postgres-target \
  psql -U airflow -d mailex_warehouse \
  -c "SELECT event_type, COUNT(*) FROM email_events GROUP BY event_type ORDER BY 2 DESC;"
#    Expected total: 9,168 rows across 12 event types
```

> See [`Stage 1/README.md`](./Stage%201/README.md) for the full runbook, architecture
> diagram, failure test, and troubleshooting guide.

---

## Quick Start — Stage 2 (CSV → MinIO Parquet)

```bash
# 1. Copy the env template
cd DHAP-42/
cp .env.example .env

# 2. Build and start the stack (Airflow + MinIO/Silo + Postgres metadata)
docker compose up -d --build
#    First build: ~5-10 min

# 3. Services
#    Airflow UI  ->  http://localhost:8082   (admin / admin)
#    MinIO UI    ->  http://localhost:9021   (minioadmin / minioadmin123)

# 4. Trigger the pipeline
docker compose exec airflow-scheduler airflow dags trigger csv_to_parquet_minio

# 5. Verify output — browse the MinIO console at http://localhost:9021
#    Expected: 3 partitioned Parquet files (split=train / dev / test), 9,159 rows total

# 6. Run the negative test suite
docker compose exec airflow-scheduler python3 /opt/airflow/tests/run_negative_tests.py
#    Expected: all 9 schema-contract tests pass
```

> See [`DHAP-42/README.md`](./DHAP-42/README.md) for the full runbook, architecture
> diagram, demo script, and verification checklist.

### Stage 2 Key Facts

| Item | Detail |
|------|--------|
| Dataset | MailEx Email-Event-Extraction (EMNLP 2023) — 9,168 rows, 10 columns |
| Pipeline | `read_csv -> validate_contract -> transform -> write_parquet_to_minio` |
| Object store | pgsty/silo (MinIO-compatible fork, web console on :9021) |
| Partitioning | By `split` column — `split=train / dev / test` Parquet files |
| Compression | Snappy |
| Idempotency | Existing partition objects deleted before each write |
| Ports | Airflow :8082 / MinIO API :9020 / MinIO console :9021 (no clash with Stage 1) |

---

## Environment Files

Each sub-project ships an `.env.example` with **placeholder values only** — no real
credentials are stored in this repository. Copy it to `.env` and fill in your own
values before running the stack:

```bash
# Stage 1
cp "Stage 1/.env.example" "Stage 1/.env"

# Stage 2
cp DHAP-42/.env.example DHAP-42/.env
```

> `.env` files are listed in each folder's `.gitignore` and are **never committed**.
> Raw dataset CSVs (large generated files) are also excluded and must be produced
> locally by running `flatten_to_csv.py`.

---

## Dataset

All pipelines use the **MailEx** dataset (Srivastava et al., EMNLP 2023):
- 1,500 email threads from the Enron corpus
- ~9,168 event instances across 12 event types
- 76 argument roles, BIO-tagged at token level

```bibtex
@inproceedings{srivastava-etal-2023-mailex,
    title     = "{M}ail{E}x: Email Event and Argument Extraction",
    author    = "Srivastava, Saurabh and Singh, Gaurav and Matsumoto, Shou
                 and Raz, Ali and Costa, Paulo and Poore, Joshua and Yao, Ziyu",
    booktitle = "Proceedings of the 2023 Conference on Empirical Methods
                 in Natural Language Processing",
    month     = dec,
    year      = "2023",
    address   = "Singapore",
    publisher = "Association for Computational Linguistics",
    url       = "https://aclanthology.org/2023.emnlp-main.801",
    doi       = "10.18653/v1/2023.emnlp-main.801",
    pages     = "12964--12987",
}
```

Dataset GitHub: <https://github.com/salokr/Email-Event-Extraction>
License: [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/)

---

*Owner: Shruti Agarwal | Springer Capital*
