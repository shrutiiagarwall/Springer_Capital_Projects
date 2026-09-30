# Springer Capital Projects

Internal data engineering and analytics projects for **Glynac AI / Springer Capital**.

## Repository Structure

| Folder | Jira | Description |
|---|---|---|
| [`Email_Event_Extraction/`](./Email_Event_Extraction/) | DHAP-34 | Airflow → PostgreSQL ingestion pipeline for the MailEx email event extraction dataset (Srivastava et al., EMNLP 2023) |

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

## Environment Setup

Each sub-project has its own `.env.example`. Copy it to `.env` and fill in values before running:

```bash
cp Email_Event_Extraction/.env.example Email_Event_Extraction/.env
# Edit .env with real credentials — never commit .env
```

> **Note:** `.env` files are excluded by `.gitignore`. Raw dataset CSVs (large files) are also excluded and must be generated locally via the flatten script.

---

*Owner: Shruti Agarwal | Contact: shrutiagarwaljsr@gmail.com*
