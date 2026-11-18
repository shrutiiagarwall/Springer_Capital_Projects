"""
load_email_event_extraction_to_postgres.py
==========================================
Airflow DAG — DHAP-34 (Glynac AI)

Purpose
-------
Ingest the pre-flattened MailEx CSV (produced by scripts/flatten_to_csv.py)
into the email_events table on the postgres-target warehouse database.

Task graph
----------
    read_csv  ──►  validate_schema  ──►  transform  ──►  load_to_postgres

Task descriptions
-----------------
1. read_csv
   Loads data/raw/email_event_extraction.csv with pandas.  Returns the
   DataFrame serialised to a JSON string (XCom transport).

   Also reads email_event_extraction/manifest.yaml and logs the recorded
   dataset analysis status.  If status2 is "pending", a WARNING is logged
   to alert the analyst that dataset analysis is still in progress, but
   the pipeline run continues normally (see "Dataset status" section in
   the README for details).

2. validate_schema
   Reads email_event_extraction/schema.yaml and verifies:
     • All required columns are present.
     • Integer columns contain only integers (no strings, no NaN for non-
       nullable columns).
     • Not-nullable columns have no missing values.
   Raises ValueError loudly on any mismatch — the DAG run fails immediately,
   no silent coercion.

3. transform
   Applies cleaning rules:
     • Strip leading/trailing whitespace from all string columns.
     • Drop exact duplicate rows (same values in every column).
     • Normalize empty strings ("", "  ") to NULL/None so they land as SQL
       NULL in Postgres.
   Returns cleaned DataFrame as JSON string.

4. load_to_postgres
   Loads the cleaned DataFrame into email_events using TRUNCATE + INSERT.

   Design choice — TRUNCATE then INSERT (not upsert):
     The MailEx dataset is a static research corpus.  Its content does not
     change incrementally; when it is updated (e.g. a corrected annotation),
     the canonical action is to re-flatten and re-load the whole table.
     TRUNCATE + INSERT is therefore:
       • Simpler (no conflict-key logic to maintain)
       • Faster for bulk loads (no per-row conflict checks)
       • Correct by construction (re-running the DAG always yields a clean,
         non-duplicated table)
     This makes re-running safe: idempotent by definition.

Connection
----------
The PostgresHook uses connection_id="postgres_target".  This Connection is
created automatically by airflow-init (see docker-compose.yaml).  It can
also be set via the environment variable AIRFLOW_CONN_POSTGRES_TARGET.

No module-level I/O
-------------------
All file reads (CSV, YAML) happen inside the @task callables, not at the
module level.  This prevents DAG parse-time failures when the files are not
yet present (e.g. before flatten_to_csv.py has been run).

Schedule
--------
schedule=None  — manual trigger only.  This is a one-shot research dataset
load; no automated schedule makes sense.  Re-run via UI or CLI when the CSV
changes.
"""

from __future__ import annotations

import io
import logging
import os
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level constants — no file I/O here, just string paths
# ---------------------------------------------------------------------------
# schema.yaml and manifest.yaml live in email_event_extraction/
_DEFAULT_CSV_PATH      = "/opt/airflow/data/raw/email_event_extraction.csv"
_DEFAULT_SCHEMA_PATH   = "/opt/airflow/email_event_extraction/schema.yaml"
_DEFAULT_MANIFEST_PATH = "/opt/airflow/email_event_extraction/manifest.yaml"
_TARGET_CONN_ID = "postgres_target"
_TARGET_TABLE   = "email_events"

# Read paths from environment so they can be overridden in tests or alternate runs.
# The env vars are set in docker-compose.yaml; they fall back to the defaults above.
CSV_PATH      = os.getenv("MAILEX_CSV_PATH",      _DEFAULT_CSV_PATH)
SCHEMA_PATH   = os.getenv("MAILEX_SCHEMA_PATH",   _DEFAULT_SCHEMA_PATH)
MANIFEST_PATH = os.getenv("MAILEX_MANIFEST_PATH", _DEFAULT_MANIFEST_PATH)


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="load_email_event_extraction_to_postgres",
    description=(
        "DHAP-34 — Load the MailEx Email-Event-Extraction dataset CSV into "
        "the email_events table on postgres-target."
    ),
    schedule=None,          # manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=["dhap-34", "mailex", "email-events", "ingestion"],
    doc_md=__doc__,
)
def load_email_event_extraction():

    # -----------------------------------------------------------------------
    # Task 1: read_csv
    # -----------------------------------------------------------------------
    @task(task_id="read_csv")
    def read_csv() -> str:
        """
        Load the flattened CSV into a pandas DataFrame and return it
        serialised as a JSON string for XCom transport.

        Also reads manifest.yaml and logs the dataset analysis status.
        If status2 == "pending", a WARNING is emitted but the run continues.

        Raises
        ------
        FileNotFoundError   if the CSV does not exist.
        pd.errors.EmptyDataError  if the CSV has no rows.
        """
        import pandas as pd
        import yaml

        # -- Read and log manifest status ------------------------------------
        manifest_path = Path(MANIFEST_PATH)
        if manifest_path.exists():
            with open(manifest_path, encoding="utf-8") as mf:
                manifest = yaml.safe_load(mf)
            status  = manifest.get("status",  "")
            status2 = manifest.get("status2", "")
            log.info(
                "read_csv: manifest status=%r  status2=%r  (dataset: %s)",
                status,
                status2,
                manifest.get("dataset_name", "unknown"),
            )
            if str(status2).strip().lower() == "pending":
                log.warning(
                    "DATASET STATUS WARNING: manifest.yaml reports status2='pending'. "
                    "Dataset analysis for '%s' is still in progress. "
                    "The pipeline will continue, but analytical results should be "
                    "treated as preliminary until the status is updated.",
                    manifest.get("dataset_name", "unknown"),
                )
        else:
            log.warning(
                "read_csv: manifest.yaml not found at %s — skipping status check.",
                manifest_path,
            )

        # -- Load CSV --------------------------------------------------------
        csv_path = Path(CSV_PATH)
        if not csv_path.exists():
            raise FileNotFoundError(
                f"CSV not found at {csv_path}.\n"
                "Have you run scripts/flatten_to_csv.py first?\n"
                "See the README — Step 0."
            )

        df = pd.read_csv(csv_path, dtype=str)  # read everything as str initially
        log.info("read_csv: loaded %d rows, %d columns from %s", len(df), len(df.columns), csv_path)
        log.info("Columns: %s", list(df.columns))

        if df.empty:
            raise ValueError(f"CSV at {csv_path} is empty — nothing to ingest.")

        # Serialise to JSON for XCom.  orient='records' keeps column names.
        return df.to_json(orient="records")

    # -----------------------------------------------------------------------
    # Task 2: validate_schema
    # -----------------------------------------------------------------------
    @task(task_id="validate_schema")
    def validate_schema(df_json: str) -> str:
        """
        Validate the DataFrame against email_event_extraction/schema.yaml.

        Checks performed
        ----------------
        1. All schema-defined columns are present in the CSV.
        2. Not-nullable columns have no missing / empty values.
        3. Integer-typed columns can be cast to int (raises if they cannot).

        Raises
        ------
        ValueError  with a descriptive message listing all violations found.
                    The DAG run fails immediately; no silent coercion.
        """
        import pandas as pd
        import yaml

        schema_path = Path(SCHEMA_PATH)
        if not schema_path.exists():
            raise FileNotFoundError(
                f"Schema file not found at {schema_path}. "
                "Is the email_event_extraction/ directory mounted into the container?"
            )

        with open(schema_path, encoding="utf-8") as f:
            schema = yaml.safe_load(f)

        df = pd.read_json(io.StringIO(df_json), orient="records", dtype=str)
        errors: list[str] = []

        for col_def in schema["columns"]:
            col_name = col_def["name"]
            col_type = col_def["type"]
            nullable = col_def.get("nullable", True)

            # ── 1. Column presence ──────────────────────────────────────────
            if col_name not in df.columns:
                errors.append(f"MISSING COLUMN: '{col_name}' not found in CSV.")
                continue  # can't check further for a missing column

            series = df[col_name]

            # ── 2. Nullability ──────────────────────────────────────────────
            if not nullable:
                null_mask = series.isna() | (series.astype(str).str.strip() == "")
                null_count = null_mask.sum()
                if null_count > 0:
                    errors.append(
                        f"NULL VIOLATION: Column '{col_name}' is NOT NULL but "
                        f"has {null_count} missing/empty value(s)."
                    )

            # ── 3. Type check (integers) ────────────────────────────────────
            if col_type == "integer":
                non_null = series.dropna()
                non_null = non_null[non_null.astype(str).str.strip() != ""]
                try:
                    non_null.astype(str).str.strip().astype(int)
                except (ValueError, TypeError) as exc:
                    # Find a sample of bad values for the error message
                    def _is_bad(v: str) -> bool:
                        try:
                            int(str(v).strip())
                            return False
                        except (ValueError, TypeError):
                            return True

                    bad_vals = non_null[non_null.apply(_is_bad)].head(3).tolist()
                    errors.append(
                        f"TYPE VIOLATION: Column '{col_name}' must be integer but "
                        f"contains non-integer values: {bad_vals}. "
                        f"(Original error: {exc})"
                    )

        if errors:
            error_report = "\n".join(f"  • {e}" for e in errors)
            raise ValueError(
                f"Schema validation FAILED with {len(errors)} error(s):\n"
                f"{error_report}\n\n"
                f"Fix the upstream CSV or schema.yaml and re-run the DAG."
            )

        log.info(
            "validate_schema: PASSED — %d columns checked against %s",
            len(schema["columns"]),
            schema_path,
        )
        return df_json  # pass through unchanged

    # -----------------------------------------------------------------------
    # Task 3: transform
    # -----------------------------------------------------------------------
    @task(task_id="transform")
    def transform(df_json: str) -> str:
        """
        Apply cleaning rules to the validated DataFrame:

        1. Strip whitespace from all string/object columns.
        2. Drop exact duplicate rows (all columns identical).
        3. Normalise empty strings ("", "  ", "None", "nan") → None (SQL NULL).
        4. Cast typed columns to their correct Python types so the loader
           can pass correct values to psycopg2.

        Returns the cleaned DataFrame as a JSON string.
        """
        import numpy as np
        import pandas as pd

        df = pd.read_json(io.StringIO(df_json), orient="records", dtype=str)
        original_len = len(df)

        # ── 1. Strip whitespace ─────────────────────────────────────────────
        str_cols = df.select_dtypes(include=["object"]).columns
        for col in str_cols:
            df[col] = df[col].str.strip()

        # ── 2. Drop exact duplicates ────────────────────────────────────────
        df = df.drop_duplicates()
        dup_dropped = original_len - len(df)
        if dup_dropped:
            log.warning("transform: dropped %d exact duplicate rows.", dup_dropped)

        # ── 3. Normalise empty strings / "None" / "nan" → actual None ───────
        EMPTY_SENTINELS = {"", "none", "nan", "null", "n/a"}
        for col in df.columns:
            mask = df[col].str.lower().isin(EMPTY_SENTINELS)
            df.loc[mask, col] = None

        # ── 4. Type casting for integer columns ─────────────────────────────
        # Casting after NULL-normalisation so we don't trip on empty strings.
        # Use pandas nullable Int64 so NaN is preserved without converting to float.
        df["id"] = pd.array(pd.to_numeric(df["id"], errors="raise"), dtype="Int64")
        df["num_arguments"] = pd.array(
            pd.to_numeric(df["num_arguments"], errors="raise"), dtype="Int64"
        )

        log.info(
            "transform: %d rows in => %d rows out (%d duplicates dropped).",
            original_len,
            len(df),
            dup_dropped,
        )
        return df.to_json(orient="records")

    # -----------------------------------------------------------------------
    # Task 4: load_to_postgres
    # -----------------------------------------------------------------------
    @task(task_id="load_to_postgres")
    def load_to_postgres(df_json: str) -> None:
        """
        Load the cleaned DataFrame into the email_events table via PostgresHook.

        Strategy: TRUNCATE then INSERT (see module docstring for rationale).

        The table is truncated first so re-running the DAG never creates
        duplicate rows.  The entire INSERT is wrapped in a single transaction:
        if any INSERT fails, the TRUNCATE is rolled back and the table keeps
        its previous contents.

        Connection
        ----------
        Uses the 'postgres_target' Airflow Connection (set by airflow-init or
        via the AIRFLOW_CONN_POSTGRES_TARGET environment variable).
        """
        import pandas as pd
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        df = pd.read_json(io.StringIO(df_json), orient="records")

        hook = PostgresHook(postgres_conn_id=_TARGET_CONN_ID)
        conn = hook.get_conn()

        # Build the INSERT statement with positional placeholders
        columns = [
            "id", "thread_file", "split", "turn_id", "event_type",
            "trigger_text", "trigger_indices", "extras",
            "sentence_text", "num_arguments",
        ]
        placeholders = ", ".join(["%s"] * len(columns))
        col_list = ", ".join(columns)
        insert_sql = (
            f"INSERT INTO {_TARGET_TABLE} ({col_list}) VALUES ({placeholders})"
        )

        # Prepare rows — convert pandas NA → Python None for psycopg2
        def _val(v):
            """Convert pandas NA / numpy nan to None."""
            import math
            if v is None:
                return None
            try:
                if math.isnan(float(v)):
                    return None
            except (TypeError, ValueError):
                pass
            return v

        rows = [
            tuple(_val(row[col]) for col in columns)
            for _, row in df.iterrows()
        ]

        try:
            with conn:
                with conn.cursor() as cur:
                    log.info(
                        "load_to_postgres: TRUNCATE %s (idempotent reset).",
                        _TARGET_TABLE,
                    )
                    cur.execute(f"TRUNCATE TABLE {_TARGET_TABLE} RESTART IDENTITY;")

                    log.info(
                        "load_to_postgres: Inserting %d rows into %s ...",
                        len(rows),
                        _TARGET_TABLE,
                    )
                    cur.executemany(insert_sql, rows)
                    log.info("load_to_postgres: INSERT complete.")
        finally:
            conn.close()

        log.info(
            "load_to_postgres: SUCCESS — %d rows loaded into %s.",
            len(rows),
            _TARGET_TABLE,
        )

    # -----------------------------------------------------------------------
    # Wire the task graph
    # -----------------------------------------------------------------------
    raw = read_csv()
    validated = validate_schema(raw)
    cleaned = transform(validated)
    load_to_postgres(cleaned)


# Instantiate the DAG
load_email_event_extraction()
