"""
csv_to_parquet_minio.py
=======================
Airflow DAG — Stage 2 (Springer Capital)

Purpose
-------
Ingest the MailEx Email-Event-Extraction CSV into MinIO as Snappy-compressed
Parquet files, partitioned by `split` (train / dev / test).

Task graph
----------
    read_csv ──► validate_contract ──► transform ──► write_parquet_to_minio

Task descriptions
-----------------
1. read_csv
   Reads the CSV (path overridable via dag_run.conf["csv_path"]).
   Writes a raw Parquet snapshot to /tmp/<run_id>/raw.parquet.
   Returns the temp-dir path for downstream tasks.

2. validate_contract
   Reads schema_contract.yaml (path overridable via conf["contract_path"]).
   Collects ALL violations before raising — never fail-fast on the first one.
   Raises AirflowFailException (retries=0) on any violation so downstream
   tasks are immediately marked upstream_failed.

   Checks performed (in order):
     a) File not empty (min_rows: 1)
     b) No extra columns              (allow_extra_columns: false)
     c) No missing columns
     d) Column order matches contract (enforce_column_order: true)
     e) Per-column: type (strict, no coercion), nullable, allowed_values,
        regex, min_value, non_empty
     f) Uniqueness on id

3. transform
   • Strip leading/trailing whitespace from all string columns.
   • Drop exact duplicates ignoring id (9 known duplicates in this dataset).
   • Keep legitimate nulls as nulls (do NOT coerce them away).
   • Enforce final dtypes: id → Int64, num_arguments → Int64.
   • Add audit columns: ingested_at (UTC ISO-8601), run_id.
   • Writes cleaned Parquet to /tmp/<run_id>/clean.parquet.

4. write_parquet_to_minio
   • Partitions by split → 3 objects:
       <bucket>/email_event_extraction/split=train/part-0.parquet
       <bucket>/email_event_extraction/split=dev/part-0.parquet
       <bucket>/email_event_extraction/split=test/part-0.parquet
   • Idempotency: deletes ALL existing objects under the prefix before
     writing, so re-running always yields exactly 3 objects.
   • Logs: rows read, duplicates dropped, rows per partition, object keys,
     object sizes.
   • Cleans up /tmp/<run_id>/ at the end.

Data transport
--------------
Tasks communicate via file paths in /tmp/<run_id>/  (NOT large XCom payloads).
The CSV is ~5.6 MB — too large for XCom's default 48 KB limit.
Temp dir is cleaned up by write_parquet_to_minio.

Connection
----------
MinIO is accessed via the 'minio_s3' Airflow connection, injected as:
  AIRFLOW_CONN_MINIO_S3=aws://<key>:<secret>@/?endpoint_url=<url>
The endpoint points to the Docker-internal MinIO service (http://minio:9000).

No module-level I/O
-------------------
All file reads happen inside @task callables — never at import time.
This prevents DAG-parse failures when files are not yet present.

Partitioning rationale
----------------------
`split` was chosen because:
  - Only 3 distinct values (train / dev / test) — manageable file count.
  - No nulls — Hive-style partitioning requires a non-null key.
  - Reasonably balanced (7,207 / 1,016 / 945 rows).
  - Downstream consumers typically query by split, enabling predicate push-down.
Rejected: `event_type` (12 unbalanced partitions), `thread_file` (1,500 files).

Schedule
--------
schedule=None — manual trigger only.  Re-run via UI or CLI.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from airflow.decorators import dag, task
from airflow.exceptions import AirflowFailException
from airflow.models.param import Param

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# DAG-level constants — no file I/O here
# ---------------------------------------------------------------------------
_DEFAULT_CSV_PATH = os.getenv(
    "MAILEX_CSV_PATH",
    "/opt/airflow/dags/extraction/sample.csv",
)
_DEFAULT_CONTRACT_PATH = os.getenv(
    "MAILEX_CONTRACT_PATH",
    "/opt/airflow/mailex_email_event_extraction/schema_contract.yaml",
)
_MINIO_CONN_ID = "minio_s3"
_MINIO_BUCKET = os.getenv("MINIO_BUCKET", "mailex-parquet")
_S3_PREFIX = "email_event_extraction"


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

@dag(
    dag_id="csv_to_parquet_minio",
    description=(
        "Stage 2 — Ingest MailEx Email-Event-Extraction CSV into MinIO "
        "as Snappy-compressed Parquet, partitioned by split."
    ),
    schedule=None,
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    tags=["stage-2", "mailex", "email-events", "parquet", "minio", "ingestion"],
    doc_md=__doc__,
    params={
        "csv_path": Param(
            _DEFAULT_CSV_PATH,
            type="string",
            description="Absolute path to the source CSV (inside the container).",
        ),
        "contract_path": Param(
            _DEFAULT_CONTRACT_PATH,
            type="string",
            description="Absolute path to schema_contract.yaml (inside the container).",
        ),
    },
)
def csv_to_parquet_minio() -> None:  # noqa: C901

    # -----------------------------------------------------------------------
    # Task 1: read_csv
    # -----------------------------------------------------------------------
    @task(task_id="read_csv", retries=0)
    def read_csv(**context: Any) -> str:
        """
        Load the source CSV into a pandas DataFrame and persist it as a raw
        Parquet file in a run-scoped temp directory.

        Returns
        -------
        str
            Absolute path to the temp directory (shared with downstream tasks).

        Raises
        ------
        AirflowFailException
            If the CSV does not exist or cannot be parsed.
        """
        import pandas as pd

        run_id: str = context["run_id"]
        params: dict = context["params"]
        csv_path = Path(params.get("csv_path", _DEFAULT_CSV_PATH))

        # Create a run-scoped temp directory (safe across concurrent runs)
        safe_run_id = run_id.replace(":", "_").replace("+", "_")
        tmp_dir = Path(f"/tmp/dhap42_{safe_run_id}")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        log.info("read_csv: temp directory = %s", tmp_dir)

        if not csv_path.exists():
            raise AirflowFailException(
                f"CSV not found: {csv_path}\n"
                "Check that dags/extraction/sample.csv is mounted into the container."
            )

        try:
            # Read everything as str first — strict type validation happens in validate_contract
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
        except Exception as exc:
            raise AirflowFailException(f"Failed to parse CSV: {exc}") from exc

        if df.empty:
            raise AirflowFailException(f"CSV is empty (no data rows): {csv_path}")

        log.info(
            "read_csv: loaded %d rows × %d columns from %s",
            len(df),
            len(df.columns),
            csv_path,
        )
        log.info("read_csv: columns = %s", list(df.columns))

        # Persist to raw Parquet (preserves all string values without dtype conversion)
        raw_parquet = tmp_dir / "raw.parquet"
        df.to_parquet(raw_parquet, index=False, engine="pyarrow", compression="snappy")
        log.info("read_csv: wrote raw snapshot → %s (%d bytes)", raw_parquet, raw_parquet.stat().st_size)

        return str(tmp_dir)

    # -----------------------------------------------------------------------
    # Task 2: validate_contract
    # -----------------------------------------------------------------------
    @task(task_id="validate_contract", retries=0)
    def validate_contract(tmp_dir: str, **context: Any) -> str:
        """
        Validate the raw Parquet snapshot against schema_contract.yaml.

        Collects ALL violations before raising — the caller sees the complete
        picture in one error message.

        Raises
        ------
        AirflowFailException
            On any violation.  retries=0 so the DAG run fails immediately
            without wasting retries on a data-quality problem.
        """
        import re

        import pandas as pd
        import yaml

        params: dict = context["params"]
        contract_path = Path(params.get("contract_path", _DEFAULT_CONTRACT_PATH))

        if not contract_path.exists():
            raise AirflowFailException(
                f"Schema contract not found: {contract_path}\n"
                "Is mailex_email_event_extraction/schema_contract.yaml mounted into the container?"
            )

        with open(contract_path, encoding="utf-8") as f:
            contract: dict = yaml.safe_load(f)

        raw_parquet = Path(tmp_dir) / "raw.parquet"
        df = pd.read_parquet(raw_parquet, engine="pyarrow")

        global_cfg: dict = contract.get("global", {})
        col_defs: list[dict] = contract.get("columns", [])
        expected_cols: list[str] = [c["name"] for c in col_defs]

        errors: list[str] = []

        # ── (a) min_rows ─────────────────────────────────────────────────────
        min_rows: int = global_cfg.get("min_rows", 1)
        if len(df) < min_rows:
            errors.append(
                f"EMPTY FILE: CSV has {len(df)} rows, minimum required is {min_rows}."
            )

        # ── (b) Extra columns ────────────────────────────────────────────────
        if global_cfg.get("allow_extra_columns", True) is False:
            extra_cols = [c for c in df.columns if c not in expected_cols]
            if extra_cols:
                errors.append(
                    f"EXTRA COLUMNS not allowed: {extra_cols}. "
                    "Remove them or update the contract."
                )

        # ── (c) Missing columns ──────────────────────────────────────────────
        missing_cols = [c for c in expected_cols if c not in df.columns]
        if missing_cols:
            errors.append(f"MISSING COLUMNS: {missing_cols}")

        # ── (d) Column order ─────────────────────────────────────────────────
        if global_cfg.get("enforce_column_order", False):
            actual_ordered = [c for c in df.columns if c in expected_cols]
            if actual_ordered != expected_cols:
                errors.append(
                    f"WRONG COLUMN ORDER.\n"
                    f"  Expected : {expected_cols}\n"
                    f"  Got      : {actual_ordered}"
                )

        # ── (e) Per-column checks ────────────────────────────────────────────
        for col_def in col_defs:
            col_name: str = col_def["name"]
            if col_name not in df.columns:
                continue  # already reported as MISSING

            series = df[col_name].copy()

            # Normalise empty strings → pd.NA so nullable checks work uniformly
            series = series.replace("", pd.NA)

            # ── Nullability ──────────────────────────────────────────────
            nullable: bool = col_def.get("nullable", True)
            null_count = series.isna().sum()
            if not nullable and null_count > 0:
                errors.append(
                    f"NULL VIOLATION: '{col_name}' is NOT NULL but has "
                    f"{null_count} missing/empty value(s)."
                )

            # Work on non-null values for subsequent checks
            non_null = series.dropna()

            # ── Type check ───────────────────────────────────────────────
            col_type: str = col_def.get("type", "string")
            if col_type == "integer":
                def _is_bad_int(v: str) -> bool:  # noqa: E306
                    try:
                        int(str(v).strip())
                        return False
                    except (ValueError, TypeError):
                        return True

                bad_mask = non_null.apply(_is_bad_int)
                if bad_mask.any():
                    bad_samples = non_null[bad_mask].head(5).tolist()
                    errors.append(
                        f"TYPE VIOLATION: '{col_name}' must be integer but contains "
                        f"non-integer values (first 5): {bad_samples}"
                    )

            # ── non_empty (string columns only) ─────────────────────────
            if col_def.get("non_empty", False):
                blank_count = (non_null.astype(str).str.strip() == "").sum()
                if blank_count > 0:
                    errors.append(
                        f"EMPTY VALUE: '{col_name}' must be non-empty but has "
                        f"{blank_count} blank value(s)."
                    )

            # ── allowed_values ───────────────────────────────────────────
            if "allowed_values" in col_def:
                allowed: set = set(col_def["allowed_values"])
                bad_vals = non_null[~non_null.astype(str).isin(allowed)].unique()
                if len(bad_vals) > 0:
                    errors.append(
                        f"INVALID VALUE: '{col_name}' has values not in allowed list "
                        f"{sorted(allowed)}: {bad_vals[:5].tolist()}"
                    )

            # ── regex ────────────────────────────────────────────────────
            if "regex" in col_def:
                pattern: str = col_def["regex"]
                compiled = re.compile(pattern)
                bad_mask = ~non_null.astype(str).str.match(compiled)
                bad_count = bad_mask.sum()
                if bad_count > 0:
                    bad_samples = non_null[bad_mask].head(5).tolist()
                    errors.append(
                        f"REGEX VIOLATION: '{col_name}' values must match '{pattern}' "
                        f"but {bad_count} do not. First 5: {bad_samples}"
                    )

            # ── min_value (numeric columns) ──────────────────────────────
            if "min_value" in col_def and col_type == "integer":
                min_val: int = col_def["min_value"]
                try:
                    numeric = pd.to_numeric(non_null, errors="coerce")
                    below_min = numeric[numeric < min_val]
                    if not below_min.empty:
                        errors.append(
                            f"MIN VALUE VIOLATION: '{col_name}' must be >= {min_val} "
                            f"but has {len(below_min)} value(s) below minimum: "
                            f"{below_min.head(5).tolist()}"
                        )
                except Exception:
                    pass  # type error already reported above

        # ── (f) Uniqueness ───────────────────────────────────────────────────
        for unique_col in global_cfg.get("unique", []):
            if unique_col in df.columns:
                dup_count = df[unique_col].duplicated().sum()
                if dup_count > 0:
                    dup_vals = df[df[unique_col].duplicated(keep=False)][unique_col].head(5).tolist()
                    errors.append(
                        f"UNIQUENESS VIOLATION: '{unique_col}' has {dup_count} "
                        f"duplicate value(s). First 5: {dup_vals}"
                    )

        # ── Final result ─────────────────────────────────────────────────────
        if errors:
            report = "\n".join(f"  [{i + 1}] {e}" for i, e in enumerate(errors))
            raise AirflowFailException(
                f"\n{'=' * 70}\n"
                f"SCHEMA CONTRACT VALIDATION FAILED — {len(errors)} violation(s):\n"
                f"{'=' * 70}\n"
                f"{report}\n"
                f"{'=' * 70}\n"
                f"Fix the CSV or update schema_contract.yaml and re-trigger the DAG.\n"
                f"NO DATA was written to MinIO.\n"
            )

        log.info(
            "validate_contract: PASSED — %d columns, %d rows checked against %s",
            len(col_defs),
            len(df),
            contract_path,
        )
        return tmp_dir

    # -----------------------------------------------------------------------
    # Task 3: transform
    # -----------------------------------------------------------------------
    @task(task_id="transform", retries=1)
    def transform(tmp_dir: str, **context: Any) -> str:
        """
        Clean and enrich the validated DataFrame.

        Steps:
        1. Strip leading/trailing whitespace from all string columns.
        2. Drop exact duplicate rows ignoring `id` (the 9 known duplicates).
           Keeps the first occurrence (lowest id) of each duplicate group.
        3. Enforce final dtypes: id → Int64, num_arguments → Int64.
           Legitimate nulls stay null; no sentinel values are introduced.
        4. Add audit columns:
             ingested_at — UTC timestamp of this DAG run (ISO-8601).
             run_id      — Airflow run_id for traceability.
        5. Write cleaned Parquet to /tmp/<run_id>/clean.parquet.

        Returns
        -------
        str
            Same tmp_dir (path to clean.parquet for the next task).
        """
        import pandas as pd

        run_id: str = context["run_id"]
        raw_parquet = Path(tmp_dir) / "raw.parquet"
        clean_parquet = Path(tmp_dir) / "clean.parquet"

        df = pd.read_parquet(raw_parquet, engine="pyarrow")
        original_len = len(df)

        # ── 1. Strip whitespace from all string/object columns ───────────────
        str_cols = df.select_dtypes(include=["object"]).columns
        for col in str_cols:
            # Replace empty strings that came from the CSV with None
            df[col] = df[col].str.strip().replace("", None)
        log.info("transform: stripped whitespace from %d string columns.", len(str_cols))

        # ── 2. Drop exact duplicates ignoring id ─────────────────────────────
        # Keep the first occurrence (lowest id) of each duplicate group.
        subset = [c for c in df.columns if c != "id"]
        df = df.drop_duplicates(subset=subset, keep="first")
        dup_dropped = original_len - len(df)
        if dup_dropped > 0:
            log.warning(
                "transform: dropped %d exact duplicate rows (ignoring id). "
                "This matches the 9 known duplicates in the dataset.",
                dup_dropped,
            )
        else:
            log.info("transform: no duplicate rows found.")

        # ── 3. Enforce final dtypes ──────────────────────────────────────────
        # Use pandas nullable integer types so NA stays NA (not NaN → float).
        df["id"] = pd.to_numeric(df["id"], errors="raise").astype("Int64")
        df["num_arguments"] = pd.to_numeric(df["num_arguments"], errors="raise").astype("Int64")

        # ── 4. Add audit columns ─────────────────────────────────────────────
        ingested_at = datetime.now(tz=timezone.utc).isoformat()
        df["ingested_at"] = ingested_at
        df["run_id"] = run_id

        log.info(
            "transform: %d rows in → %d rows out (%d duplicates dropped). "
            "Audit columns: ingested_at=%s, run_id=%s",
            original_len,
            len(df),
            dup_dropped,
            ingested_at,
            run_id,
        )

        # ── 5. Write clean Parquet ───────────────────────────────────────────
        df.to_parquet(clean_parquet, index=False, engine="pyarrow", compression="snappy")
        log.info(
            "transform: wrote clean snapshot → %s (%d bytes)",
            clean_parquet,
            clean_parquet.stat().st_size,
        )

        return tmp_dir

    # -----------------------------------------------------------------------
    # Task 4: write_parquet_to_minio
    # -----------------------------------------------------------------------
    @task(task_id="write_parquet_to_minio", retries=1)
    def write_parquet_to_minio(tmp_dir: str, **context: Any) -> None:
        """
        Write the cleaned DataFrame to MinIO as Hive-partitioned Parquet.

        Partition layout:
            <bucket>/email_event_extraction/split=train/part-0.parquet
            <bucket>/email_event_extraction/split=dev/part-0.parquet
            <bucket>/email_event_extraction/split=test/part-0.parquet

        Idempotency strategy:
            Before writing, ALL existing objects under the prefix
            <bucket>/email_event_extraction/ are deleted.  This means
            re-running the DAG always results in exactly 3 objects — no
            stale files from previous runs.

        Cleanup:
            Deletes /tmp/<run_id>/ after successful upload.
        """
        import io

        import boto3
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        from airflow.hooks.base import BaseHook

        clean_parquet = Path(tmp_dir) / "clean.parquet"
        df = pd.read_parquet(clean_parquet, engine="pyarrow")

        total_rows = len(df)
        log.info("write_parquet_to_minio: %d rows to write, partitioned by 'split'.", total_rows)

        # ── Resolve MinIO credentials from Airflow connection ────────────────
        conn = BaseHook.get_connection(_MINIO_CONN_ID)
        endpoint_url = conn.extra_dejson.get("endpoint_url", "http://minio:9000")
        aws_access_key = conn.login
        aws_secret_key = conn.password

        s3 = boto3.client(
            "s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name="us-east-1",
        )
        bucket = _MINIO_BUCKET

        # ── Idempotency: delete existing objects under prefix ────────────────
        prefix = f"{_S3_PREFIX}/"
        log.info(
            "write_parquet_to_minio: deleting existing objects under s3://%s/%s",
            bucket,
            prefix,
        )
        paginator = s3.get_paginator("list_objects_v2")
        objects_to_delete = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects_to_delete.append({"Key": obj["Key"]})

        if objects_to_delete:
            s3.delete_objects(
                Bucket=bucket,
                Delete={"Objects": objects_to_delete, "Quiet": True},
            )
            log.info(
                "write_parquet_to_minio: deleted %d existing object(s).",
                len(objects_to_delete),
            )
        else:
            log.info("write_parquet_to_minio: no existing objects to delete.")

        # ── Write one Parquet file per split value ───────────────────────────
        partition_col = "split"
        split_values = df[partition_col].unique()
        written_summary: list[str] = []

        for split_val in sorted(split_values):
            partition_df = df[df[partition_col] == split_val].copy()
            n_rows = len(partition_df)

            # Serialise to in-memory Parquet buffer (snappy compressed)
            table = pa.Table.from_pandas(partition_df, preserve_index=False)
            buf = io.BytesIO()
            pq.write_table(table, buf, compression="snappy")
            buf.seek(0)
            parquet_bytes = buf.getvalue()

            # Hive-style key
            object_key = f"{_S3_PREFIX}/{partition_col}={split_val}/part-0.parquet"
            s3.put_object(
                Bucket=bucket,
                Key=object_key,
                Body=parquet_bytes,
                ContentType="application/octet-stream",
            )
            size_kb = len(parquet_bytes) / 1024
            log.info(
                "write_parquet_to_minio: uploaded s3://%s/%s  (%d rows, %.1f KB)",
                bucket,
                object_key,
                n_rows,
                size_kb,
            )
            written_summary.append(
                f"  split={split_val:6s}  {n_rows:5d} rows  {size_kb:7.1f} KB  "
                f"s3://{bucket}/{object_key}"
            )

        # ── Summary log ─────────────────────────────────────────────────────
        log.info(
            "\n%s\nWRITE SUMMARY — csv_to_parquet_minio\n%s\n"
            "  Total rows written : %d\n"
            "  Partitions         : %d\n"
            "%s\n%s",
            "=" * 70,
            "=" * 70,
            total_rows,
            len(split_values),
            "\n".join(written_summary),
            "=" * 70,
        )

        # ── Cleanup temp directory ───────────────────────────────────────────
        import shutil
        shutil.rmtree(tmp_dir, ignore_errors=True)
        log.info("write_parquet_to_minio: cleaned up temp dir %s", tmp_dir)

    # -----------------------------------------------------------------------
    # Wire the task graph
    # -----------------------------------------------------------------------
    raw_dir = read_csv()
    validated_dir = validate_contract(raw_dir)
    cleaned_dir = transform(validated_dir)
    write_parquet_to_minio(cleaned_dir)


# Instantiate the DAG
csv_to_parquet_minio()
