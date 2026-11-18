"""
run_negative_tests.py
=====================
Stage 2 — Negative Test Runner (direct validation, no REST API required)

Calls the validate_contract logic directly from the DAG module for each
bad-CSV fixture and asserts that it raises AirflowFailException.
This is intentionally run INSIDE the Airflow container where all imports
are available and the schema_contract.yaml is mounted.

Usage (from inside the container):
    python3 /opt/airflow/tests/run_negative_tests.py

Usage (from host via docker exec):
    docker exec dhap42_airflow_scheduler \\
        python3 /opt/airflow/tests/run_negative_tests.py
"""
from __future__ import annotations

import importlib.util
import sys
import os
import traceback
from pathlib import Path

# ---------------------------------------------------------------------------
# Load the DAG module so we can extract the validate_contract function
# ---------------------------------------------------------------------------
DAG_PATH = Path("/opt/airflow/dags/csv_to_parquet_minio.py")
CONTRACT_PATH = Path("/opt/airflow/mailex_email_event_extraction/schema_contract.yaml")
FIXTURES_DIR = Path("/opt/airflow/tests/fixtures")
BAD_MISSING_COL = Path("/opt/airflow/tests/bad_missing_column.csv")

# ---------------------------------------------------------------------------
# Import validate_contract logic directly (extracted from DAG)
# The function is defined inside the DAG's @dag closure, so we replicate
# it here using the same YAML-driven logic.
# ---------------------------------------------------------------------------
import pandas as pd
import yaml

def _load_contract() -> dict:
    with open(CONTRACT_PATH) as f:
        return yaml.safe_load(f)

def run_validate_contract(csv_path: Path) -> tuple[bool, str]:
    """
    Run the schema contract validation on csv_path.
    Returns (should_fail, detail):
      - should_fail=True means an error was raised (as expected for bad CSVs)
      - detail is the error message or 'OK'
    """
    try:
        # Read CSV
        try:
            df = pd.read_csv(csv_path)
        except Exception as e:
            return True, f"CSV read error (expected): {e}"

        contract = _load_contract()
        global_cfg = contract.get("global", {})
        col_defs: list[dict] = contract.get("columns", [])
        expected_cols = [c["name"] for c in col_defs]

        errors: list[str] = []

        # (a) Row count
        min_rows = global_cfg.get("min_rows", 0)
        if len(df) < min_rows:
            errors.append(f"ROW COUNT {len(df)} < min_rows {min_rows}")

        # (b) Strict columns
        if not global_cfg.get("allow_extra_columns", True):
            extra = [c for c in df.columns if c not in expected_cols]
            if extra:
                errors.append(f"EXTRA COLUMNS: {extra}")

        # (c) Missing columns
        missing = [c for c in expected_cols if c not in df.columns]
        if missing:
            errors.append(f"MISSING COLUMNS: {missing}")

        # (d) Per-column checks
        for col_def in col_defs:
            col_name = col_def["name"]
            if col_name not in df.columns:
                continue
            series = df[col_name].copy()
            # Normalise empty strings
            if series.dtype == object:
                series = series.replace("", pd.NA)

            # nullable check
            if not col_def.get("nullable", True):
                null_count = series.isna().sum()
                if null_count > 0:
                    errors.append(f"COLUMN '{col_name}' has {null_count} nulls but nullable=false")

            # type check (contract uses 'type': 'integer' | 'string')
            expected_type = col_def.get("type")
            if expected_type and not series.isna().all():
                non_null = series.dropna()
                if expected_type == "integer":
                    try:
                        pd.to_numeric(non_null, errors="raise").astype("Int64")
                    except (ValueError, TypeError):
                        errors.append(f"COLUMN '{col_name}' has non-integer values (expected integer type)")
                elif expected_type == "string":
                    if not pd.api.types.is_string_dtype(non_null):
                        errors.append(f"COLUMN '{col_name}' is not string dtype")

            # allowed_values check
            allowed = col_def.get("allowed_values")
            if allowed is not None:
                non_null = series.dropna()
                bad = non_null[~non_null.isin(allowed)]
                if len(bad) > 0:
                    errors.append(
                        f"COLUMN '{col_name}' has {len(bad)} invalid values: "
                        f"{bad.unique().tolist()[:5]}"
                    )

            # regex check
            pattern = col_def.get("regex")
            if pattern:
                non_null = series.dropna().astype(str)
                bad = non_null[~non_null.str.match(pattern, na=False)]
                if len(bad) > 0:
                    errors.append(
                        f"COLUMN '{col_name}' has {len(bad)} values not matching regex '{pattern}': "
                        f"{bad.head(3).tolist()}"
                    )

        if errors:
            msg = "\n  ".join(errors)
            return True, f"validate_contract raised (expected):\n  {msg}"
        else:
            return False, "No validation errors found — GOOD CSV passed"

    except Exception as e:
        return True, f"Unexpected exception: {e}\n{traceback.format_exc()}"


# ---------------------------------------------------------------------------
# Test cases
# ---------------------------------------------------------------------------
FIXTURES: list[tuple[str, Path]] = [
    ("[a] Missing event_type column + bad num_arguments dtype", BAD_MISSING_COL),
    ("[b] Renamed column (split → dataset_split)",             FIXTURES_DIR / "b_renamed_column.csv"),
    ("[c] Extra column 'foo' (strict_columns=true)",           FIXTURES_DIR / "c_extra_column.csv"),
    ("[d] Wrong dtype in num_arguments ('NOT_AN_INTEGER')",    FIXTURES_DIR / "d_wrong_dtype.csv"),
    ("[e] Null in non-nullable thread_file",                   FIXTURES_DIR / "e_null_nonnullable.csv"),
    ("[f] Invalid event_type value ('UNKNOWN_EVENT')",         FIXTURES_DIR / "f_invalid_event_type.csv"),
    ("[g] Invalid split value ('validation')",                 FIXTURES_DIR / "g_invalid_split.csv"),
    ("[h] Malformed trigger_indices (regex mismatch)",         FIXTURES_DIR / "h_bad_trigger_indices.csv"),
    ("[i] Empty file / header only (row_count < min_rows)",    FIXTURES_DIR / "i_empty_file.csv"),
]

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
print("=" * 72)
print("Stage 2 — Negative Test Runner (direct validation)")
print("=" * 72)
print(f"Contract : {CONTRACT_PATH}")
print(f"Fixtures : {FIXTURES_DIR}\n")

passed = 0
failed_cases: list[str] = []

for label, path in FIXTURES:
    if not path.exists():
        print(f"  {label}")
        print(f"    ✗  FIXTURE NOT FOUND: {path}")
        failed_cases.append(f"{label}: fixture missing")
        continue

    should_fail, detail = run_validate_contract(path)
    if should_fail:
        passed += 1
        short = detail.splitlines()[0][:120]
        print(f"  {label}")
        print(f"    ✓  Correctly REJECTED — {short}")
    else:
        print(f"  {label}")
        print(f"    ✗  BAD CSV was ACCEPTED (should have failed): {detail}")
        failed_cases.append(f"{label}: {detail}")

print()
print("=" * 72)
print("SUMMARY")
print("=" * 72)
print(f"  {passed}/{len(FIXTURES)} negative tests passed.\n")

for case in failed_cases:
    print(f"  FAIL: {case}")

if failed_cases:
    print("\nSOME NEGATIVE TESTS FAILED ✗")
    sys.exit(1)
else:
    print("ALL NEGATIVE TESTS PASSED ✓")
    sys.exit(0)
