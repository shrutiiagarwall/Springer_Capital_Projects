"""
generate_fixtures.py
====================
DHAP-42 — Negative Test Fixture Generator

Generates all CSV fixtures used by the negative tests.  Run this ONCE before
running run_negative_tests.py (or the Docker compose stack).

Fixtures produced
-----------------
  tests/bad_missing_column.csv          (a) — provided bad CSV spec:
                                              missing event_type + bad num_arguments
  tests/fixtures/b_renamed_column.csv   (b) — 'split' renamed to 'dataset_split'
  tests/fixtures/c_extra_column.csv     (c) — extra column 'foo' appended
  tests/fixtures/d_wrong_dtype.csv      (d) — num_arguments contains 'NOT_AN_INTEGER'
  tests/fixtures/e_null_nonnullable.csv (e) — thread_file has nulls
  tests/fixtures/f_invalid_event_type.csv (f) — event_type contains 'UNKNOWN_EVENT'
  tests/fixtures/g_invalid_split.csv    (g) — split contains 'validation'
  tests/fixtures/h_bad_trigger_indices.csv (h) — trigger_indices with bad format
  tests/fixtures/i_empty_file.csv       (i) — header only, no data rows

Each fixture is derived from the first 100 rows of the good CSV (except i).
Usage:
    python tests/generate_fixtures.py
"""

from __future__ import annotations

import sys
from pathlib import Path

# Resolve paths relative to THIS script's location
TESTS_DIR = Path(__file__).parent
PROJECT_ROOT = TESTS_DIR.parent
GOOD_CSV = PROJECT_ROOT / "dags" / "extraction" / "sample.csv"
FIXTURES_DIR = TESTS_DIR / "fixtures"


def load_sample(n: int = 100) -> "pd.DataFrame":
    """Load first n rows of the good CSV as all-strings."""
    import pandas as pd
    return pd.read_csv(GOOD_CSV, dtype=str, keep_default_na=False, nrows=n)


def generate_all() -> None:
    import pandas as pd

    FIXTURES_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Good CSV       : {GOOD_CSV}")
    print(f"Fixtures dir   : {FIXTURES_DIR}")
    print()

    # ── (a) bad_missing_column.csv ────────────────────────────────────────
    # Specification: missing event_type column + non-integer num_arguments.
    # Written to tests/bad_missing_column.csv (not fixtures/).
    out_a = TESTS_DIR / "bad_missing_column.csv"
    df = load_sample(100)
    df = df.drop(columns=["event_type"])
    df.loc[df.index[:5], "num_arguments"] = "NOT_AN_INTEGER"
    df.to_csv(out_a, index=False)
    print(f"[a] Written → {out_a}  ({len(df)} rows, columns={list(df.columns)})")

    # ── (b) renamed column ────────────────────────────────────────────────
    out_b = FIXTURES_DIR / "b_renamed_column.csv"
    df = load_sample(100)
    df = df.rename(columns={"split": "dataset_split"})
    df.to_csv(out_b, index=False)
    print(f"[b] Written → {out_b}  (renamed 'split' → 'dataset_split')")

    # ── (c) extra column ──────────────────────────────────────────────────
    out_c = FIXTURES_DIR / "c_extra_column.csv"
    df = load_sample(100)
    df["foo"] = "bar"
    df.to_csv(out_c, index=False)
    print(f"[c] Written → {out_c}  (extra column 'foo')")

    # ── (d) wrong dtype in num_arguments ──────────────────────────────────
    out_d = FIXTURES_DIR / "d_wrong_dtype.csv"
    df = load_sample(100)
    df.loc[df.index[10:15], "num_arguments"] = "NOT_AN_INTEGER"
    df.to_csv(out_d, index=False)
    print(f"[d] Written → {out_d}  (num_arguments rows 10-14 = 'NOT_AN_INTEGER')")

    # ── (e) null in non-nullable column (thread_file) ─────────────────────
    out_e = FIXTURES_DIR / "e_null_nonnullable.csv"
    df = load_sample(100)
    df.loc[df.index[5:10], "thread_file"] = ""   # empty string → treated as null
    df.to_csv(out_e, index=False)
    print(f"[e] Written → {out_e}  (thread_file rows 5-9 = empty/null)")

    # ── (f) invalid event_type value ──────────────────────────────────────
    out_f = FIXTURES_DIR / "f_invalid_event_type.csv"
    df = load_sample(100)
    df.loc[df.index[0:3], "event_type"] = "UNKNOWN_EVENT"
    df.to_csv(out_f, index=False)
    print(f"[f] Written → {out_f}  (event_type rows 0-2 = 'UNKNOWN_EVENT')")

    # ── (g) invalid split value ───────────────────────────────────────────
    out_g = FIXTURES_DIR / "g_invalid_split.csv"
    df = load_sample(100)
    df.loc[df.index[0:5], "split"] = "validation"
    df.to_csv(out_g, index=False)
    print(f"[g] Written → {out_g}  (split rows 0-4 = 'validation')")

    # ── (h) malformed trigger_indices ─────────────────────────────────────
    out_h = FIXTURES_DIR / "h_bad_trigger_indices.csv"
    df = load_sample(100)
    # Find rows where trigger_indices is not null and corrupt them
    non_null_mask = df["trigger_indices"].str.strip() != ""
    target_rows = df[non_null_mask].head(5).index
    df.loc[target_rows, "trigger_indices"] = "abc xyz"  # not space-separated ints
    df.to_csv(out_h, index=False)
    print(f"[h] Written → {out_h}  (trigger_indices corrupted to 'abc xyz')")

    # ── (i) empty file / header only ──────────────────────────────────────
    out_i = FIXTURES_DIR / "i_empty_file.csv"
    df = load_sample(0)   # just the header
    df.to_csv(out_i, index=False)
    print(f"[i] Written → {out_i}  (header only, 0 data rows)")

    print()
    print("All fixtures generated successfully.")
    print("Run tests/run_negative_tests.py to execute them against the live stack.")


if __name__ == "__main__":
    try:
        import pandas  # noqa: F401
    except ImportError:
        print("ERROR: pandas not installed. Run: pip install pandas", file=sys.stderr)
        sys.exit(1)

    generate_all()
