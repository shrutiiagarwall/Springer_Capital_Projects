"""
flatten_to_csv.py
=================
Pre-processing step for DHAP-34 (Glynac AI internship task).

Purpose
-------
The MailEx dataset (Srivastava et al., EMNLP 2023) ships as a collection of
JSON files — one per email thread — where each file encodes per-turn, per-
event-type annotations.  This script "flattens" that nested structure into a
single, tidy CSV that the Airflow DAG can ingest as if it were a spreadsheet
downloaded from SharePoint.

Output
------
data/raw/email_event_extraction.csv   — the canonical input for the pipeline
data/raw/email_event_extraction_bad.csv — intentionally broken CSV used to
                                          demonstrate validate_schema failing

Usage
-----
Run this script ONCE before starting `docker compose up`.  It is idempotent:
re-running it overwrites the output files.

    python scripts/flatten_to_csv.py

The script expects to be executed from the project root
(stage-1-csv-to-postgres/) OR from anywhere — it resolves paths
relative to its own location.

Dependencies: standard library only (json, csv, ast, os, pathlib).
"""

import ast
import csv
import json
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Path setup — resolve relative to this file so the script can be called
# from any working directory.
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent

# The raw dataset lives one level up from the project root:
# stage-1-csv-to-postgres/ sits inside Email_Event_Extraction_DHAP_34/,
# and data/ is a sibling folder: Email_Event_Extraction_DHAP_34/data/data/
DATA_ROOT = PROJECT_ROOT.parent / "data" / "data"

OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_CSV = OUTPUT_DIR / "email_event_extraction.csv"
BAD_CSV = OUTPUT_DIR / "email_event_extraction_bad.csv"

SPLITS = ["train", "dev", "test"]

CSV_COLUMNS = [
    "id",
    "thread_file",
    "split",
    "turn_id",
    "event_type",
    "trigger_text",
    "trigger_indices",
    "extras",
    "sentence_text",
    "num_arguments",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def parse_trigger(trigger_str: str) -> tuple[str, str]:
    """
    Parse a trigger string like "{'words': 'we need', 'indices': '0 1'}"
    into (trigger_text, trigger_indices).

    The dataset stores triggers as string representations of Python dicts.
    We use ast.literal_eval for safe parsing (no exec/eval).
    Returns ('', '') on any parse failure rather than crashing the whole run.
    """
    try:
        d = ast.literal_eval(trigger_str)
        return str(d.get("words", "")).strip(), str(d.get("indices", "")).strip()
    except (ValueError, SyntaxError):
        return trigger_str.strip(), ""


def count_non_o_tags(labels_list: list) -> int:
    """
    Count the total number of non-O BIO tags in a list-of-lists structure.

    The dataset stores labels as a list of per-sentence label lists.
    Each label is either "O" or a string like "Request_Action:B-Action Members".
    A token is counted as an argument token if its label is not exactly "O".
    """
    count = 0
    for sentence_labels in labels_list:
        for label in sentence_labels:
            if label != "O":
                count += 1
    return count


def reconstruct_sentence(sentences: list, turn_index: int) -> str:
    """
    Reconstruct the raw sentence text for a given turn by joining its tokens.
    The `sentences` array is parallel to turns (index 0 → turn_0, etc.).
    Returns empty string if the index is out of range.
    """
    try:
        tokens = sentences[turn_index]
        return " ".join(str(t) for t in tokens)
    except (IndexError, TypeError):
        return ""


def flatten_file(
    filepath: Path,
    split: str,
    row_id_counter: list,  # mutable list used as a simple counter reference
    writer: csv.DictWriter,
) -> int:
    """
    Parse one JSON file and write all trigger-level rows to the CSV writer.
    Returns the number of rows written.
    """
    rows_written = 0
    try:
        with open(filepath, encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as exc:
        print(f"  [WARN] Skipping {filepath.name}: {exc}")
        return 0

    events = data.get("events", {})
    sentences = data.get("sentences", [])
    thread_file = filepath.name

    for turn_id, turn_events in events.items():
        # turn_id is "turn_0", "turn_1", etc.
        # Extract the numeric index to look up the matching sentence.
        try:
            turn_index = int(turn_id.split("_")[1])
        except (IndexError, ValueError):
            turn_index = -1

        sentence_text = reconstruct_sentence(sentences, turn_index)

        for event_type, annotation in turn_events.items():
            triggers = annotation.get("triggers", [])
            extras_list = annotation.get("extras", [])
            labels = annotation.get("labels", [])
            num_args = count_non_o_tags(labels)

            if not triggers:
                # Some turns have event annotations with no triggers; emit one
                # placeholder row so we don't silently drop the annotation.
                row_id_counter[0] += 1
                writer.writerow(
                    {
                        "id": row_id_counter[0],
                        "thread_file": thread_file,
                        "split": split,
                        "turn_id": turn_id,
                        "event_type": event_type,
                        "trigger_text": None,
                        "trigger_indices": None,
                        "extras": extras_list[0].strip() if extras_list else None,
                        "sentence_text": sentence_text if sentence_text else None,
                        "num_arguments": num_args,
                    }
                )
                rows_written += 1
                continue

            for trig_idx, trigger_str in enumerate(triggers):
                trigger_text, trigger_indices = parse_trigger(trigger_str)
                # Align extras: use corresponding index if available, else first.
                extras_val = ""
                if extras_list:
                    extras_val = extras_list[trig_idx] if trig_idx < len(extras_list) else extras_list[0]

                row_id_counter[0] += 1
                writer.writerow(
                    {
                        "id": row_id_counter[0],
                        "thread_file": thread_file,
                        "split": split,
                        "turn_id": turn_id,
                        "event_type": event_type,
                        "trigger_text": trigger_text if trigger_text else None,
                        "trigger_indices": trigger_indices if trigger_indices else None,
                        "extras": extras_val.strip() if extras_val.strip() else None,
                        "sentence_text": sentence_text if sentence_text else None,
                        "num_arguments": num_args,
                    }
                )
                rows_written += 1

    return rows_written


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if not DATA_ROOT.exists():
        raise FileNotFoundError(
            f"Dataset root not found: {DATA_ROOT}\n"
            "Make sure the raw data folder (data/data/) is present relative "
            "to the project root as described in the README."
        )

    total_rows = 0
    id_counter = [0]  # mutable container so flatten_file can increment it

    print(f"Output => {OUTPUT_CSV}")
    print(f"Data root => {DATA_ROOT}")
    print()

    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for split in SPLITS:
            split_dir = DATA_ROOT / split
            if not split_dir.exists():
                print(f"[WARN] Split directory not found, skipping: {split_dir}")
                continue

            json_files = sorted(split_dir.glob("*.json"))
            print(f"Processing split={split!r}: {len(json_files)} files …")

            split_rows = 0
            for filepath in json_files:
                n = flatten_file(filepath, split, id_counter, writer)
                split_rows += n

            print(f"  => {split_rows} rows written for split={split!r}")
            total_rows += split_rows

    print(f"\nDone. Total rows: {total_rows}")
    print(f"CSV saved to: {OUTPUT_CSV}")


    # -----------------------------------------------------------------------
    # Generate the deliberately BAD CSV for validate_schema failure testing
    # -----------------------------------------------------------------------
    _generate_bad_csv(total_rows)


def _generate_bad_csv(total_rows: int) -> None:
    """
    Write a malformed CSV that is missing the required `event_type` column
    and has `num_arguments` as a string instead of an integer.

    This is used in the README / acceptance-criteria demonstration to show
    that the validate_schema DAG task fails loudly (raises ValueError) rather
    than silently accepting bad data.
    """
    BAD_COLUMNS = [
        "id",
        "thread_file",
        "split",
        "turn_id",
        # "event_type" is intentionally MISSING
        "trigger_text",
        "trigger_indices",
        "extras",
        "sentence_text",
        "num_arguments",  # will be written as a non-integer string
    ]

    with open(BAD_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=BAD_COLUMNS)
        writer.writeheader()
        writer.writerow(
            {
                "id": 1,
                "thread_file": "bad_file.json",
                "split": "train",
                "turn_id": "turn_0",
                # event_type omitted
                "trigger_text": "some trigger",
                "trigger_indices": "0 1",
                "extras": None,
                "sentence_text": "some sentence text here",
                "num_arguments": "NOT_AN_INTEGER",  # wrong dtype
            }
        )

    print(f"\nBad CSV (for failure-test) written to: {BAD_CSV}")
    print("  Missing column: event_type")
    print("  Bad dtype:      num_arguments = 'NOT_AN_INTEGER' (expected integer)")


if __name__ == "__main__":
    main()
