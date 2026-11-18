"""trigger_bad_csv.py — trigger the DAG with the bad CSV to test validate_contract failure."""
import subprocess, sys

result = subprocess.run(
    [
        "airflow", "dags", "trigger", "csv_to_parquet_minio",
        "--conf", '{"csv_path": "/opt/airflow/tests/bad_missing_column.csv"}',
    ],
    capture_output=True, text=True
)
print(result.stdout)
print(result.stderr, file=sys.stderr)
sys.exit(result.returncode)
