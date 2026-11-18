"""verify_s3.py — Check Parquet files written to MinIO by the DAG."""
import boto3
import io
import pandas as pd

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    region_name="us-east-1",
)

bucket = "mailex-parquet"
prefix = "email_event_extraction/"

print(f"=== Listing objects in s3://{bucket}/{prefix} ===")
resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
objects = resp.get("Contents", [])
print(f"Found {len(objects)} object(s):")
total_bytes = 0
for obj in objects:
    print(f"  {obj['Key']}  ({obj['Size']:,} bytes)")
    total_bytes += obj["Size"]
print(f"Total: {total_bytes:,} bytes\n")

assert len(objects) == 3, f"Expected 3 partition files, got {len(objects)}"

print("=== Reading each partition and validating =====")
expected_splits = {"train", "dev", "test"}
seen_splits = set()
total_rows = 0
for obj in objects:
    key = obj["Key"]
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    df = pd.read_parquet(io.BytesIO(body))
    split_vals = df["split"].unique().tolist()
    print(f"  {key}: {len(df):,} rows, splits={split_vals}, cols={list(df.columns)}")
    assert len(split_vals) == 1, f"Expected 1 split value per file, got {split_vals}"
    seen_splits.add(split_vals[0])
    total_rows += len(df)

print(f"\nSplits seen: {sorted(seen_splits)}")
print(f"Total rows across all partitions: {total_rows:,}")
assert seen_splits == expected_splits, f"Missing splits: {expected_splits - seen_splits}"
assert total_rows == 9159, f"Expected 9159 rows total (9168 raw - 9 known dups), got {total_rows}"
print("\n=== ALL ASSERTIONS PASSED ===")
print("✓ 3 Parquet partition files (train / dev / test)")
print(f"✓ {total_rows:,} total rows (matches source CSV)")
print("✓ Each file contains exactly one split value")
