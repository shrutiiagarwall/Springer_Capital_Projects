"""idempotency_check.py — Verify S3 still has exactly 3 objects after 2 DAG runs."""
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin123",
    region_name="us-east-1",
)

bucket = "mailex-parquet"
prefix = "email_event_extraction/"

resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
objects = resp.get("Contents", [])

print(f"Object count after 2 successful DAG runs: {len(objects)}")
for obj in objects:
    print(f"  {obj['Key']}  ({obj['Size']:,} bytes)")

assert len(objects) == 3, f"IDEMPOTENCY FAILED: expected 3 objects, got {len(objects)}"
print("\nIDEMPOTENCY PASSED — exactly 3 partition files, no accumulation across runs.")
