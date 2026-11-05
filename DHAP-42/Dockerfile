# Dockerfile
# DHAP-42 — Custom Airflow image with data-engineering dependencies
# ---------------------------------------------------------------------------
# Extends the official Apache Airflow 2.9.3 image (Python 3.11) and adds:
#   - pandas           2.1.4   — CSV/DataFrame operations
#   - pyarrow          16.1.0  — Parquet read/write
#   - PyYAML           6.0.1   — schema_contract.yaml parsing
#   - boto3            ≥1.34.90 — S3-compatible object-store client
#   - moto[s3,server]  5.x     — S3-compatible local server (replaces MinIO
#                                when Docker Hub is unavailable; same boto3 API)
#
# Dependency note:
#   moto's transitive dep cfn-lint forces cryptography to 50.x.
#   The base Airflow image ships pyOpenSSL 24.1.0 which breaks at runtime
#   with cryptography ≥ 42 (AttributeError: GEN_EMAIL).
#   Fix: upgrade pyOpenSSL to ≥24.2.0 in the SAME pip invocation so the
#   resolver picks a version that is jointly compatible with cryptography 50.x.
#
# We install as the 'airflow' user (the image's default non-root user) so
# that the pip install path is in the expected location.
# ---------------------------------------------------------------------------

FROM apache/airflow:2.9.3-python3.11

# Switch to root only to install system packages
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
    && rm -rf /var/lib/apt/lists/*

# Switch back to airflow user for pip installs (required by the base image)
USER airflow

# Single pip invocation so the resolver sees all constraints at once.
# pyopenssl>=24.2.0 is explicitly listed so it gets upgraded from 24.1.0
# to a version compatible with whatever cryptography version moto installs.
RUN pip install --no-cache-dir \
    "pandas==2.1.4" \
    "pyarrow==16.1.0" \
    "PyYAML==6.0.1" \
    "boto3>=1.34.90,<2.0" \
    "moto[s3,server]" \
    "pyopenssl>=24.2.0"
