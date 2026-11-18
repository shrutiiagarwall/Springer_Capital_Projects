# Dockerfile
# Stage 2 — Custom Airflow image with data-engineering dependencies
# ---------------------------------------------------------------------------
# Extends the official Apache Airflow 2.9.3 image (Python 3.11) and adds:
#   - pandas           2.1.4   — CSV/DataFrame operations
#   - pyarrow          16.1.0  — Parquet read/write
#   - PyYAML           6.0.1   — schema_contract.yaml parsing
#   - boto3            ≥1.34.90 — S3-compatible object-store client
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
RUN pip install --no-cache-dir \
    "pandas==2.1.4" \
    "pyarrow==16.1.0" \
    "PyYAML==6.0.1" \
    "boto3>=1.34.90,<2.0"
