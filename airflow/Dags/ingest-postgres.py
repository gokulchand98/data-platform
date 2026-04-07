"""
ingest_postgres.py

Incrementally extracts customers, orders, and products from Postgres
and writes them to the S3 bronze layer as Snappy-compressed Parquet.

Design principles:
- Incremental only — never full scans on the source DB
- Idempotent — re-running the same interval produces the same result
- Isolated failures — one table failing does not block others
- Zero external dependencies beyond what Airflow already ships
"""

from __future__ import annotations

import io
import logging
from datetime import datetime, timedelta
from typing import Any

import boto3
import pandas as pd
import psycopg2
import pyarrow as pa
import pyarrow.parquet as pq
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── logging ───────────────────────────────────────────────────────────────────
log = logging.getLogger(__name__)

# ── constants ─────────────────────────────────────────────────────────────────
BRONZE_BUCKET   = "bronze"
S3_ENDPOINT     = "http://localstack:4566"
WATERMARK_FMT   = "%Y-%m-%dT%H:%M:%S"
EPOCH_START     = "1970-01-01T00:00:00"

# tables that support incremental extract via updated_at
INCREMENTAL_TABLES = ["customers", "orders", "products"]

# ── connection factories ───────────────────────────────────────────────────────
def get_postgres_conn() -> psycopg2.extensions.connection:
    """
    Returns a Postgres connection.
    In production this would read from Airflow connections (encrypted),
    not hardcoded values. Kept simple here for local dev clarity.
    """
    return psycopg2.connect(
        host="postgres",       # Docker service name, not localhost
        port=5432,
        dbname="ecommerce",
        user="platform",
        password="platform",
        connect_timeout=10,
    )


def get_s3_client() -> boto3.client:
    """
    Returns a boto3 S3 client pointed at LocalStack locally.
    In production, remove endpoint_url and let IAM role handle auth.
    """
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id="test",
        aws_secret_access_key="test",
        region_name="us-east-1",
    )


# ── task functions ────────────────────────────────────────────────────────────
def ensure_bronze_bucket() -> None:
    """
    Creates the bronze S3 bucket if it does not already exist.
    Idempotent — safe to call on every DAG run.
    """
    s3 = get_s3_client()
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}

    if BRONZE_BUCKET not in existing:
        s3.create_bucket(Bucket=BRONZE_BUCKET)
        log.info("created bucket: %s", BRONZE_BUCKET)
    else:
        log.info("bucket already exists: %s", BRONZE_BUCKET)


def extract_and_load(table: str, **context: Any) -> None:
    """
    Core EL function — extract changed rows from Postgres, write to S3.

    Steps:
        1. Read watermark from Airflow Variables
        2. Query only rows where updated_at > watermark
        3. Serialize to Parquet in memory (no temp files on disk)
        4. Upload to S3 under a date-partitioned key
        5. Update watermark so next run continues from here

    Args:
        table:   Name of the Postgres table to extract.
        context: Airflow task context injected automatically.
    """
    # ── 1. watermark ──────────────────────────────────────────────
    watermark_key = f"watermark_{table}"
    last_extracted = Variable.get(watermark_key, default_var=EPOCH_START)
    current_run_ts = context["logical_date"].strftime(WATERMARK_FMT)

    log.info("extracting %s | watermark: %s → %s", table, last_extracted, current_run_ts)

    # ── 2. incremental query ──────────────────────────────────────
    query = """
        SELECT *
        FROM {table}
        WHERE updated_at > %(watermark)s
        ORDER BY updated_at ASC
    """.format(table=table)

    conn = get_postgres_conn()
    try:
        df = pd.read_sql(
            query,
            conn,
            params={"watermark": last_extracted},
        )
    finally:
        conn.close()   # always release the connection

    if df.empty:
        log.info("no new rows for %s since %s — skipping upload", table, last_extracted)
        return

    log.info("extracted %d rows from %s", len(df), table)

    # ── 3. serialize to parquet in memory ─────────────────────────
    # Converting UUID columns to string prevents Arrow schema issues
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)

    arrow_table = pa.Table.from_pandas(df, preserve_index=False)

    buffer = io.BytesIO()
    pq.write_table(
        arrow_table,
        buffer,
        compression="snappy",       # fast compress, good ratio
        write_statistics=True,      # enables predicate pushdown later
    )
    buffer.seek(0)

    # ── 4. upload to s3 ───────────────────────────────────────────
    # Partition by ingestion date so downstream tools can prune partitions
    date_partition = context["ds"]   # YYYY-MM-DD string
    s3_key = f"table={table}/date={date_partition}/data.parquet"

    s3 = get_s3_client()
    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    log.info("uploaded s3://%s/%s (%d bytes)", BRONZE_BUCKET, s3_key, buffer.tell())

    # ── 5. advance watermark ──────────────────────────────────────
    # Only update AFTER successful upload — guarantees no data loss on failure
    Variable.set(watermark_key, current_run_ts)
    log.info("watermark advanced to %s for %s", current_run_ts, table)


# ── DAG definition ────────────────────────────────────────────────────────────
default_args = {
    "owner":             "data-platform",
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 5m, 10m, 20m — backs off on repeated failure
    "email_on_failure":  False,          # set True and add email in production
    "depends_on_past":   False,
}

with DAG(
    dag_id="ingest_postgres_to_s3_bronze",
    description="Incremental extract from Postgres to S3 bronze layer",
    default_args=default_args,
    schedule="@hourly",
    start_date=days_ago(1),
    catchup=False,          # do not backfill historical runs on first deploy
    max_active_runs=1,      # prevent overlapping runs writing to same partition
    tags=["ingestion", "postgres", "bronze", "phase-1"],
) as dag:

    # Task 1 — ensure bucket exists before any extract runs
    t_ensure_bucket = PythonOperator(
        task_id="ensure_bronze_bucket",
        python_callable=ensure_bronze_bucket,
    )

    # Tasks 2,3,4 — one extract task per table, all run in parallel
    extract_tasks = [
        PythonOperator(
            task_id=f"extract_{table}",
            python_callable=extract_and_load,
            op_kwargs={"table": table},
        )
        for table in INCREMENTAL_TABLES
    ]

    # Dependency — bucket must exist before any extract starts
    # Extracts run in parallel after that
    t_ensure_bucket >> extract_tasks