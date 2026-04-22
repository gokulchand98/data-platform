"""
load_postgres_to_bigquery.py

One-time full load from local Postgres into BigQuery.
Creates the raw ecommerce dataset in BigQuery which dbt
staging models will read from as their source.

In production this would be replaced by a continuous
Datastream or Airbyte pipeline — but for portfolio purposes
this demonstrates the full extract-load pattern clearly.
"""

import logging
import psycopg2
import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

# ── config ────────────────────────────────────────────────────
PROJECT_ID   = "data-platform-portfolio"
DATASET      = "ecommerce"
KEY_PATH     = "/Users/gokulchandmallampati/.gcp/data-platform-key.json"

TABLES = [
    "customers",
    "orders",
    "products",
    "order_items",
]

# ── connections ───────────────────────────────────────────────
def get_postgres_conn():
    return psycopg2.connect(
        host="localhost",
        port=5432,
        dbname="ecommerce",
        user="platform",
        password="platform",
        connect_timeout=10,
    )

def get_bq_credentials():
    return service_account.Credentials.from_service_account_file(
        KEY_PATH,
        scopes=["https://www.googleapis.com/auth/bigquery"],
    )

# ── load ──────────────────────────────────────────────────────
def load_table(table: str, conn, credentials) -> None:
    log.info("extracting %s from postgres...", table)

    df = pd.read_sql(f"SELECT * FROM {table}", conn)

    # convert UUID columns to string — BigQuery has no UUID type
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str)

    # convert timezone-aware timestamps — BigQuery requires UTC
    for col in df.select_dtypes(include="datetimetz").columns:
        df[col] = df[col].dt.tz_convert("UTC")

    destination = f"{DATASET}.{table}"
    log.info("loading %d rows into bigquery %s...", len(df), destination)

    pandas_gbq.to_gbq(
        df,
        destination_table=destination,
        project_id=PROJECT_ID,
        credentials=credentials,
        if_exists="replace",    # full refresh for initial load
        progress_bar=True,
    )

    log.info("done — %s loaded successfully", table)


def main():
    log.info("starting postgres to bigquery load")

    credentials = get_bq_credentials()
    conn = get_postgres_conn()

    try:
        for table in TABLES:
            load_table(table, conn, credentials)

        log.info("all tables loaded successfully")
        log.info("verifying row counts in bigquery...")

        # verify counts match what we seeded
        for table in TABLES:
            query = f"SELECT COUNT(*) as cnt FROM `{PROJECT_ID}.{DATASET}.{table}`"
            result = pandas_gbq.read_gbq(
                query,
                project_id=PROJECT_ID,
                credentials=credentials,
            )
            log.info("  %-15s %d rows", table, result["cnt"][0])

    finally:
        conn.close()


if __name__ == "__main__":
    main()