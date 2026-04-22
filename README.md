# Data Platform

I built this to understand how data moves through a real company's infrastructure from a transactional database all the way to a cloud warehouse with automated quality checks on every deployment.

It's an e-commerce data platform. Orders, customers, products. Nothing exotic about the domain  I picked it because it's easy to reason about and has natural use cases for both batch and streaming patterns.


## What it does

Raw data lives in Postgres. An Airflow pipeline extracts it incrementally every hour — only pulling rows that changed since the last run — and lands them in Google Cloud Storage as Parquet files. dbt picks those up, cleans them in staging models, and builds out a set of mart tables in BigQuery that analysts can actually query. Every model has tests. Every deployment runs those tests automatically via GitHub Actions before anything merges.

```
Postgres → Airflow DAG → GCS (bronze) → dbt → BigQuery (staging / core / finance)
                                                      ↑
                                             GitHub Actions CI
                                             runs dbt test on every PR
```




## Stack

| Layer | Tool | Why I chose it |
|---|---|---|
| Source | Postgres 15 | Standard transactional DB, realistic starting point |
| Orchestration | Apache Airflow 2.8 | Industry standard, good for understanding DAG-based pipelines |
| Storage | GCS | Object storage with lifecycle management — moves cold data to cheaper tiers automatically |
| Transform | dbt Core | SQL-first, version controlled, built-in testing and docs |
| Warehouse | BigQuery | Serverless — no cluster management, pay per query, scales without thinking about it |
| Infrastructure | Terraform | Everything reproducible, nothing clicked in a console |
| CI/CD | GitHub Actions | dbt test runs on every PR, bad data can't reach production |




## Project structure

```
data-platform/
├── airflow/
│   └── dags/
│       └── ingest_postgres.py      # incremental extract with watermark pattern
├── dbt/
│   ├── models/
│   │   ├── staging/                # one model per source table, cleaning only
│   │   └── marts/
│   │       ├── core/               # dim_customers, dim_products, fct_orders
│   │       └── finance/            # daily revenue, customer LTV
│   ├── dbt_project.yml
│   └── profiles.yml                # gitignored — built from secrets in CI
├── infrastructure/
│   └── terraform/
│       └── main.tf                 # GCS buckets, BigQuery datasets, IAM
├── scripts/
│   ├── seed.py                     # generates realistic fake data in Postgres
│   └── load_postgres_to_bigquery.py
├── sql/
│   └── init.sql                    # schema, indexes, updated_at triggers
├── .github/
│   └── workflows/
│       └── dbt_ci.yml              # CI pipeline
├── .env.example
├── docker-compose.yml
└── requirements.txt
```




## Local setup

You need Docker Desktop, Python 3.11, and the gcloud CLI.

```bash
git clone https://github.com/gokulchand98/data-platform.git
cd data-platform

# copy env template and fill in your values
cp .env.example .env

# generate the airflow keys
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"

# create a virtual environment
python -m venv ~/envs/dataplatform
source ~/envs/dataplatform/bin/activate
pip install -r requirements.txt

# start postgres and the full local stack
docker compose up -d

# seed the database
python scripts/seed.py
```

Airflow runs at `http://localhost:8080` (admin/admin). Trigger the `ingest_postgres_to_s3_bronze` DAG manually on first run.




## dbt models

**Staging** — views, no storage cost. Each model maps to one source table and does exactly three things: rename columns consistently, cast types correctly, handle nulls. No joins, no business logic here.

**Core marts** — materialized tables. This is where joins happen.

- `dim_customers` — one row per customer, enriched with order counts, lifetime value, and a segment label (high/mid/low value, no orders)
- `dim_products` — one row per product, enriched with sales metrics and stock status
- `fct_orders` — one row per order, joined with customer info and item rollups. Has a `has_amount_discrepancy` flag that fires when the order total doesn't match the sum of its line items

**Finance marts** — aggregations on top of core.

- `finance_daily_revenue` — daily gross revenue by currency, excludes cancelled orders
- `finance_customer_ltv` — lifetime value per customer with revenue rank




## Data quality

36 tests across all 9 models. Running `dbt test` checks:

- Primary keys are unique and never null
- Foreign keys reference real records
- Status columns only contain valid values
- Revenue figures are never null
- Line item totals add up to order totals

These run locally with `dbt test` and automatically in CI on every pull request.




## Infrastructure

Everything in `infrastructure/terraform/main.tf`. Running `terraform apply` creates:

- 3 GCS buckets (bronze, silver, gold) — all with public access blocked
- Bronze bucket has versioning enabled and lifecycle rules that move data to NEARLINE after 90 days and COLDLINE after 365 days
- 3 BigQuery datasets (staging, core, finance)
- A service account with least-privilege access — read/write on buckets, data editor on BigQuery, secret accessor on Secret Manager
- Postgres credentials stored in Secret Manager

```bash
cd infrastructure/terraform
terraform init
terraform apply
```




## CI/CD

Every push to `dbt/` triggers the GitHub Actions workflow. It:

1. Spins up a fresh Ubuntu runner
2. Installs dbt with pinned versions
3. Authenticates to GCP using a service account key stored as a GitHub secret
4. Builds `profiles.yml` from the secret at runtime — no credentials in the repo
5. Runs `dbt run` — materializes all 9 models in a separate `dbt_ci` dataset
6. Runs `dbt test` — all 36 tests must pass or the PR is blocked
7. Generates and uploads the dbt docs as a build artifact

Pull requests cannot merge with failing tests.




## Design decisions worth explaining

**Incremental extract over full refresh** — the Airflow DAG uses a watermark stored in Airflow Variables. Each run queries `WHERE updated_at > last_run_timestamp` and updates the watermark only after a successful upload. Full table scans on every run would put unnecessary load on the source DB and get slower as data grows.

**Parquet with Snappy compression** — row-oriented formats like CSV read every column even when you only need two. Parquet is columnar, so queries that touch a subset of columns skip the rest entirely. Snappy gives a good compression ratio with fast decompression — better than gzip for data that gets queried frequently.

**Hive-style S3/GCS partitioning** — files land at `table=orders/date=2024-01-15/data.parquet`. BigQuery and other tools understand this format and use it to skip entire partitions when the query filters by date. A query for one day reads one file instead of scanning the whole bucket.

**Staging views, mart tables** — staging models are just SQL logic, no data stored. Views cost nothing. Mart models are queried constantly by analysts and dashboards, so materializing them as tables means queries return in milliseconds instead of recomputing every time.

**Service account over user credentials** — the pipeline authenticates as a dedicated service account, not a human user account. If it's ever compromised, I revoke one service account. The blast radius is limited to exactly the permissions that service account holds.

**`max_active_runs=1` on the DAG** — prevents two hourly runs overlapping and writing to the same GCS partition simultaneously. Without this, if one run takes longer than an hour the next starts and you get a race condition on both the Parquet file and the watermark.




## What's next

- Add Great Expectations for more granular data quality contracts on the bronze layer
- Set up DataHub for lineage tracking and a data catalog
- Replace the one-time Postgres load with a continuous GCP Datastream pipeline for CDC replication
- Add Pub/Sub consumer for real-time clickstream events as a streaming source alongside the batch pipeline




## Author

Gokul Chand Mallampati  
[github.com/gokulchand98](https://github.com/gokulchand98)