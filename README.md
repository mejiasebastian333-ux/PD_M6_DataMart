# PD_M6_DataMart

*Versión en español: [README.es.md](README.es.md)*

Dockerized ETL pipeline with Apache Airflow that extracts DataMart's sales from two sources (daily CSV + Kaggle historical), cleans/transforms them through 4 layers (`raw` → `staging` → `intermediate` → `mart`), and makes them available in a remote PostgreSQL analytical repository.

Decisions made and their justification: [`docs/Technical_Decisions.md`](docs/Technical_Decisions.md). Original assignment: [`docs/PruebaC6-IngCienDat.pdf`](docs/PruebaC6-IngCienDat.pdf).

## Requirements

* Docker and Docker Compose.
* `data/raw/data.csv` and `data/raw/online_retail_II.xlsx` are already included in the repository as seeds (no download or Kaggle credentials required).
* Network access to the remote PostgreSQL server and credentials with `CREATE DATABASE` permission there.

## 1. Bring up the environment

```bash
cp .env.example .env
# fill in REMOTE_DWH_USER and REMOTE_DWH_PASSWORD with your remote server credentials
docker compose up -d
```

This brings up, with no additional manual steps:

* `postgres-airflow`: Airflow metadata (local).
* `dwh-remote-init`: creates the `REMOTE_DWH_NAME` database and the `sql/ddl/` schemas/tables **on the remote server** (runs once and exits; idempotent, safe to re-run without duplicating anything).
* `postgres-dwh`: local analytical repository, only as **fallback** for offline testing — not the pipeline's real target.
* `airflow-init`: initializes the Airflow database and creates the admin user (runs once and exits).
* `airflow-webserver`: UI at [http://localhost:8080](http://localhost:8080) (user/password: the ones defined in `.env`, `admin`/`admin` by default).
* `airflow-scheduler`: detects and runs the `datamart_sales_pipeline` DAG automatically (scheduled `@daily`, no need to unpause it in the UI). Waits for `dwh-remote-init` to finish before running any task that depends on the analytical repository.

The first run takes a few extra minutes because the Airflow container installs `pandas`, `psycopg2-binary`, and `openpyxl` on startup (`AIRFLOW_PIP_ADDITIONAL_REQUIREMENTS` variable).

## 2. Verify Connections and Variables were configured

Connections and Variables are injected as environment variables (`AIRFLOW_CONN_DWH_POSTGRES`, `AIRFLOW_VAR_RAW_DATA_PATH`, `AIRFLOW_VAR_CATEGORY_KEYWORDS_PATH`) — Airflow picks them up on its own, no manual creation needed. To confirm:

```bash
docker compose exec airflow-webserver airflow connections get dwh_postgres
docker compose exec airflow-webserver airflow variables get raw_data_path
docker compose exec airflow-webserver airflow variables get category_keywords_path
```

Or from the UI: Admin → Connections / Admin → Variables.

## 3. Run the pipeline

The scheduler triggers the first run automatically as soon as the DAG is active. To trigger it manually:

```bash
docker compose exec airflow-webserver airflow dags trigger datamart_sales_pipeline
```

Follow progress in the UI (`datamart_sales_pipeline` DAG graph) or via logs:

```bash
docker compose logs -f airflow-scheduler
```

## 4. Validate that data reached the analytical repository

The data lives on the remote server, not in a local container. The `psql` client from the `postgres-dwh` image is used purely as a tool to connect outward (`-h`/`-p` point at the remote server, not at the container):

```bash
source .env   # loads REMOTE_DWH_* into your shell
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.fact_sales;"
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.fact_returns;"
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.rejected_records;"
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT * FROM mart.etl_execution_log ORDER BY id DESC LIMIT 5;"
```

Queries for the 7 business questions are in [`sql/queries/business_questions.sql`](sql/queries/business_questions.sql); they can all be run at once:

```bash
docker compose exec -T -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" < sql/queries/business_questions.sql
```

## 5. Confirm idempotency

```bash
docker compose exec airflow-webserver airflow dags trigger datamart_sales_pipeline
# wait for it to finish, then compare against the previous count:
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.fact_sales;"
```

The count must be identical between runs: each task does a `TRUNCATE` + full reload of its layer (see section 8 of [`docs/Technical_Decisions.md`](docs/Technical_Decisions.md)).

## Repository structure

```
airflow/dags/        Airflow DAG
etl/                  Extraction, transformation, categorization and load (Python)
sql/ddl/              Schemas and tables; executed by the dwh-remote-init service against the remote server
sql/queries/          SQL queries for the business questions
data/raw/             Seeds: data.csv and online_retail_II.xlsx
docs/                 Assignment, decisions document
docker-compose.yml
.env.example
```

## Shutting down the environment

```bash
docker compose down        # keeps local data (volumes)
docker compose down -v     # also deletes local volumes (Airflow + postgres-dwh fallback)
                            # does NOT affect the remote repository: that data lives outside Docker
```
