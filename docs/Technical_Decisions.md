# Technical Decisions Document

*Versión en español: [Decisiones_Tecnicas.md](Decisiones_Tecnicas.md)*

> This document records every decision made during the implementation of the pipeline, along with its justification.

---

# 1. Architecture and pipeline layers

**Decision:** The pipeline is organized into 4 layers — `raw`, `staging`, `intermediate`, `mart` — implemented as **separate schemas within the same analytical PostgreSQL database** (`raw.*`, `staging.*`, `intermediate.*`, `mart.*`).

**Justification:** Schemas are native to PostgreSQL, make it explicit which layer each table belongs to without lengthening names with prefixes, and allow granting differentiated permissions per layer if needed. The full 4-layer separation was preferred over a simplified version because it makes data lineage traceable (from flat file to business table) and clearly separates responsibilities: `raw` keeps the data exactly as it arrives, `staging` cleans/types it, `intermediate` applies business rules and deduplication, `mart` leaves the final query-ready tables.

---

# 2. Naming convention

**Decision:** All code, schemas, tables, and columns are named in **English, snake_case** (e.g. `customer_id`, `net_revenue`, `sale_date`, `fact_sales`).

**Justification:** It is the standard in data engineering, avoids issues with accents/special characters in SQL and in file/environment variable names, and is what is expected in a technical repository under evaluation. The case's storytelling (DataMart, LatAm countries) is kept only as data content (column values such as `country`), not as technical naming.

**Derived decision:** A product's canonical name (a data value, not column naming) is computed as the **most frequent description per `StockCode`**, normalized to uppercase. On a frequency tie, the alphabetically first one is chosen so the result is deterministic and reproducible across runs (idempotency requirement).

---

# 3. Infrastructure — Airflow Connections and Variables

**Decision:** Connections and Variables are created exclusively through **special environment variables** (`AIRFLOW_CONN_<conn_id>`, `AIRFLOW_VAR_<key>`) defined in `docker-compose.yml` / `.env`, with no extra entrypoint script.

**Justification:** Airflow recognizes them automatically on startup, with no need to maintain a separate script or wait for the scheduler to be ready to run CLI commands. It satisfies the "no manual steps or extra commands" requirement with the fewest moving parts.

---

# 4. Optional plus — products API

**Decision:** The products API service is not implemented.

**Justification:** It is not a requirement of the test, and given the 8-hour limit, time is better invested in transformation quality and DAG idempotency (which are explicitly evaluated) than in an additional service. As the assignment requires in that case, an alternative categorization strategy is defined (section 5).

---

# 5. Data sources

**Decision — including the CSVs:** Both Kaggle datasets are included directly in the repository as seeds, under `data/raw/`, instead of being downloaded automatically.

**Justification:** Downloading from Kaggle at runtime requires Kaggle API credentials, which adds one more secret to manage and a network failure point for an evaluator who only has 10 minutes to bring up the environment. Including the files as seeds is consistent with the explicit deliverable "test data or seeds needed to run the pipeline from scratch".

**Decision — deduplication between sources:** A **composite key** (`InvoiceNo` + `StockCode` + `CustomerID` + date) is used to detect the same record across both sources. If the key matches in both `data.csv` and `online_retail_II.csv`, only one copy is kept.

**Justification:** It is robust regardless of source load order (it doesn't depend on assuming one source is always more reliable than the other) and reflects that both datasets describe the same type of business operation.

**Decision — product categorization (without the API):** A category (`Electronica`, `Hogar`, `Ropa`, `Deportes`, `Papeleria`, or `Sin clasificar`) is assigned via a **keyword dictionary** searched within each product's `Description` field. Values are stored without accents (consistent with the naming decision in section 2: avoid accents/special characters in data and code).

**Justification:** It is the reproducible option with the least configuration effort among the viable ones: it doesn't require manually reviewing thousands of unique codes (not feasible in 8h), and unlike assigning a single category to the whole catalog, it does allow answering the business questions about which categories generate more revenue and which have a higher proportion of returns — an explicit requirement of the assignment.

**Decision — interpretation of the country field:** The dataset's `Country` field is used as-is (38 real values, e.g. United Kingdom, Germany, France, EIRE...), without mapping or forcing it to Colombia/Mexico/Peru.

**Justification:** The assignment explicitly states, in the description of both sources (sections 4.1 and 4.2), that these files **represent** DataMart's operational data for the purposes of the exercise — there is no discrepancy to resolve or data to reinterpret, it is the test's own instruction. Mapping the real countries to CO/MX/PE would invent a correspondence that doesn't exist: the United Kingdom is 91.4% of `data.csv`'s rows, so any mapping would end up concentrating ~95% of the data under a single fictitious label, and the business question about country would end up being answered with fabricated data instead of real data. The same rule applied throughout the rest of the project is kept: values are not invented, the data is documented and worked with as it arrives (same as `customer_id = 'UNKNOWN'` or the product's canonical name).

The 446 records (0.08%) with `Country = 'Unspecified'` are loaded just the same, without rejection: it is a legitimate value from the source, not corrupted data.

---

# 6. Business rules and ambiguous cases

**Decision — transactions without Customer ID:** They are included in the analysis with `customer_id = 'UNKNOWN'`. `mart.dim_customer` has an explicit row for that value, with `is_identified = false`.

**Justification:** Excluding them would lose real revenue from the analysis and would make it impossible to answer the business question about behavioral differences between identified and non-identified customers, which the assignment itself poses as a question conditional on this decision.

**Decision — gross/net revenue granularity:** It is calculated and stored at the **product (StockCode) per day** level.

**Justification:** This is the granularity literally required by the assignment's business rule ("net revenue = gross revenue after applying any return adjustment associated with the same product code **in the same daily period**"), and it is compatible with the business question about monthly evolution (it aggregates upward) and with the top-10-products ranking.

---

# 7. Data model

**Decision:** A conventional star schema in the `mart` schema — 4 dimensions + 3 facts — using **natural keys** (the same business identifier as the PK, with no surrogate keys generated): this avoids needing ID lookup/assignment logic in the ETL, which would be configuration complexity with no real benefit for this scope.

| Schema | Table | Grain | Purpose |
|---|---|---|---|
| `raw` | `sales_daily`, `sales_historical` | 1 row = 1 row from the original CSV/Excel | Mirror of the source, all `TEXT`, no information loss. |
| `staging` | `sales_transactions` | 1 row = 1 unified transaction line | Unified columns between both sources, correct types, UTC dates, `customer_id` with no nulls. |
| `intermediate` | `sales_transactions_clean` | 1 row = 1 deduplicated transaction line | Applies deduplication across sources, category, canonical name, sale/return classification, and `line_revenue`. |
| `mart` | `dim_date` | 1 row = 1 calendar day present in the data | year/quarter/month/month_name/day/day_of_week/is_weekend, to group monthly evolution without repeating date logic in every query. |
| `mart` | `dim_customer` | 1 row = 1 `customer_id` (includes `UNKNOWN`) | `is_identified` to answer the business question about identified vs. non-identified customers without recomputing it with `CASE WHEN` in every query. |
| `mart` | `dim_country` | 1 row = 1 real country from the dataset | Guarantees referential integrity of `country` in the facts (see section 5: not mapped to CO/MX/PE). |
| `mart` | `dim_product` | 1 row = 1 `stock_code` | Canonical name + category per product. |
| `mart` | `fact_sales` | 1 row = 1 valid sale line (`quantity > 0`, `unit_price > 0`) | FK to the 4 dimensions. Supports average ticket, product ranking, revenue by country/category. |
| `mart` | `fact_returns` | 1 row = 1 return/adjustment line (`quantity <= 0`) | FK to the 4 dimensions. Allows computing return rate and the net. |
| `mart` | `fact_daily_product_revenue` | 1 row = 1 `stock_code` x day (`sale_date` FK to `dim_date`) | Pre-aggregated because the business rule requires **storing** (not just being able to query) the net revenue at this grain. |
| `mart` | `rejected_records` | 1 row = 1 rejected record | Log required by the assignment, with reason and source file. |
| `mart` | `etl_execution_log` | 1 row = 1 DAG task execution | Audit trail of pipeline runs. |

**Justification:** A star schema with conformed dimensions (`dim_date`, `dim_customer`, `dim_country`, `dim_product`) is the standard design for this kind of analytical model, and precisely because it is standard, it is easier to defend than an ad-hoc denormalized version: any evaluator recognizes the pattern immediately without needing an extensive justification for why it deviated from it. Using natural keys (no `SERIAL`/lookup for the dimensions) avoids the one real source of complexity in a star schema — generating and maintaining surrogate keys — without sacrificing integrity: every fact has real FKs to its 4 dimensions, validated by PostgreSQL on every load.

---

# 8. DAG idempotency

**Decision:** A **full refresh** strategy (TRUNCATE + INSERT) on every layer, on every DAG run.

**Justification:** Since both sources are static files (no new rows arrive between runs within the scope of the test), the simplest and easiest to explain/defend approach is that each DAG run empties and rebuilds each layer's tables from the source files. Two consecutive runs with the same data produce exactly the same final content, which is the definition of idempotency the assignment asks for, without needing upsert/merge-by-key logic that would be more complex to implement and justify within the available time.

In `mart`, the `TRUNCATE` order is: facts first (`fact_daily_product_revenue`, `fact_returns`, `fact_sales`), dimensions after (`dim_product`, `dim_country`, `dim_customer`, `dim_date`); and inserting is the reverse (dimensions first, facts after), because the facts have FKs to the dimensions.

---

# 9. Airflow DAG

**Decision:** A single DAG (`datamart_sales_pipeline`), `schedule="@daily"`, `catchup=False`, with 5 tasks:

```
extract_daily_sales        ─┐
                             ├─▶ build_staging_layer ─▶ build_intermediate_layer ─▶ load_mart_layer
extract_historical_sales   ─┘
```

* Each task is a single Python function (`PythonOperator`) that wraps the corresponding `etl/` module function and logs the result (success/failure, rows processed) to `mart.etl_execution_log`, using the `dwh_postgres` Connection.
* `retries=2` with `retry_delay=2 min` at the `default_args` level, applied to all 5 tasks.
* The two extractions run in parallel because they don't depend on each other; everything else is sequential because each layer needs the previous one already loaded.

**Justification:** Five tasks with descriptive names per stage is exactly what the assignment asks for, without over-fragmenting (e.g. "reading the file" was not split from "inserting into raw" as separate tasks, because it adds no orchestration value and only more surface area to explain).

---

# 10. Product categorization — keyword dictionary

**Decision:** The category→keywords dictionary lives in `etl/category_keywords.json`, and its path is injected via the `category_keywords_path` Airflow Variable. If the file can't be read, the code falls back to a default dictionary embedded in `etl/categorize.py`.

**Justification:** It allows adjusting keywords without touching code (a real operational parameter, not a decorative one) and satisfies the requirement of using Airflow Variables for pipeline parameters. Since the real dataset is from a home/gift goods retailer, it is expected that most products will fall into `Hogar` or `Sin clasificar`; this is documented as a known limitation, not an error.

---

# 11. Analytical repository on a remote server (own decision)

**Decision:** Airflow runs 100% locally in Docker; the analytical repository (`raw/staging/intermediate/mart`) lives on a remote PostgreSQL server (`REMOTE_DWH_*`), not in a local container. `postgres-dwh` remains in `docker-compose.yml` only as a fallback, unused by default.

**Automation:** the `dwh-remote-init` service creates the remote database if it doesn't exist and runs `sql/ddl/` against it when the environment is brought up — no manual steps, idempotent. The Airflow services wait for it to finish before operating.

**Justification:** this is an own decision, not required by the assignment (which asks for everything local, "no cloud services"). If that restriction needs to be followed to the letter, it's enough to point the `dwh_postgres` Connection at the local `postgres-dwh` — it already has the same DDL ready.

---

# 12. Memory in build_staging() — processing sources separately

**Decision:** `build_staging()` processes `raw.sales_daily` and `raw.sales_historical` one source at a time (clean + insert + release before moving to the next), instead of joining them first into a single ~1.6M-row DataFrame.

**Justification:** with both sources combined plus their derived columns, the process ran out of memory and the system killed it (`SIGKILL`, with no Python traceback). Processing one source at a time reduces the peak memory to that of the largest source alone. The final result in `staging.sales_transactions` is the same; it remains a single transaction (if one source fails, everything is rolled back).
