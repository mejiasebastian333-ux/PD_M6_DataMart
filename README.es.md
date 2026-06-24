# PD_M6_DataMart

*English version: [README.md](README.md)*

Pipeline ETL con Apache Airflow corriendo localmente en Docker, que extrae las ventas de DataMart desde dos fuentes (CSV diario + histórico de Kaggle), las limpia/transforma en 4 capas (`raw` → `staging` → `intermediate` → `mart`) y las deja disponibles en un repositorio analítico PostgreSQL remoto.

Decisiones tomadas y su justificación: [`docs/Decisiones_Tecnicas.md`](docs/Decisiones_Tecnicas.md). Enunciado original: [`docs/PruebaC6-IngCienDat.pdf`](docs/PruebaC6-IngCienDat.pdf).

## Requisitos

* Docker y Docker Compose.
* Los archivos `data/raw/data.csv` y `data/raw/online_retail_II.xlsx` ya están incluidos en el repositorio como seeds (no requiere descarga ni credenciales de Kaggle).
* Acceso de red al servidor PostgreSQL remoto y credenciales con permiso de `CREATE DATABASE` ahí.

## 1. Levantar el entorno

```bash
cp .env.example .env
# completa REMOTE_DWH_USER y REMOTE_DWH_PASSWORD con tus credenciales del servidor remoto
docker compose up -d
```

Esto deja corriendo, sin pasos manuales adicionales:

* `postgres-airflow`: metadatos de Airflow (local).
* `dwh-remote-init`: crea la base `REMOTE_DWH_NAME` y los esquemas/tablas de `sql/ddl/` **en el servidor remoto** (corre una vez y termina; es idempotente, se puede volver a ejecutar sin duplicar nada).
* `postgres-dwh`: repositorio analítico local, solo como **respaldo/fallback** para pruebas offline — no es el destino real del pipeline.
* `airflow-init`: inicializa la base de Airflow y crea el usuario administrador (corre una vez y termina).
* `airflow-webserver`: UI en [http://localhost:8080](http://localhost:8080) (usuario/clave: los definidos en `.env`, por defecto `admin`/`admin`).
* `airflow-scheduler`: detecta y ejecuta el DAG `datamart_sales_pipeline` automáticamente (programado `@daily`, sin necesidad de despausarlo en la UI). Espera a que `dwh-remote-init` termine antes de poder correr tareas que dependen del repositorio analítico.

La primera vez tarda unos minutos extra porque el contenedor de Airflow instala `pandas`, `psycopg2-binary` y `openpyxl` al arrancar (variable `AIRFLOW_PIP_ADDITIONAL_REQUIREMENTS`).

## 2. Verificar que Connections y Variables quedaron configuradas

Connections y Variables se inyectan como variables de entorno (`AIRFLOW_CONN_DWH_POSTGRES`, `AIRFLOW_VAR_RAW_DATA_PATH`, `AIRFLOW_VAR_CATEGORY_KEYWORDS_PATH`) — Airflow las reconoce solo, no requieren crearse a mano. Para confirmarlo:

```bash
docker compose exec airflow-webserver airflow connections get dwh_postgres
docker compose exec airflow-webserver airflow variables get raw_data_path
docker compose exec airflow-webserver airflow variables get category_keywords_path
```

O desde la UI: Admin → Connections / Admin → Variables.

## 3. Ejecutar el pipeline

El scheduler dispara automáticamente la primera corrida apenas el DAG queda activo. Para forzarla manualmente:

```bash
docker compose exec airflow-webserver airflow dags trigger datamart_sales_pipeline
```

Seguir el progreso en la UI (Grafo del DAG `datamart_sales_pipeline`) o por logs:

```bash
docker compose logs -f airflow-scheduler
```

## 4. Validar que los datos llegaron al repositorio analítico

Los datos quedan en el servidor remoto, no en un contenedor local. Se usa el cliente `psql` de la imagen `postgres-dwh` solo como herramienta para conectarse hacia afuera (`-h`/`-p` apuntan al servidor remoto, no al contenedor):

```bash
source .env   # carga REMOTE_DWH_* en tu shell
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.fact_sales;"
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.fact_returns;"
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.rejected_records;"
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT * FROM mart.etl_execution_log ORDER BY id DESC LIMIT 5;"
```

Las consultas para las 7 preguntas de negocio están en [`sql/queries/business_questions.sql`](sql/queries/business_questions.sql); se pueden correr todas de una vez:

```bash
docker compose exec -T -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" < sql/queries/business_questions.sql
```

## 5. Confirmar idempotencia

```bash
docker compose exec airflow-webserver airflow dags trigger datamart_sales_pipeline
# esperar a que termine, y comparar contra el conteo anterior:
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" -c "SELECT COUNT(*) FROM mart.fact_sales;"
```

El conteo debe ser idéntico entre corridas: cada tarea hace `TRUNCATE` + recarga completa de su capa (ver sección 8 de [`docs/Decisiones_Tecnicas.md`](docs/Decisiones_Tecnicas.md)).

## Estructura del repositorio

```
airflow/dags/        DAG de Airflow
etl/                  Extracción, transformación, categorización y carga (Python)
sql/ddl/              Esquemas y tablas; las ejecuta el servicio dwh-remote-init contra el servidor remoto
sql/queries/          Consultas SQL de las preguntas de negocio
data/raw/             Seeds: data.csv y online_retail_II.xlsx
docs/                 Enunciado, planeación y documento de decisiones técnicas
docker-compose.yml
.env.example
```

## Apagar el entorno

```bash
docker compose down        # conserva los datos (volúmenes locales)
docker compose down -v     # borra también los volúmenes locales (Airflow + postgres-dwh fallback)
                            # NO afecta el repositorio remoto: esos datos viven fuera de Docker
```
