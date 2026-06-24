# Comandos Docker

Guía de referencia con los comandos Docker / Docker Compose usados a lo largo del pipeline (`PD_M6_DataMart`), agrupados por momento de uso, con el motivo de cada uno.

## 1. Levantar el entorno

```bash
cp .env.example .env
# completar REMOTE_DWH_USER y REMOTE_DWH_PASSWORD con las credenciales del servidor remoto
docker compose up -d
```
- `cp .env.example .env`: crea el archivo de variables de entorno real a partir de la plantilla.
- `docker compose up -d`: levanta todos los servicios definidos en `docker-compose.yml` en segundo plano (Postgres de Airflow, init del repositorio remoto, webserver, scheduler, etc.).

```bash
docker compose ps
```
- Muestra el estado de cada servicio (`running`, `healthy`, `exited (0)`). Útil para confirmar que `airflow-init` y `dwh-remote-init` terminaron correctamente (deben quedar en `exited (0)`, no corren indefinidamente).

## 2. Seguir el arranque y diagnosticar

```bash
docker compose logs -f
```
- Sigue en vivo los logs de todos los servicios a la vez.

```bash
docker compose logs -f airflow-scheduler
```
- Sigue los logs de un servicio puntual (reemplazar por `airflow-webserver`, `postgres-airflow`, etc.).

```bash
docker compose logs dwh-remote-init
```
- Revisa la salida del contenedor que crea la base de datos y las tablas (`sql/ddl/*.sql`) en el servidor PostgreSQL remoto. Corre una sola vez y termina; es idempotente (no duplica nada si se vuelve a ejecutar).

```bash
docker compose logs airflow-init
```
- Confirma que la migración de la base de metadatos de Airflow y la creación del usuario admin se ejecutaron sin errores.

## 3. Verificar Connections y Variables de Airflow

```bash
docker compose exec airflow-webserver airflow connections get dwh_postgres
docker compose exec airflow-webserver airflow variables get raw_data_path
docker compose exec airflow-webserver airflow variables get category_keywords_path
```
- `docker compose exec <servicio> <comando>`: ejecuta un comando dentro de un contenedor que ya está corriendo (no crea uno nuevo).
- Estas tres confirman que la Connection hacia el repositorio analítico remoto y las Variables operativas se inyectaron correctamente vía variables de entorno (`AIRFLOW_CONN_DWH_POSTGRES`, `AIRFLOW_VAR_*`), sin haberlas creado a mano en la UI.

## 4. Disparar y seguir el DAG

```bash
docker compose exec airflow-webserver airflow dags trigger datamart_sales_pipeline
```
- Dispara manualmente una corrida del DAG (el scheduler ya lo hace solo en `@daily`, esto es para forzarlo en el momento).

```bash
docker compose exec airflow-webserver airflow dags list
docker compose exec airflow-webserver airflow tasks list datamart_sales_pipeline
```
- Lista los DAGs detectados y las tareas que componen el pipeline (`raw` → `staging` → `intermediate` → `mart`).

```bash
docker compose logs -f airflow-scheduler
```
- Sigue la ejecución de las tareas en tiempo real (con `LocalExecutor` las corre el propio contenedor del scheduler).

## 5. Validar que los datos llegaron al repositorio analítico remoto

```bash
source .env
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql \
  -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" \
  -c "SELECT COUNT(*) FROM mart.fact_sales;"
```
- `-e PGPASSWORD=...`: inyecta una variable de entorno extra solo para ese comando (evita que pida contraseña interactivamente).
- El cliente `psql` corre dentro del contenedor `postgres-dwh`, pero los flags `-h`/`-p` apuntan **hacia afuera**, al servidor remoto — el contenedor solo se usa como herramienta cliente, los datos no viven ahí.
- Mismo patrón para `mart.fact_returns`, `mart.rejected_records` y `mart.etl_execution_log`.

```bash
docker compose exec -T -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql \
  -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" \
  < sql/queries/business_questions.sql
```
- `-T`: desactiva la asignación de pseudo-TTY, necesario para poder redirigir un archivo (`<`) como entrada estándar del comando dentro del contenedor.
- Ejecuta de una sola vez las 7 consultas de negocio contra el repositorio remoto.

## 6. Confirmar idempotencia

```bash
docker compose exec airflow-webserver airflow dags trigger datamart_sales_pipeline
# esperar a que termine, luego comparar contra el conteo anterior:
docker compose exec -e PGPASSWORD="$REMOTE_DWH_PASSWORD" postgres-dwh psql \
  -h "$REMOTE_DWH_HOST" -p "$REMOTE_DWH_PORT" -U "$REMOTE_DWH_USER" -d "$REMOTE_DWH_NAME" \
  -c "SELECT COUNT(*) FROM mart.fact_sales;"
```
- Disparar el DAG dos veces y comparar el conteo de filas demuestra que cada capa hace `TRUNCATE` + recarga completa, sin duplicar datos entre corridas.

## 7. Entrar a inspeccionar un contenedor por dentro

```bash
docker compose exec airflow-webserver bash
```
- Abre una shell interactiva dentro del contenedor del webserver, útil para explorar archivos montados (`/opt/airflow/dags`, `/opt/airflow/etl`) o probar comandos sueltos.

```bash
docker compose exec airflow-webserver airflow info
```
- Muestra un diagnóstico general de la instalación de Airflow (versión, executor, conexión a la base de metadatos, paths).

## 8. Reiniciar o recrear servicios si algo falla

```bash
docker compose restart airflow-scheduler
```
- Reinicia un servicio puntual sin tocar el resto del entorno.

```bash
docker compose up -d --force-recreate airflow-init
```
- Fuerza a recrear el contenedor `airflow-init` desde cero (por ejemplo, si se necesita rehacer la migración/usuario admin).

```bash
docker compose pull
```
- Descarga de nuevo las imágenes declaradas en `docker-compose.yml` (`apache/airflow:2.10.3`, `postgres:16`) si se actualizó la versión.

## 9. Apagar el entorno

```bash
docker compose down
```
- Detiene y elimina los contenedores, pero conserva los volúmenes (datos locales de Airflow y del `postgres-dwh` de respaldo).

```bash
docker compose down -v
```
- Además de detener los contenedores, borra los volúmenes nombrados (`postgres_airflow_data`, `postgres_dwh_data`, `airflow_logs`). No afecta al repositorio remoto: esos datos viven fuera de Docker, en el servidor PostgreSQL externo.

## 10. Comandos generales de Docker (fuera de Compose)

```bash
docker ps
```
- Lista los contenedores corriendo en el sistema (equivalente a `docker compose ps` pero sin filtrar por proyecto).

```bash
docker images
```
- Lista las imágenes descargadas localmente (`apache/airflow:2.10.3`, `postgres:16`).

```bash
docker volume ls
```
- Lista los volúmenes nombrados creados por Compose (`postgres_airflow_data`, `postgres_dwh_data`, `airflow_logs`).

```bash
docker network ls
```
- Muestra la red que Compose crea automáticamente para que los servicios se vean entre sí por nombre (`postgres-airflow`, `postgres-dwh`, etc.).

```bash
docker compose config
```
- Valida el `docker-compose.yml` y lo imprime ya resuelto, con las variables de entorno (`${...}`) sustituidas por sus valores reales. Sirve para confirmar que el `.env` se está leyendo bien antes de levantar el entorno.

## Puntos clave para defender en la sustentación

- **Orden garantizado por `depends_on`**: las condiciones `service_healthy` (Postgres listo) y `service_completed_successfully` (`dwh-remote-init` / `airflow-init` terminaron) evitan que el scheduler/webserver arranquen antes de tiempo.
- **Contenedores "run-once"**: `dwh-remote-init` y `airflow-init` ejecutan su tarea y salen con código 0; no son procesos de larga duración.
- **YAML anchor (`x-airflow-common`)**: evita repetir la misma configuración (imagen, variables de entorno, volúmenes) en los tres servicios de Airflow (`init`, `webserver`, `scheduler`).
- **`postgres-dwh` es solo *fallback* local**: el destino real de los datos es el servidor remoto, referenciado vía la Connection `AIRFLOW_CONN_DWH_POSTGRES`; el contenedor `postgres-dwh` solo se reutiliza como cliente `psql` para consultar el repositorio remoto.
