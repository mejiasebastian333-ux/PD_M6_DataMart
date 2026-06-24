"""DAG del pipeline de ventas de DataMart.

Grafo de dependencias:

    extract_daily_sales        ─┐
                                 ├─▶ build_staging_layer ─▶ build_intermediate_layer ─▶ load_mart_layer
    extract_historical_sales   ─┘

Las dos extracciones corren en paralelo (no dependen entre si); todo lo demas
es secuencial porque cada capa necesita la anterior ya cargada en la base.
Cada tarea es una sola funcion Python que envuelve el modulo etl/ correspondiente.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from etl.db import get_dwh_connection, log_execution
from etl.extract import extract_daily, extract_historical
from etl.load import build_mart
from etl.transform import build_intermediate, build_staging

DAG_ID = "datamart_sales_pipeline"

default_args = {
    "owner": "datamart",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def _run_logged(callable_, task_id, **context):
    """Ejecuta una etapa del pipeline y registra el resultado en mart.etl_execution_log,
    usando la Connection dwh_postgres exigida por el enunciado. Se usa una conexion
    aparte (no la que abre cada funcion de etl/) solo para escribir este log,
    asi el log se registra incluso si la etapa falla."""
    conn = get_dwh_connection()
    started_at = datetime.utcnow()
    status = "failed"
    rows_processed = None
    try:
        rows_processed = callable_()
        status = "success"
        return rows_processed
    finally:
        # finally (no except): se registra el resultado tanto si la tarea
        # tuvo éxito como si lanzó una excepción; en ese segundo caso status
        # se queda en "failed" y la excepción sigue propagándose después de
        # este bloque (Airflow la necesita para marcar la tarea como fallida y reintentar).
        with conn.cursor() as cur:
            log_execution(
                cur,
                dag_id=context["dag"].dag_id,
                run_id=context["run_id"],
                task_id=task_id,
                execution_date=context["ds"],
                status=status,
                rows_processed=rows_processed,
                started_at=started_at,
                ended_at=datetime.utcnow(),
            )
        conn.commit()
        conn.close()


# Un wrapper por tarea: PythonOperator necesita un callable distinto por tarea
# (no se puede reusar _run_logged directo porque cada una necesita su propio
# task_id y su propia función de etl/ a ejecutar).
def extract_daily_task(**context):
    return _run_logged(extract_daily, "extract_daily_sales", **context)


def extract_historical_task(**context):
    return _run_logged(extract_historical, "extract_historical_sales", **context)


def build_staging_task(**context):
    return _run_logged(build_staging, "build_staging_layer", **context)


def build_intermediate_task(**context):
    return _run_logged(build_intermediate, "build_intermediate_layer", **context)


def load_mart_task(**context):
    return _run_logged(build_mart, "load_mart_layer", **context)


with DAG(
    dag_id=DAG_ID,
    description="Extrae, limpia y carga las ventas de DataMart en el repositorio analitico",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    # catchup=False: al activar el DAG no se backfillean todos los dias desde
    # start_date, solo corre desde la proxima ejecucion programada (o cuando se dispare a mano).
    catchup=False,
    default_args=default_args,
    tags=["datamart"],
) as dag:

    extract_daily_sales = PythonOperator(
        task_id="extract_daily_sales",
        python_callable=extract_daily_task,
    )
    extract_historical_sales = PythonOperator(
        task_id="extract_historical_sales",
        python_callable=extract_historical_task,
    )
    build_staging_layer = PythonOperator(
        task_id="build_staging_layer",
        python_callable=build_staging_task,
    )
    build_intermediate_layer = PythonOperator(
        task_id="build_intermediate_layer",
        python_callable=build_intermediate_task,
    )
    load_mart_layer = PythonOperator(
        task_id="load_mart_layer",
        python_callable=load_mart_task,
    )

    # Dependencias explicitas: las dos extracciones en paralelo, luego la
    # cadena secuencial de capas (staging -> intermediate -> mart).
    [extract_daily_sales, extract_historical_sales] >> build_staging_layer
    build_staging_layer >> build_intermediate_layer >> load_mart_layer
