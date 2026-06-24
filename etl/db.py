"""Utilidades de conexion y carga compartidas por extract.py, transform.py y load.py.
Todas las funciones reciben un cursor/conexion ya abierto: este modulo no decide
cuando hacer commit/rollback, eso lo controla quien llama (para que cada etapa
del pipeline sea una sola transaccion)."""

import logging

import pandas as pd
import psycopg2
import psycopg2.extras
from airflow.hooks.base import BaseHook

# Debe coincidir con el conn_id de la Airflow Connection inyectada por
# AIRFLOW_CONN_DWH_POSTGRES en docker-compose.yml (env var en mayusculas -> conn_id en minusculas).
DWH_CONN_ID = "dwh_postgres"

logger = logging.getLogger("datamart_etl")


def get_dwh_connection():
    """Abre una conexion psycopg2 al repositorio analitico usando la Airflow
    Connection 'dwh_postgres' (requisito del enunciado: usar Connections de
    Airflow en vez de credenciales sueltas en el codigo)."""
    conn = BaseHook.get_connection(DWH_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port or 5432,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )


def truncate_table(cursor, schema, table):
    """CASCADE porque varias tablas de mart tienen FK entre si; RESTART IDENTITY
    para que los ids autoincrementales vuelvan a empezar en 1 en cada corrida
    (parte de la estrategia de idempotencia por full refresh, ver Decisiones_Tecnicas.md)."""
    cursor.execute(f"TRUNCATE TABLE {schema}.{table} RESTART IDENTITY CASCADE;")


def read_dataframe(conn, sql):
    """SELECT -> DataFrame sin pasar por SQLAlchemy (pandas.read_sql con una
    conexion psycopg2 cruda esta deprecado y tira warnings; esto evita eso)."""
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return pd.DataFrame(rows, columns=columns)


def bulk_insert(cursor, table, df, columns, page_size=1000):
    """INSERT masivo con execute_values (mucho mas rapido que insertar fila por
    fila) para los volumenes de este dataset (cientos de miles de filas)."""
    rows = list(df[columns].itertuples(index=False, name=None))
    if rows:
        psycopg2.extras.execute_values(
            cursor,
            f"INSERT INTO {table} ({', '.join(columns)}) VALUES %s",
            rows,
            page_size=page_size,
        )


def log_execution(cursor, dag_id, run_id, task_id, execution_date, status, rows_processed, started_at, ended_at):
    """Auditoria de cada tarea del DAG (tabla mart.etl_execution_log), exigida por el enunciado."""
    cursor.execute(
        """
        INSERT INTO mart.etl_execution_log
            (dag_id, run_id, task_id, execution_date, status, rows_processed, started_at, ended_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
        """,
        (dag_id, run_id, task_id, execution_date, status, rows_processed, started_at, ended_at),
    )


def log_rejected(cursor, source_file, raw_data_json, reason):
    """Log de registros rechazados (tabla mart.rejected_records), exigido por el enunciado:
    cada fila que no pasa una validacion de calidad/negocio queda aqui con su motivo."""
    cursor.execute(
        """
        INSERT INTO mart.rejected_records (source_file, raw_data, reason)
        VALUES (%s, %s, %s);
        """,
        (source_file, raw_data_json, reason),
    )
