"""Tarea de extraccion: lee los 2 archivos fuente y los vuelca tal cual (sin
limpiar ni tipar) en la capa raw. Es el unico modulo que toca los archivos en
data/raw/; todo lo demas (transform.py, load.py) parte de lo que ya quedo en la BD."""

import logging
from pathlib import Path

import pandas as pd
import psycopg2.extras
from airflow.models import Variable

from etl.db import get_dwh_connection, truncate_table

logger = logging.getLogger("datamart_etl")

# Nombres de columna unificados con los que se inserta en raw.*, sin importar
# de cual de las 2 fuentes vinieron (ver los dos mapeos de columnas abajo).
RAW_COLUMNS = [
    "invoice_no", "stock_code", "description", "quantity",
    "invoice_date", "unit_price", "customer_id", "country",
]

# Las dos fuentes traen nombres de columna distintos para el mismo dato
# (ver Decisiones_Tecnicas.md sección 5) — se unifican aquí, antes de tocar la base de datos.
DAILY_COLUMN_MAP = {
    "InvoiceNo": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "UnitPrice": "unit_price",
    "CustomerID": "customer_id",
    "Country": "country",
}

HISTORICAL_COLUMN_MAP = {
    "Invoice": "invoice_no",
    "StockCode": "stock_code",
    "Description": "description",
    "Quantity": "quantity",
    "InvoiceDate": "invoice_date",
    "Price": "unit_price",
    "Customer ID": "customer_id",
    "Country": "country",
}


def _to_text(value):
    """raw.* es todo TEXT a propósito (ver sql/ddl/02_raw.sql): aquí solo se
    convierte cada valor a string, sin validar ni tipar nada todavía."""
    if pd.isna(value):
        return None
    return str(value)


def _insert_raw(cursor, table, df, source_file):
    rows = [
        tuple(_to_text(record[col]) for col in RAW_COLUMNS) + (source_file,)
        for record in df[RAW_COLUMNS].to_dict("records")
    ]
    psycopg2.extras.execute_values(
        cursor,
        f"INSERT INTO raw.{table} ({', '.join(RAW_COLUMNS)}, source_file) VALUES %s",
        rows,
        page_size=1000,
    )


def extract_daily():
    """Fuente obligatoria 1 (data.csv). raw_data_path viene de la Airflow
    Variable del mismo nombre, no esta hardcodeado."""
    raw_path = Path(Variable.get("raw_data_path"))
    # encoding ISO-8859-1: este dataset (carrie1/ecommerce-data) trae caracteres
    # que no son UTF-8 válido en algunas descripciones de producto.
    df = pd.read_csv(raw_path / "data.csv", encoding="ISO-8859-1")
    df = df.rename(columns=DAILY_COLUMN_MAP)[RAW_COLUMNS]

    conn = get_dwh_connection()
    try:
        with conn.cursor() as cur:
            # TRUNCATE + INSERT completo: estrategia de idempotencia (full refresh),
            # ver Decisiones_Tecnicas.md sección 8.
            truncate_table(cur, "raw", "sales_daily")
            _insert_raw(cur, "sales_daily", df, "data.csv")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("extract_daily: %s filas cargadas en raw.sales_daily", len(df))
    return len(df)


def extract_historical():
    """Fuente obligatoria 2 (online_retail_II.xlsx). El archivo trae 2 hojas
    (años distintos); se cargan ambas, cada una marcada con su propio source_file
    para poder rastrear de cual hoja vino cada fila."""
    raw_path = Path(Variable.get("raw_data_path"))
    sheets = pd.read_excel(raw_path / "online_retail_II.xlsx", sheet_name=None)

    conn = get_dwh_connection()
    total_rows = 0
    try:
        with conn.cursor() as cur:
            truncate_table(cur, "raw", "sales_historical")
            for sheet_name, sheet_df in sheets.items():
                sheet_df = sheet_df.rename(columns=HISTORICAL_COLUMN_MAP)[RAW_COLUMNS]
                _insert_raw(cur, "sales_historical", sheet_df, f"online_retail_II.xlsx:{sheet_name}")
                total_rows += len(sheet_df)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    logger.info("extract_historical: %s filas cargadas en raw.sales_historical", total_rows)
    return total_rows
