"""Tareas de transformacion: staging (limpieza/tipado/union de fuentes) e
intermediate (deduplicacion + reglas de negocio). Cada funcion lee de la capa
anterior directamente en la base (no reutiliza los DataFrames de extract.py),
para que cada etapa sea independiente y se pueda re-ejecutar sola si falla."""

import json
import logging

import pandas as pd
from airflow.models import Variable

from etl.categorize import categorize, load_category_keywords, pick_canonical_descriptions
from etl.db import bulk_insert, get_dwh_connection, log_rejected, read_dataframe, truncate_table

logger = logging.getLogger("datamart_etl")

STAGING_COLUMNS = [
    "invoice_no", "stock_code", "description", "quantity", "unit_price",
    "invoice_date", "customer_id", "country", "source_file", "dedup_key",
]
INTERMEDIATE_COLUMNS = [
    "invoice_no", "stock_code", "canonical_description", "category", "quantity",
    "unit_price", "invoice_date", "customer_id", "country", "is_return",
    "line_revenue", "source_file",
]

# Orden en que se procesan las fuentes en build_staging(): importa para la
# deduplicacion en build_intermediate (keep="first" se queda con data.csv).
RAW_SOURCE_TABLES = ["sales_daily", "sales_historical"]


def _log_rejected_rows(cursor, df, reason):
    """Vuelca cada fila rechazada a mart.rejected_records como JSON, para no
    perder ningun dato del registro original (motivo + contenido completo)."""
    for record in df.to_dict("records"):
        payload = {k: (None if pd.isna(v) else str(v)) for k, v in record.items()}
        log_rejected(cursor, record.get("source_file"), json.dumps(payload), reason)


def _prepare_source(raw_df):
    """Tipa y limpia un DataFrame de UNA sola fuente raw.*. Devuelve (valid, invalid).
    Las columnas se sobrescriben en el mismo lugar (no se guardan columnas auxiliares
    "_clean" en paralelo a las originales) para no duplicar memoria con ~500k-1M filas."""
    quantity_clean = pd.to_numeric(raw_df["quantity"], errors="coerce")
    unit_price_clean = pd.to_numeric(raw_df["unit_price"], errors="coerce")
    invoice_date_clean = pd.to_datetime(raw_df["invoice_date"], errors="coerce")
    invoice_no_blank = raw_df["invoice_no"].isna() | (raw_df["invoice_no"].astype(str).str.strip() == "")
    stock_code_blank = raw_df["stock_code"].isna() | (raw_df["stock_code"].astype(str).str.strip() == "")

    # Una fila es invalida si CUALQUIERA de sus campos clave no se pudo tipar/limpiar.
    is_invalid = (
        quantity_clean.isna()
        | unit_price_clean.isna()
        | invoice_date_clean.isna()
        | invoice_no_blank
        | stock_code_blank
    )
    invalid = raw_df[is_invalid]
    valid = raw_df[~is_invalid].copy()

    valid["quantity"] = quantity_clean[~is_invalid].astype(int)
    valid["unit_price"] = unit_price_clean[~is_invalid]
    # tz_localize (no tz_convert): el dataset no trae zona horaria, se asume
    # que el valor ya esta en UTC y solo se le agrega la etiqueta de tz
    # (ver Decisiones_Tecnicas.md). No se aplica ningun corrimiento de hora.
    valid["invoice_date"] = invoice_date_clean[~is_invalid].dt.tz_localize("UTC")
    # Transacciones sin Customer ID se incluyen como 'UNKNOWN', no se excluyen.
    valid["customer_id"] = valid["customer_id"].fillna("UNKNOWN").astype(str).str.strip()
    valid.loc[valid["customer_id"] == "", "customer_id"] = "UNKNOWN"
    # Regla del enunciado: codigos de producto a mayusculas y sin espacios.
    valid["stock_code"] = valid["stock_code"].astype(str).str.strip().str.upper().str.replace(" ", "", regex=False)
    valid["invoice_no"] = valid["invoice_no"].astype(str).str.strip()
    valid["country"] = valid["country"].astype(str).str.strip()
    valid["description"] = valid["description"].astype(str).str.strip()
    # Clave compuesta para detectar el mismo registro repetido entre las dos
    # fuentes (decisión de deduplicación, ver Decisiones_Tecnicas.md sección 5).
    valid["dedup_key"] = (
        valid["invoice_no"] + "|" + valid["stock_code"] + "|" + valid["customer_id"] + "|"
        + valid["invoice_date"].dt.strftime("%Y-%m-%d")
    )
    return valid, invalid


def build_staging():
    """raw -> staging: unifica las dos fuentes, tipa, normaliza fechas a UTC y
    rechaza filas que no se pueden tipar (cantidad/precio/fecha no numéricos,
    o invoice_no/stock_code vacíos). El precio <= 0 todavía NO se rechaza aquí:
    hasta no saber si la fila es venta o devolución (eso se decide en
    build_intermediate), rechazar por precio sería aplicar una regla de negocio
    antes de tiempo.

    Las dos fuentes (~540k y ~1.07M filas) se procesan UNA A LA VEZ, no unidas
    en un solo DataFrame combinado: con ~1.6M filas en memoria a la vez mas las
    columnas derivadas, el proceso se quedaba sin RAM y el sistema lo mataba
    (SIGKILL) sin ni siquiera un traceback de Python. Procesando una fuente,
    insertando, y liberandola antes de pasar a la siguiente, el pico de memoria
    se reduce a la fuente mas grande sola, no a la suma de las dos."""
    conn = get_dwh_connection()
    total_valid = 0
    total_invalid = 0
    try:
        with conn.cursor() as cur:
            # Se trunca aqui (no en build_intermediate) porque esta es la primera
            # etapa del run que puede generar rechazos; full refresh por corrida.
            truncate_table(cur, "mart", "rejected_records")
            truncate_table(cur, "staging", "sales_transactions")

        for table in RAW_SOURCE_TABLES:
            raw_df = read_dataframe(conn, f"SELECT * FROM raw.{table}")
            valid, invalid = _prepare_source(raw_df)
            del raw_df  # ya no se necesita el crudo de esta fuente

            with conn.cursor() as cur:
                _log_rejected_rows(cur, invalid, "cantidad, precio, fecha, invoice_no o stock_code no validos")
                bulk_insert(cur, "staging.sales_transactions", valid, STAGING_COLUMNS)

            total_valid += len(valid)
            total_invalid += len(invalid)
            del valid, invalid  # liberar antes de procesar la siguiente fuente

        # Un solo commit al final: si una fuente falla a mitad de camino, se
        # revierte todo (incluida la fuente que sí se proceso bien), no se
        # queda staging con una sola fuente cargada a medias.
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("build_staging: %s filas validas, %s rechazadas", total_valid, total_invalid)
    return total_valid


def build_intermediate():
    """staging -> intermediate: deduplica entre fuentes por clave compuesta
    (se conserva la primera ocurrencia, que corresponde a data.csv por el orden
    de extracción), clasifica venta/devolución, rechaza ventas con precio <= 0,
    calcula revenue de línea y asigna categoría + nombre canónico."""
    conn = get_dwh_connection()
    try:
        staging = read_dataframe(conn, "SELECT * FROM staging.sales_transactions ORDER BY id")
        # keep="first": con el ORDER BY id de arriba y el orden de insercion de
        # build_staging (data.csv antes que online_retail_II), esto deja la
        # version de data.csv cuando un registro existe en ambas fuentes.
        deduped = staging.drop_duplicates(subset="dedup_key", keep="first").copy()
        del staging

        # Regla del enunciado: cantidad <= 0 es devolucion/ajuste, no venta.
        deduped["is_return"] = deduped["quantity"] <= 0
        # Precio <= 0 solo es invalido en una venta; en una devolucion no se valida.
        invalid_sale = (~deduped["is_return"]) & (deduped["unit_price"] <= 0)
        clean = deduped[~invalid_sale].copy()
        rejected = deduped[invalid_sale]
        del deduped

        clean["line_revenue"] = clean["quantity"] * clean["unit_price"]
        keywords = load_category_keywords(Variable.get("category_keywords_path"))
        clean["category"] = clean["description"].apply(lambda d: categorize(d, keywords))

        # Nombre canonico por stock_code (descripcion mas frecuente), calculado
        # sobre todo el conjunto ya deduplicado y unido de vuelta por stock_code.
        canonical = pick_canonical_descriptions(clean)
        clean = clean.merge(canonical, on="stock_code", how="left")

        with conn.cursor() as cur:
            # No se trunca rejected_records aqui: ya se truncó una vez por
            # corrida en build_staging; esto solo agrega los rechazos de esta etapa.
            _log_rejected_rows(cur, rejected, "precio unitario <= 0 en una venta")

            truncate_table(cur, "intermediate", "sales_transactions_clean")
            bulk_insert(cur, "intermediate.sales_transactions_clean", clean, INTERMEDIATE_COLUMNS)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info("build_intermediate: %s filas limpias, %s rechazadas", len(clean), len(rejected))
    return len(clean)
