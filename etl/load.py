"""Tarea de carga: intermediate -> mart. Construye las 4 dimensiones (date,
customer, country, product) y las 3 tablas de hechos (fact_sales, fact_returns,
fact_daily_product_revenue) del modelo en estrella (ver Decisiones_Tecnicas.md
sección 7). Es la única tarea que escribe en mart.*."""

import logging

import pandas as pd

from etl.db import bulk_insert, get_dwh_connection, read_dataframe, truncate_table

logger = logging.getLogger("datamart_etl")

DIM_DATE_COLUMNS = ["date_day", "year", "quarter", "month", "month_name", "day", "day_of_week", "is_weekend"]
DIM_CUSTOMER_COLUMNS = ["customer_id", "is_identified"]
DIM_COUNTRY_COLUMNS = ["country"]
DIM_PRODUCT_COLUMNS = ["stock_code", "canonical_description", "category"]
SALES_COLUMNS = [
    "invoice_no", "stock_code", "customer_id", "country", "sale_date",
    "invoice_ts", "quantity", "unit_price", "gross_revenue",
]
RETURNS_COLUMNS = [
    "invoice_no", "stock_code", "customer_id", "country", "sale_date",
    "invoice_ts", "quantity", "unit_price", "return_amount",
]
DAILY_REVENUE_COLUMNS = ["stock_code", "sale_date", "gross_revenue", "returns_amount", "net_revenue"]


def _build_dim_date(dates):
    """Genera mart.dim_date solo con los días que realmente aparecen en los
    datos (no es un calendario perpetuo) y deriva sus atributos directamente
    de cada fecha (año, mes, día de la semana...)."""
    unique_dates = pd.to_datetime(pd.Series(dates).dropna().unique())
    dim_date = pd.DataFrame({"date_day": unique_dates})
    dim_date["year"] = dim_date["date_day"].dt.year
    dim_date["quarter"] = dim_date["date_day"].dt.quarter
    dim_date["month"] = dim_date["date_day"].dt.month
    dim_date["month_name"] = dim_date["date_day"].dt.month_name()
    dim_date["day"] = dim_date["date_day"].dt.day
    dim_date["day_of_week"] = dim_date["date_day"].dt.day_name()
    dim_date["is_weekend"] = dim_date["date_day"].dt.dayofweek >= 5
    dim_date["date_day"] = dim_date["date_day"].dt.date
    return dim_date


def build_mart():
    """intermediate -> mart: puebla las dimensiones (date/customer/country/product)
    y separa ventas/devoluciones a nivel de línea (fact_sales / fact_returns), más el
    agregado diario por producto (fact_daily_product_revenue) que exige la regla de negocio."""
    conn = get_dwh_connection()
    try:
        clean = read_dataframe(conn, "SELECT * FROM intermediate.sales_transactions_clean")
        clean["sale_date"] = pd.to_datetime(clean["invoice_date"]).dt.date

        # Dimensiones: claves naturales, derivadas directo de los valores que ya
        # estan en intermediate (sin generar surrogate keys, ver Decisiones_Tecnicas.md).
        dim_date = _build_dim_date(clean["sale_date"])
        dim_customer = pd.DataFrame({"customer_id": clean["customer_id"].dropna().unique()})
        dim_customer["is_identified"] = dim_customer["customer_id"] != "UNKNOWN"
        dim_country = pd.DataFrame({"country": clean["country"].dropna().unique()})
        dim_product = clean[DIM_PRODUCT_COLUMNS].drop_duplicates(subset="stock_code")

        # Hechos a nivel de linea: separar ventas/devoluciones es la regla de
        # negocio central del enunciado (poder calcular el neto).
        sales = clean[~clean["is_return"]].copy()
        sales["gross_revenue"] = sales["line_revenue"]
        sales["invoice_ts"] = sales["invoice_date"]

        returns = clean[clean["is_return"]].copy()
        returns["return_amount"] = returns["line_revenue"].abs()  # se guarda como monto positivo, mas legible en reportes
        returns["invoice_ts"] = returns["invoice_date"]

        # Agregado producto x dia: el grano exacto que pide la regla de negocio
        # para el revenue neto ("...en el mismo periodo diario").
        gross_by_day = sales.groupby(["stock_code", "sale_date"])["gross_revenue"].sum().reset_index()
        returns_by_day = (
            returns.groupby(["stock_code", "sale_date"])["return_amount"]
            .sum()
            .reset_index()
            .rename(columns={"return_amount": "returns_amount"})
        )
        # outer join: un producto puede tener ventas sin devoluciones ese dia, o viceversa.
        daily = gross_by_day.merge(returns_by_day, on=["stock_code", "sale_date"], how="outer")
        daily[["gross_revenue", "returns_amount"]] = daily[["gross_revenue", "returns_amount"]].fillna(0)
        daily["net_revenue"] = daily["gross_revenue"] - daily["returns_amount"]

        with conn.cursor() as cur:
            # Orden obligatorio por las FK: hechos (hijos) antes que dimensiones
            # (padres) al truncar; al revés al insertar, mas abajo.
            truncate_table(cur, "mart", "fact_daily_product_revenue")
            truncate_table(cur, "mart", "fact_returns")
            truncate_table(cur, "mart", "fact_sales")
            truncate_table(cur, "mart", "dim_product")
            truncate_table(cur, "mart", "dim_country")
            truncate_table(cur, "mart", "dim_customer")
            truncate_table(cur, "mart", "dim_date")

            # Dimensiones primero: los hechos tienen FK hacia ellas.
            bulk_insert(cur, "mart.dim_date", dim_date, DIM_DATE_COLUMNS)
            bulk_insert(cur, "mart.dim_customer", dim_customer, DIM_CUSTOMER_COLUMNS)
            bulk_insert(cur, "mart.dim_country", dim_country, DIM_COUNTRY_COLUMNS)
            bulk_insert(cur, "mart.dim_product", dim_product, DIM_PRODUCT_COLUMNS)

            bulk_insert(cur, "mart.fact_sales", sales, SALES_COLUMNS)
            bulk_insert(cur, "mart.fact_returns", returns, RETURNS_COLUMNS)
            bulk_insert(cur, "mart.fact_daily_product_revenue", daily, DAILY_REVENUE_COLUMNS)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    logger.info(
        "build_mart: %s fechas, %s clientes, %s paises, %s productos, %s ventas, %s devoluciones",
        len(dim_date), len(dim_customer), len(dim_country), len(dim_product), len(sales), len(returns),
    )
    return len(sales) + len(returns)
