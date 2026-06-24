-- Capa mart: estrella convencional (dimensiones + hechos) con claves naturales
-- (sin generar surrogate keys): cada dimensión usa como PK el mismo identificador
-- de negocio que ya traen los datos, así no hace falta lógica de lookup/asignación
-- de IDs en el ETL. Incluye las tablas de auditoría exigidas por el enunciado.

-- ============================================================ Dimensiones ===

CREATE TABLE IF NOT EXISTS mart.dim_date (
    date_day     DATE PRIMARY KEY,
    year         SMALLINT NOT NULL,
    quarter      SMALLINT NOT NULL,
    month        SMALLINT NOT NULL,
    month_name   TEXT NOT NULL,
    day          SMALLINT NOT NULL,
    day_of_week  TEXT NOT NULL,
    is_weekend   BOOLEAN NOT NULL
);
COMMENT ON TABLE mart.dim_date IS
    'Una fila por cada día calendario que aparece en las transacciones (no es un calendario perpetuo).';

CREATE TABLE IF NOT EXISTS mart.dim_customer (
    customer_id    TEXT PRIMARY KEY,
    is_identified  BOOLEAN NOT NULL
);
COMMENT ON TABLE mart.dim_customer IS
    'Incluye el valor UNKNOWN para transacciones sin customer ID (Decisiones_Tecnicas.md sección 6).';

CREATE TABLE IF NOT EXISTS mart.dim_country (
    country  TEXT PRIMARY KEY
);
COMMENT ON TABLE mart.dim_country IS
    'Países reales del dataset, sin mapear a Colombia/México/Perú (Decisiones_Tecnicas.md sección 5).';

CREATE TABLE IF NOT EXISTS mart.dim_product (
    stock_code             TEXT PRIMARY KEY,
    canonical_description  TEXT,
    category               TEXT NOT NULL DEFAULT 'Sin clasificar'
);
COMMENT ON TABLE mart.dim_product IS
    'Nombre canónico (descripción más frecuente por stock_code) y categoría inferida por palabras clave.';

-- ================================================================ Hechos ===
-- Las 3 tablas de hechos tienen FK hacia las 4 dimensiones de arriba. Por eso
-- etl/load.py siempre inserta dimensiones primero y hechos despues (y al
-- limpiar para una nueva corrida, al reves: hechos primero, dimensiones despues).

-- Grano: una fila por línea de factura que es venta válida (quantity > 0, unit_price > 0).
CREATE TABLE IF NOT EXISTS mart.fact_sales (
    id             BIGSERIAL PRIMARY KEY,
    invoice_no     TEXT NOT NULL,
    stock_code     TEXT NOT NULL REFERENCES mart.dim_product (stock_code),
    customer_id    TEXT NOT NULL REFERENCES mart.dim_customer (customer_id),
    country        TEXT NOT NULL REFERENCES mart.dim_country (country),
    sale_date      DATE NOT NULL REFERENCES mart.dim_date (date_day),
    invoice_ts     TIMESTAMPTZ NOT NULL,
    quantity       INTEGER NOT NULL CHECK (quantity > 0),
    unit_price     NUMERIC(12, 4) NOT NULL CHECK (unit_price > 0),
    gross_revenue  NUMERIC(14, 4) NOT NULL
);

-- Grano: una fila por línea de factura que es devolución/ajuste (quantity <= 0).
CREATE TABLE IF NOT EXISTS mart.fact_returns (
    id             BIGSERIAL PRIMARY KEY,
    invoice_no     TEXT NOT NULL,
    stock_code     TEXT NOT NULL REFERENCES mart.dim_product (stock_code),
    customer_id    TEXT NOT NULL REFERENCES mart.dim_customer (customer_id),
    country        TEXT NOT NULL REFERENCES mart.dim_country (country),
    sale_date      DATE NOT NULL REFERENCES mart.dim_date (date_day),
    invoice_ts     TIMESTAMPTZ NOT NULL,
    quantity       INTEGER NOT NULL CHECK (quantity <= 0),
    unit_price     NUMERIC(12, 4) NOT NULL,  -- sin CHECK > 0: el enunciado solo exige precio valido en ventas, no en devoluciones
    return_amount  NUMERIC(14, 4) NOT NULL
);

-- Grano: producto x día. Pre-calculada porque la regla de negocio exige
-- almacenar (no solo poder consultar) el revenue neto a este nivel.
CREATE TABLE IF NOT EXISTS mart.fact_daily_product_revenue (
    stock_code      TEXT NOT NULL REFERENCES mart.dim_product (stock_code),
    sale_date       DATE NOT NULL REFERENCES mart.dim_date (date_day),
    gross_revenue   NUMERIC(14, 4) NOT NULL DEFAULT 0,
    returns_amount  NUMERIC(14, 4) NOT NULL DEFAULT 0,
    net_revenue     NUMERIC(14, 4) NOT NULL DEFAULT 0,
    PRIMARY KEY (stock_code, sale_date)
);

-- ============================================================ Auditoría ===

CREATE TABLE IF NOT EXISTS mart.rejected_records (
    id           BIGSERIAL PRIMARY KEY,
    source_file  TEXT NOT NULL,
    raw_data     JSONB,
    reason       TEXT NOT NULL,
    rejected_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS mart.etl_execution_log (
    id              BIGSERIAL PRIMARY KEY,
    dag_id          TEXT NOT NULL,
    run_id          TEXT NOT NULL,
    task_id         TEXT NOT NULL,
    execution_date  DATE NOT NULL,
    status          TEXT NOT NULL,
    rows_processed  INTEGER,
    started_at      TIMESTAMPTZ NOT NULL,
    ended_at        TIMESTAMPTZ
);

-- ============================================================== Índices ===

CREATE INDEX IF NOT EXISTS ix_fact_sales_sale_date ON mart.fact_sales (sale_date);
CREATE INDEX IF NOT EXISTS ix_fact_sales_stock_code ON mart.fact_sales (stock_code);
CREATE INDEX IF NOT EXISTS ix_fact_sales_country ON mart.fact_sales (country);
CREATE INDEX IF NOT EXISTS ix_fact_returns_sale_date ON mart.fact_returns (sale_date);
CREATE INDEX IF NOT EXISTS ix_fact_returns_stock_code ON mart.fact_returns (stock_code);
