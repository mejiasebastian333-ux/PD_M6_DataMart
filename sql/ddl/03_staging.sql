-- Capa staging: columnas unificadas entre las dos fuentes, tipadas, fechas en UTC,
-- customer_id sin nulos (ver Decisiones_Tecnicas.md). Sin reglas de negocio todavía.
CREATE TABLE IF NOT EXISTS staging.sales_transactions (
    id            BIGSERIAL PRIMARY KEY,
    invoice_no    TEXT NOT NULL,
    stock_code    TEXT NOT NULL,
    description   TEXT,
    quantity      INTEGER NOT NULL,
    unit_price    NUMERIC(12, 4) NOT NULL,
    invoice_date  TIMESTAMPTZ NOT NULL,          -- siempre UTC, ver etl/transform.py
    customer_id   TEXT NOT NULL DEFAULT 'UNKNOWN', -- transacciones sin Customer ID se incluyen asi (no se excluyen)
    country       TEXT,
    source_file   TEXT NOT NULL,
    dedup_key     TEXT NOT NULL,                 -- clave compuesta para detectar duplicados entre data.csv y online_retail_II
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- dedup_key: usado por build_intermediate() para quedarse con una sola copia por clave.
CREATE INDEX IF NOT EXISTS ix_staging_sales_dedup_key ON staging.sales_transactions (dedup_key);
CREATE INDEX IF NOT EXISTS ix_staging_sales_stock_code ON staging.sales_transactions (stock_code);

COMMENT ON TABLE staging.sales_transactions IS
    'raw.sales_daily + raw.sales_historical unificadas y tipadas. dedup_key = invoice_no|stock_code|customer_id|fecha, usada en intermediate para deduplicar entre las dos fuentes.';
