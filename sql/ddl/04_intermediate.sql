-- Capa intermediate: deduplicada entre fuentes, con reglas de negocio aplicadas
-- (clasificación venta/devolución, categoría, nombre canónico) a nivel de línea de transacción.
CREATE TABLE IF NOT EXISTS intermediate.sales_transactions_clean (
    id                    BIGSERIAL PRIMARY KEY,
    invoice_no            TEXT NOT NULL,
    stock_code            TEXT NOT NULL,
    canonical_description TEXT,
    category              TEXT NOT NULL DEFAULT 'Sin clasificar',
    quantity              INTEGER NOT NULL,
    unit_price            NUMERIC(12, 4) NOT NULL,
    invoice_date          TIMESTAMPTZ NOT NULL,
    customer_id           TEXT NOT NULL,
    country               TEXT,
    is_return             BOOLEAN NOT NULL,       -- quantity <= 0 => true (regla de negocio del enunciado)
    line_revenue          NUMERIC(14, 4) NOT NULL, -- quantity * unit_price; negativo o cero en devoluciones
    source_file           TEXT NOT NULL,
    loaded_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_intermediate_sales_stock_code ON intermediate.sales_transactions_clean (stock_code);
CREATE INDEX IF NOT EXISTS ix_intermediate_sales_invoice_date ON intermediate.sales_transactions_clean (invoice_date);

COMMENT ON TABLE intermediate.sales_transactions_clean IS
    'staging.sales_transactions deduplicada por dedup_key, con is_return/line_revenue/category/canonical_description calculados. Fuente directa de mart.';
