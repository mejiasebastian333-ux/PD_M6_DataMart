-- Capa raw: espejo del archivo fuente, sin tipado ni limpieza. Todo TEXT a propósito
-- (si el dato viene corrupto o en un formato raro, queremos poder verlo tal cual llegó,
-- no que una conversión de tipo lo rechace antes de poder inspeccionarlo).
-- Mismas columnas logicas que raw.sales_historical, aunque en el CSV original
-- (data.csv) se llaman distinto (InvoiceNo, UnitPrice, CustomerID...): la
-- unificacion de nombres la hace etl/extract.py antes de insertar aqui.
CREATE TABLE IF NOT EXISTS raw.sales_daily (
    invoice_no    TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      TEXT,
    invoice_date  TEXT,
    unit_price    TEXT,
    customer_id   TEXT,
    country       TEXT,
    source_file   TEXT NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Misma estructura que sales_daily; la fuente (online_retail_II.xlsx) tiene sus
-- propios nombres de columna (Invoice, Price, "Customer ID"...), tambien
-- unificados en etl/extract.py.
CREATE TABLE IF NOT EXISTS raw.sales_historical (
    invoice_no    TEXT,
    stock_code    TEXT,
    description   TEXT,
    quantity      TEXT,
    invoice_date  TEXT,
    unit_price    TEXT,
    customer_id   TEXT,
    country       TEXT,
    source_file   TEXT NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE raw.sales_daily IS 'Espejo 1:1 de data.csv. source_file identifica el archivo de origen.';
COMMENT ON TABLE raw.sales_historical IS 'Espejo 1:1 de online_retail_II.xlsx. source_file incluye el nombre de la hoja de Excel.';
