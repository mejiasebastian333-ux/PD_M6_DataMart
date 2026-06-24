-- Consultas de validación para las preguntas de negocio (sección 7 del enunciado).
-- Ejecutar contra el repositorio analítico (REMOTE_DWH_NAME) después de correr el DAG.
-- NULLIF(..., 0) evita división por cero cuando un producto/categoría no tiene ventas brutas.

-- 1. Evolución mensual de las ventas netas (descontando devoluciones)
SELECT
    d.year,
    d.month,
    d.month_name,
    SUM(f.net_revenue) AS net_sales
FROM mart.fact_daily_product_revenue f
JOIN mart.dim_date d ON d.date_day = f.sale_date
GROUP BY d.year, d.month, d.month_name
ORDER BY d.year, d.month;


-- 2. Categorías con más revenue bruto y con mayor proporción de devoluciones
SELECT
    p.category,
    SUM(f.gross_revenue) AS gross_revenue,
    SUM(f.returns_amount) AS returns_amount,
    ROUND(SUM(f.returns_amount) / NULLIF(SUM(f.gross_revenue), 0), 4) AS return_ratio
FROM mart.fact_daily_product_revenue f
JOIN mart.dim_product p ON p.stock_code = f.stock_code
GROUP BY p.category
ORDER BY gross_revenue DESC;


-- 3a. Top 10 productos por revenue neto
SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    SUM(f.net_revenue) AS net_revenue
FROM mart.fact_daily_product_revenue f
JOIN mart.dim_product p ON p.stock_code = f.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
ORDER BY net_revenue DESC
LIMIT 10;

-- 3b. Top 10 productos por tasa de devolución (solo productos con ventas brutas > 0)
SELECT
    p.stock_code,
    p.canonical_description,
    p.category,
    SUM(f.gross_revenue) AS gross_revenue,
    SUM(f.returns_amount) AS returns_amount,
    ROUND(SUM(f.returns_amount) / NULLIF(SUM(f.gross_revenue), 0), 4) AS return_rate
FROM mart.fact_daily_product_revenue f
JOIN mart.dim_product p ON p.stock_code = f.stock_code
GROUP BY p.stock_code, p.canonical_description, p.category
HAVING SUM(f.gross_revenue) > 0
ORDER BY return_rate DESC
LIMIT 10;


-- 4. Países que concentran más transacciones y su ticket promedio
-- (dim_country garantiza integridad referencial; no aporta atributos propios, por
-- eso se agrupa directo sobre la columna country de fact_sales)
-- avg_ticket = revenue total / numero de facturas distintas (no por linea de producto)
SELECT
    f.country,
    COUNT(DISTINCT f.invoice_no) AS transactions,
    ROUND(SUM(f.gross_revenue) / COUNT(DISTINCT f.invoice_no), 2) AS avg_ticket
FROM mart.fact_sales f
GROUP BY f.country
ORDER BY transactions DESC;


-- 5. Comportamiento de compra: clientes identificados vs. transacciones sin customer ID
-- (aplica porque se decidió incluir las transacciones sin cliente como 'UNKNOWN' en
-- dim_customer.is_identified, ver Decisiones_Tecnicas.md)
SELECT
    c.is_identified,
    COUNT(DISTINCT f.invoice_no) AS transactions,
    ROUND(AVG(f.gross_revenue), 2) AS avg_line_revenue,
    ROUND(SUM(f.gross_revenue) / COUNT(DISTINCT f.invoice_no), 2) AS avg_ticket
FROM mart.fact_sales f
JOIN mart.dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.is_identified;


-- 6a. Productos sin descripción consistente (más de una variante de escritura en la fuente)
SELECT
    stock_code,
    COUNT(DISTINCT UPPER(TRIM(description))) AS description_variants
FROM staging.sales_transactions
GROUP BY stock_code
HAVING COUNT(DISTINCT UPPER(TRIM(description))) > 1
ORDER BY description_variants DESC;

-- 6b. Total de códigos únicos de producto
SELECT COUNT(DISTINCT stock_code) AS unique_product_codes
FROM staging.sales_transactions;


-- 7. Soporte numérico para la recomendación al equipo de producto:
-- categorías ordenadas por monto absoluto de devoluciones y su return_ratio
SELECT
    p.category,
    SUM(f.returns_amount) AS total_returns,
    SUM(f.gross_revenue) AS total_gross,
    ROUND(SUM(f.returns_amount) / NULLIF(SUM(f.gross_revenue), 0), 4) AS return_ratio
FROM mart.fact_daily_product_revenue f
JOIN mart.dim_product p ON p.stock_code = f.stock_code
GROUP BY p.category
ORDER BY total_returns DESC;
