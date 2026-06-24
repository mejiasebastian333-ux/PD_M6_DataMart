-- Un esquema de Postgres por capa del pipeline. Se ejecuta primero porque las
-- demas tablas (02_raw.sql, 03_staging.sql, ...) viven dentro de estos esquemas.
CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS mart;
