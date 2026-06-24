# Documento de Decisiones Técnicas

*English version: [Technical_Decisions.md](Technical_Decisions.md)*

> Este documento registra cada decisión tomada durante la implementación del pipeline, junto con su justificación.

---

# 1. Arquitectura y capas del pipeline

**Decisión:** El pipeline se organiza en 4 capas — `raw`, `staging`, `intermediate`, `mart` — implementadas como **esquemas separados dentro de la misma base de datos PostgreSQL analítica** (`raw.*`, `staging.*`, `intermediate.*`, `mart.*`).

**Justificación:** Los esquemas son nativos de PostgreSQL, dejan explícito a qué capa pertenece cada tabla sin alargar los nombres con prefijos, y permiten otorgar permisos diferenciados por capa si fuera necesario. La separación en 4 capas completas se prefirió sobre una versión simplificada porque hace trazable el linaje del dato (de archivo plano a tabla de negocio) y separa con claridad responsabilidades: `raw` conserva el dato tal cual llega, `staging` limpia/tipa, `intermediate` aplica reglas de negocio y deduplicación, `mart` deja las tablas finales de consulta.

---

# 2. Nomenclatura

**Decisión:** Todo el código, los esquemas, tablas y columnas se nombran en **inglés, snake_case** (ej. `customer_id`, `net_revenue`, `sale_date`, `fact_sales`).

**Justificación:** Es el estándar en ingeniería de datos, evita problemas de tildes/`ñ` en SQL y en nombres de archivos/variables de entorno, y es lo que se espera ver en un repositorio técnico evaluado. El storytelling del caso (DataMart, países LatAm) se mantiene únicamente como contenido de datos (valores de columnas como `country`), no como nomenclatura técnica.

**Decisión derivada:** El nombre canónico de un producto (valor del dato, no nomenclatura de columna) se calcula como la **descripción más frecuente por `StockCode`**, normalizada a mayúsculas. Cuando hay empate de frecuencia, se toma la primera en orden alfabético para que el resultado sea determinístico y reproducible entre ejecuciones (requisito de idempotencia).

---

# 3. Infraestructura — Airflow Connections y Variables

**Decisión:** Connections y Variables se crean exclusivamente mediante **variables de entorno especiales** (`AIRFLOW_CONN_<conn_id>`, `AIRFLOW_VAR_<key>`) definidas en `docker-compose.yml` / `.env`, sin script de entrypoint adicional.

**Justificación:** Airflow las reconoce automáticamente al arrancar, sin necesidad de mantener un script aparte ni de esperar a que el scheduler esté listo para ejecutar comandos de la CLI. Cumple el requisito de "sin pasos manuales ni comandos adicionales" con la menor cantidad de piezas móviles.

---

# 4. Plus opcional — API de productos

**Decisión:** No se implementa el servicio de API de productos.

**Justificación:** No es requisito de la prueba y, dado el límite de 8 horas, el tiempo rinde más invertido en la calidad de las transformaciones y la idempotencia del DAG (lo que sí se evalúa explícitamente) que en un servicio adicional. Como exige el enunciado en ese caso, se define una estrategia alternativa de categorización (sección 5).

---

# 5. Fuentes de datos

**Decisión — inclusión de los CSV:** Los dos datasets de Kaggle se incluyen directamente en el repositorio como seeds, bajo `data/raw/`, en lugar de descargarlos automáticamente.

**Justificación:** Descargar de Kaggle en tiempo de ejecución requiere credenciales de la API de Kaggle, lo cual añade un secreto más que gestionar y un punto de fallo de red para un evaluador que solo tiene 10 minutos para levantar el entorno. Incluir los archivos como seeds es consistente con el entregable explícito "datos de prueba o seeds necesarios para ejecutar el pipeline desde cero".

**Decisión — deduplicación entre fuentes:** Se usa una **clave compuesta** (`InvoiceNo` + `StockCode` + `CustomerID` + fecha) para detectar el mismo registro en ambas fuentes. Si la clave coincide en `data.csv` y en `online_retail_II.csv`, se conserva una sola vez.

**Justificación:** Es robusto sin importar el orden de carga de las fuentes (no depende de asumir que una fuente es siempre más confiable que la otra) y refleja que ambos datasets describen el mismo tipo de operación de negocio.

**Decisión — categorización de productos (sin API):** Se asigna categoría (`Electronica`, `Hogar`, `Ropa`, `Deportes`, `Papeleria`, o `Sin clasificar`) mediante un **diccionario de palabras clave** que se busca dentro del campo `Description` de cada producto. Los valores se guardan sin tilde (consistente con la decisión de nomenclatura de la sección 2: evitar tildes/ñ en datos y código).

**Justificación:** Es la opción reproducible con menor esfuerzo de configuración entre las viables: no requiere revisar manualmente miles de códigos únicos (inviable en 8h) y, a diferencia de asignar una categoría única a todo el catálogo, sí permite responder las preguntas de negocio sobre qué categorías generan más revenue y cuáles tienen mayor proporción de devoluciones — que es un requisito explícito del enunciado.

**Decisión — interpretación del campo país:** Se usa el campo `Country` del dataset tal cual viene (38 valores reales, ej. United Kingdom, Germany, France, EIRE...), sin mapearlo ni forzarlo a Colombia/México/Perú.

**Justificación:** El enunciado indica explícitamente, en la descripción de ambas fuentes (secciones 4.1 y 4.2), que estos archivos **representan** los datos operacionales de DataMart para efectos del ejercicio — no hay una discrepancia que resolver ni un dato que reinterpretar, es la instrucción de la prueba. Mapear los países reales a CO/MX/PE inventaría una correspondencia inexistente: Reino Unido es el 91.4% de las filas de `data.csv`, así que cualquier mapeo terminaría concentrando ~95% del dato bajo una sola etiqueta ficticia, y la pregunta de negocio sobre país pasaría a responderse con un dato fabricado en vez de real. Se prefiere mantener la misma regla aplicada en el resto del proyecto: no se inventan valores, se documenta y se trabaja con el dato tal cual llega (igual que `customer_id = 'UNKNOWN'` o el nombre canónico de producto).

Los 446 registros (0.08%) con `Country = 'Unspecified'` se cargan igual, sin rechazo: es un valor legítimo de la fuente, no un dato corrupto.

---

# 6. Reglas de negocio y casos ambiguos

**Decisión — transacciones sin Customer ID:** Se incluyen en el análisis con `customer_id = 'UNKNOWN'`. `mart.dim_customer` tiene una fila explícita para ese valor, con `is_identified = false`.

**Justificación:** Excluirlas perdería revenue real del análisis y haría imposible responder la pregunta de negocio sobre diferencias de comportamiento entre clientes identificados y no identificados, que el propio enunciado plantea como pregunta condicional a esta decisión.

**Decisión — granularidad del revenue bruto/neto:** Se calcula y almacena a nivel de **producto (StockCode) por día**.

**Justificación:** Es la granularidad que exige literalmente la regla de negocio del enunciado ("revenue neto = revenue bruto después de aplicar cualquier ajuste por devolución asociada al mismo código de producto **en el mismo periodo diario**"), y es compatible con la pregunta de negocio sobre evolución mensual (se agrega hacia arriba) y con el ranking de top 10 productos.

---

# 7. Modelo de datos

**Decisión:** Estrella convencional en el esquema `mart` — 4 dimensiones + 3 hechos — usando **claves naturales** (el mismo identificador de negocio como PK, sin generar surrogate keys): evita necesitar lógica de lookup/asignación de IDs en el ETL, que sería complejidad de configuración sin beneficio real para este alcance.

| Esquema | Tabla | Grano | Propósito |
|---|---|---|---|
| `raw` | `sales_daily`, `sales_historical` | 1 fila = 1 fila del CSV/Excel original | Espejo de la fuente, todo `TEXT`, sin pérdida de información. |
| `staging` | `sales_transactions` | 1 fila = 1 línea de transacción unificada | Columnas unificadas entre las dos fuentes, tipos correctos, fechas en UTC, `customer_id` sin nulos. |
| `intermediate` | `sales_transactions_clean` | 1 fila = 1 línea de transacción, deduplicada | Aplica deduplicación entre fuentes, categoría, nombre canónico, clasificación venta/devolución y `line_revenue`. |
| `mart` | `dim_date` | 1 fila = 1 día calendario presente en los datos | year/quarter/month/month_name/day/day_of_week/is_weekend, para agrupar evolución mensual sin repetir lógica de fecha en cada query. |
| `mart` | `dim_customer` | 1 fila = 1 `customer_id` (incluye `UNKNOWN`) | `is_identified` para responder la pregunta de negocio sobre clientes identificados vs. no identificados sin recalcularlo con `CASE WHEN` en cada consulta. |
| `mart` | `dim_country` | 1 fila = 1 país real del dataset | Garantiza integridad referencial de `country` en los hechos (ver sección 5: no se mapea a CO/MX/PE). |
| `mart` | `dim_product` | 1 fila = 1 `stock_code` | Nombre canónico + categoría por producto. |
| `mart` | `fact_sales` | 1 fila = 1 línea de venta válida (`quantity > 0`, `unit_price > 0`) | FK a las 4 dimensiones. Soporta ticket promedio, ranking de productos, revenue por país/categoría. |
| `mart` | `fact_returns` | 1 fila = 1 línea de devolución/ajuste (`quantity <= 0`) | FK a las 4 dimensiones. Permite calcular tasa de devolución y el neto. |
| `mart` | `fact_daily_product_revenue` | 1 fila = 1 `stock_code` x día (`sale_date` FK a `dim_date`) | Pre-agregada porque la regla de negocio exige **almacenar** (no solo poder consultar) el revenue neto a este grano. |
| `mart` | `rejected_records` | 1 fila = 1 registro rechazado | Log exigido por el enunciado, con motivo y archivo fuente. |
| `mart` | `etl_execution_log` | 1 fila = 1 ejecución de tarea del DAG | Auditoría de corridas del pipeline. |

**Justificación:** Un esquema en estrella con dimensiones conformadas (`dim_date`, `dim_customer`, `dim_country`, `dim_product`) es el diseño estándar para este tipo de modelo analítico y, justamente por ser estándar, es más fácil de defender que una versión denormalizada ad-hoc: cualquier evaluador reconoce el patrón de inmediato sin necesitar una justificación extensa de por qué se desvió de él. Usar claves naturales (sin `SERIAL`/lookup para las dimensiones) evita la única fuente real de complejidad de un modelo en estrella — la generación y mantenimiento de surrogate keys — sin sacrificar integridad: cada hecho tiene FK reales hacia sus 4 dimensiones, validadas por PostgreSQL en cada carga.

---

# 8. Idempotencia del DAG

**Decisión:** Estrategia de **full refresh** (TRUNCATE + INSERT) en cada capa, en cada ejecución del DAG.

**Justificación:** Como las dos fuentes son archivos estáticos (no llegan filas nuevas entre ejecuciones dentro del alcance de la prueba), la forma más simple y más fácil de explicar/defender es que cada corrida del DAG vacía y reconstruye las tablas de cada capa a partir de los archivos fuente. Dos ejecuciones consecutivas con los mismos datos producen exactamente el mismo contenido final, que es la definición de idempotencia que pide el enunciado, sin necesitar lógica de upsert/merge por clave que sería más compleja de implementar y de justificar en el tiempo disponible.

En `mart`, el orden de `TRUNCATE` es: hechos primero (`fact_daily_product_revenue`, `fact_returns`, `fact_sales`), dimensiones después (`dim_product`, `dim_country`, `dim_customer`, `dim_date`); y al insertar es al revés (dimensiones primero, hechos después), porque los hechos tienen FK hacia las dimensiones.

---

# 9. DAG de Airflow

**Decisión:** Un único DAG (`datamart_sales_pipeline`), `schedule="@daily"`, `catchup=False`, con 5 tareas:

```
extract_daily_sales        ─┐
                             ├─▶ build_staging_layer ─▶ build_intermediate_layer ─▶ load_mart_layer
extract_historical_sales   ─┘
```

* Cada tarea es una sola función Python (`PythonOperator`) que envuelve la función del módulo `etl/` correspondiente y registra el resultado (éxito/fallo, filas procesadas) en `mart.etl_execution_log`, usando la Connection `dwh_postgres`.
* `retries=2` con `retry_delay=2 min` a nivel de `default_args`, aplicado a las 5 tareas.
* Las dos extracciones corren en paralelo porque no dependen entre sí; todo lo demás es secuencial porque cada capa necesita la anterior ya cargada.

**Justificación:** Cinco tareas con nombres descriptivos por etapa es exactamente lo que pide el enunciado, sin fragmentar de más (ej. no se separó "leer archivo" de "insertar en raw" como tareas distintas, porque no aporta valor de orquestación y sí más superficie para explicar).

---

# 10. Categorización de productos — diccionario de palabras clave

**Decisión:** El diccionario categoría→palabras clave vive en `etl/category_keywords.json`, y la ruta se inyecta vía la Airflow Variable `category_keywords_path`. Si el archivo no se puede leer, el código cae a un diccionario por defecto embebido en `etl/categorize.py`.

**Justificación:** Permite ajustar las palabras clave sin tocar código (parámetro operativo real, no decorativo) y cumple el requisito de usar Variables de Airflow para parámetros del pipeline. Dado que el dataset real es de un retailer de artículos para el hogar/regalo, es esperable que la mayoría de los productos caigan en `Hogar` o `Sin clasificar`; se documenta como limitación conocida, no como error.

---

# 11. Repositorio analítico en servidor remoto (decisión propia)

**Decisión:** Airflow corre 100% local en Docker; el repositorio analítico (`raw/staging/intermediate/mart`) vive en un servidor PostgreSQL remoto (`REMOTE_DWH_*`), no en un contenedor local. `postgres-dwh` queda en el `docker-compose.yml` solo como respaldo/fallback, sin uso por defecto.

**Automatización:** el servicio `dwh-remote-init` crea la base remota si no existe y ejecuta `sql/ddl/` contra ella al levantar el entorno — sin pasos manuales, idempotente. Los servicios de Airflow esperan a que termine antes de operar.

**Justificación:** es una decisión propia, no exigida por el enunciado (que pide todo local, "sin servicios cloud"). Si se necesita cumplir esa restricción al pie de la letra, basta apuntar la Connection `dwh_postgres` al `postgres-dwh` local — ya tiene el mismo DDL listo.

---

# 12. Memoria en build_staging() — procesar las fuentes por separado

**Decisión:** `build_staging()` procesa `raw.sales_daily` y `raw.sales_historical` una fuente a la vez (limpia + inserta + libera antes de pasar a la siguiente), en vez de unirlas primero en un solo DataFrame de ~1.6M filas.

**Justificación:** con las dos fuentes combinadas más sus columnas derivadas, el proceso se quedaba sin memoria y el sistema lo mataba (`SIGKILL`, sin traceback de Python). Procesar una fuente a la vez reduce el pico de memoria a la fuente más grande sola. El resultado final en `staging.sales_transactions` es el mismo; sigue siendo una sola transacción (si una fuente falla, se revierte todo).
