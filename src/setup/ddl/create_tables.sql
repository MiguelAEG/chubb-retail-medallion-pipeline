-- ==============================================================================
-- SCRIPT: src/setup/ddl/create_tables.sql
-- PROPÓSITO: DDL idempotente para todas las capas Medallion (Bronze, Silver, Quarantine, Gold)
-- PROYECTO: Prueba_Tecnica — Chubb Regional 2026
-- ==============================================================================

-- =============================================================================
-- 0. PROVISIÓN DE CATÁLOGO Y ESQUEMA (IDEMPOTENTE)
-- =============================================================================
CREATE CATALOG IF NOT EXISTS {catalog};
USE CATALOG {catalog};
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};
USE SCHEMA {schema};

-- ==============================================================================
-- 1. CAPA BRONZE (Raw Lakehouse & Auditoría de Ingesta)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customers_bronze (
    customer_id STRING COMMENT 'Clave de negocio del cliente',
    name STRING COMMENT 'Nombre completo del cliente',
    first_name STRING COMMENT 'Nombre de pila del cliente',
    last_name STRING COMMENT 'Apellido del cliente',
    email STRING COMMENT 'Correo electronico en texto plano (PII High)',
    phone STRING COMMENT 'Numero telefonico en texto plano (PII High)',
    country STRING COMMENT 'Pais de residencia del cliente',
    segment STRING COMMENT 'Segmento de cliente (lifestyle, electronics, miscellaneous, CPG)',
    address STRING COMMENT 'Direccion fisica de residencia',
    updated_at STRING COMMENT 'Marca temporal UTC del sistema de origen',
    transaction_type STRING COMMENT 'Tipo de evento transaccional (INSERT, UPDATE, DELETE)',
    _raw_hash BIGINT COMMENT 'Hash determinista xxhash64 de la fila cruda para trazabilidad (8 bytes, Photon opt)',
    _record_hash BIGINT COMMENT 'Hash determinista xxhash64 de campos de negocio',
    _source_file STRING COMMENT 'Nombre o ruta del archivo procesado',
    _source_system STRING COMMENT 'Identificador del sistema de origen emisor',
    _batch_id STRING COMMENT 'Identificador unico del lote de procesamiento',
    _ingestion_ts TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse',
    _ingested_at TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_bronze (
    order_id STRING COMMENT 'Clave primaria de la orden transaccional',
    customer_id STRING COMMENT 'Clave foranea referenciando al cliente',
    order_date STRING COMMENT 'Fecha calendario de la transaccion (YYYY-MM-DD)',
    order_timestamp STRING COMMENT 'Marca temporal precisa de la transaccion',
    order_status STRING COMMENT 'Estado transaccional (COMPLETED, PENDING, CANCELLED, REFUNDED)',
    total_amount STRING COMMENT 'Monto monetario de la orden en texto crudo',
    currency STRING COMMENT 'Codigo ISO de la divisa de facturacion',
    updated_at STRING COMMENT 'Marca temporal de actualizacion en origen',
    transaction_type STRING COMMENT 'Tipo de evento transaccional (INSERT, UPDATE, DELETE)',
    _raw_hash BIGINT COMMENT 'Hash determinista xxhash64 de la fila cruda para trazabilidad (8 bytes, Photon opt)',
    _record_hash BIGINT COMMENT 'Hash determinista xxhash64 de campos de negocio',
    _source_file STRING COMMENT 'Nombre o ruta del archivo procesado',
    _source_system STRING COMMENT 'Identificador del sistema de origen emisor',
    _batch_id STRING COMMENT 'Identificador unico del lote de procesamiento',
    _ingestion_ts TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse',
    _ingested_at TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_ingestion_log (
    batch_id STRING COMMENT 'Identificador unico del lote ingerido',
    source_file STRING COMMENT 'Nombre o ruta del archivo de origen',
    entity STRING COMMENT 'Entidad procesada (customers, orders)',
    rows_read BIGINT COMMENT 'Total de filas leidas del archivo fuente',
    rows_ingested BIGINT COMMENT 'Total de filas insertadas con exito en Bronze',
    status STRING COMMENT 'Estado final de la ingesta (SUCCESS, FAILED, PARTIAL)',
    error_message STRING COMMENT 'Detalle del error en caso de fallo',
    ingested_at TIMESTAMP COMMENT 'Marca temporal UTC de registro del evento'
)
USING DELTA;

-- Vistas de compatibilidad en el esquema bronze
CREATE OR REPLACE VIEW bronze.raw_customers AS
SELECT * FROM {catalog}.{schema}.customers_bronze;

CREATE OR REPLACE VIEW bronze.raw_orders AS
SELECT * FROM {catalog}.{schema}.orders_bronze;

CREATE OR REPLACE VIEW bronze.bronze_ingestion_log AS
SELECT * FROM {catalog}.{schema}.bronze_ingestion_log;

-- ==============================================================================
-- 2. CAPA QUARANTINE (Dead-Letter Queue / Aislamiento de Defectos)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS quarantine.quarantine_records (
    quarantine_id STRING COMMENT 'Identificador UUID unico de la incidencia',
    source_entity STRING COMMENT 'Entidad de origen evaluada (customers, orders)',
    _batch_id STRING COMMENT 'Identificador del lote o correlacion',
    raw_payload STRING COMMENT 'Carga util completa original en formato JSON crudo',
    quarantine_reason STRING COMMENT 'Explicacion de la regla de calidad violada',
    error_code STRING COMMENT 'Codigo tipificado (ERR_NULL_PK, ERR_NEGATIVE_AMOUNT, ERR_INVALID_DATE, ERR_ORPHAN_CUSTOMER)',
    failed_at TIMESTAMP COMMENT 'Marca temporal UTC de deteccion y aislamiento'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS chubb_lakehouse.orders_quarantine (
    order_id STRING COMMENT 'Clave primaria de la orden evaluada',
    customer_id STRING COMMENT 'Clave foranea del cliente evaluado',
    order_ts STRING COMMENT 'Marca temporal precisa de la transaccion',
    amount STRING COMMENT 'Monto monetario en texto crudo',
    currency STRING COMMENT 'Codigo de divisa',
    status STRING COMMENT 'Estado transaccional',
    updated_at STRING COMMENT 'Marca temporal de actualizacion',
    transaction_type STRING COMMENT 'Tipo de evento transaccional',
    error_code STRING COMMENT 'Codigo tipificado del fallo DQ',
    rejection_reason STRING COMMENT 'Explicacion del fallo DQ',
    rejected_at TIMESTAMP COMMENT 'Marca temporal UTC del desvio a cuarentena',
    _batch_id STRING COMMENT 'Identificador del lote',
    _source_file STRING COMMENT 'Archivo fuente'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS quarantine.orders_quarantine (
    order_id STRING COMMENT 'Clave primaria de la orden evaluada',
    customer_id STRING COMMENT 'Clave foranea del cliente evaluado',
    order_ts STRING COMMENT 'Marca temporal precisa de la transaccion',
    amount STRING COMMENT 'Monto monetario en texto crudo',
    currency STRING COMMENT 'Codigo de divisa',
    status STRING COMMENT 'Estado transaccional',
    updated_at STRING COMMENT 'Marca temporal de actualizacion',
    transaction_type STRING COMMENT 'Tipo de evento transaccional',
    error_code STRING COMMENT 'Codigo tipificado del fallo DQ',
    rejection_reason STRING COMMENT 'Explicacion del fallo DQ',
    rejected_at TIMESTAMP COMMENT 'Marca temporal UTC del desvio a cuarentena',
    _batch_id STRING COMMENT 'Identificador del lote',
    _source_file STRING COMMENT 'Archivo fuente'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS chubb_lakehouse.customers_quarantine (
    customer_id STRING COMMENT 'Clave natural del cliente',
    name STRING COMMENT 'Nombre del cliente',
    email STRING COMMENT 'Correo electronico',
    country STRING COMMENT 'Pais del cliente',
    segment STRING COMMENT 'Segmento del cliente',
    updated_at STRING COMMENT 'Marca temporal de actualizacion',
    transaction_type STRING COMMENT 'Tipo de evento transaccional',
    error_code STRING COMMENT 'Codigo tipificado del fallo DQ',
    rejection_reason STRING COMMENT 'Explicacion del fallo DQ',
    rejected_at TIMESTAMP COMMENT 'Marca temporal UTC del desvio a cuarentena',
    _batch_id STRING COMMENT 'Identificador del lote',
    _source_file STRING COMMENT 'Archivo fuente'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS quarantine.customers_quarantine (
    customer_id STRING COMMENT 'Clave natural del cliente',
    name STRING COMMENT 'Nombre del cliente',
    email STRING COMMENT 'Correo electronico',
    country STRING COMMENT 'Pais del cliente',
    segment STRING COMMENT 'Segmento del cliente',
    updated_at STRING COMMENT 'Marca temporal de actualizacion',
    transaction_type STRING COMMENT 'Tipo de evento transaccional',
    error_code STRING COMMENT 'Codigo tipificado del fallo DQ',
    rejection_reason STRING COMMENT 'Explicacion del fallo DQ',
    rejected_at TIMESTAMP COMMENT 'Marca temporal UTC del desvio a cuarentena',
    _batch_id STRING COMMENT 'Identificador del lote',
    _source_file STRING COMMENT 'Archivo fuente'
)
USING DELTA;

-- ==============================================================================
-- 3. CAPA SILVER (Tablas Curadas, Deduplicadas & Modeladas SCD1 / SCD2)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS chubb_lakehouse.customers_silver (
    customer_sk BIGINT,
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    segment STRING,
    updated_at STRING,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    _batch_id STRING,
    _source_file STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.dim_customers_scd2 (
    customer_sk BIGINT,
    customer_id STRING,
    name STRING,
    email STRING,
    country STRING,
    segment STRING,
    updated_at STRING,
    effective_date TIMESTAMP,
    end_date TIMESTAMP,
    is_current BOOLEAN,
    _batch_id STRING,
    _source_file STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS chubb_lakehouse.orders_silver (
    order_id STRING,
    customer_id STRING,
    order_ts STRING,
    amount DOUBLE,
    currency STRING,
    status STRING,
    updated_at STRING,
    _batch_id STRING,
    _source_file STRING
)
USING DELTA;

CREATE TABLE IF NOT EXISTS silver.fct_orders_scd1 (
    order_id STRING,
    customer_id STRING,
    order_ts STRING,
    amount DOUBLE,
    currency STRING,
    status STRING,
    updated_at STRING,
    _batch_id STRING,
    _source_file STRING
)
USING DELTA;

-- Vistas de compatibilidad Silver
CREATE OR REPLACE VIEW silver.customers_silver AS
SELECT * FROM chubb_lakehouse.customers_silver;

CREATE OR REPLACE VIEW silver.orders_silver AS
SELECT * FROM chubb_lakehouse.orders_silver;

CREATE OR REPLACE VIEW chubb_lakehouse.dim_customers_scd2 AS
SELECT * FROM chubb_lakehouse.customers_silver;

CREATE OR REPLACE VIEW chubb_lakehouse.fct_orders_scd1 AS
SELECT * FROM chubb_lakehouse.orders_silver;

-- ==============================================================================
-- 4. CAPA GOLD (Tablas Analíticas, KPIs & Resumen de Calidad)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS gold.agg_sales_daily (
    order_date DATE COMMENT 'Fecha calendario de la agregacion diaria',
    completed_orders_count BIGINT COMMENT 'Cantidad de ordenes completadas con exito',
    gross_revenue DECIMAL(18, 2) COMMENT 'Ingresos brutos monetarios totales del dia',
    avg_ticket DECIMAL(18, 2) COMMENT 'Ticket promedio de venta del dia',
    unique_customers_count BIGINT COMMENT 'Numero de clientes distintos que compraron en el dia',
    _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo de la agregacion'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.agg_customer_kpis (
    customer_id STRING COMMENT 'Identificador natural del cliente',
    total_orders BIGINT COMMENT 'Cantidad acumulada de ordenes procesadas',
    total_spent DECIMAL(18, 2) COMMENT 'Monto monetario acumulado facturado al cliente',
    avg_order_value DECIMAL(18, 2) COMMENT 'Ticket promedio historico por cliente',
    first_order_date DATE COMMENT 'Fecha de la primera compra registrada',
    last_order_date DATE COMMENT 'Fecha de la transaccion mas reciente',
    rfm_recency_days INT COMMENT 'Dias transcurridos desde la ultima compra',
    customer_lifetime_status STRING COMMENT 'Segmento de actividad dinamico (ACTIVE, CHURN_RISK, INACTIVE)',
    _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo de la metrica'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS gold.agg_dq_summary (
    metric_date DATE COMMENT 'Fecha calendario del resumen de calidad',
    source_entity STRING COMMENT 'Entidad evaluada (customers, orders)',
    total_records_evaluated BIGINT COMMENT 'Total de registros evaluados por el DQ Gate',
    valid_records_count BIGINT COMMENT 'Registros conformes promovidos a Silver',
    quarantined_records_count BIGINT COMMENT 'Registros no conformes desviados a cuarentena',
    dq_pass_rate_pct DECIMAL(5, 2) COMMENT 'Porcentaje de aprobacion de calidad de datos',
    primary_failure_reason STRING COMMENT 'Causa principal de rechazo o aislamiento',
    _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo del resumen'
)
USING DELTA;

-- Vistas de compatibilidad Gold
CREATE OR REPLACE VIEW gold.gold_ventas_daily AS
SELECT * FROM gold.agg_sales_daily;

CREATE OR REPLACE VIEW gold.gold_customer_ltv AS
SELECT * FROM gold.agg_customer_kpis;

CREATE OR REPLACE VIEW gold.gold_dq_summary AS
SELECT * FROM gold.agg_dq_summary;

-- ==============================================================================
-- Tablas Gold Especializadas (Modelado Dimensional & DQ Observability)
-- ==============================================================================

CREATE TABLE IF NOT EXISTS chubb_lakehouse.ventas_diarias_x_pais (
    order_date DATE COMMENT 'Fecha calendario de la venta',
    country STRING COMMENT 'Pais del cliente asociado a la orden',
    total_orders BIGINT COMMENT 'Conteo total de ordenes completadas o activas',
    total_revenue DECIMAL(18, 2) COMMENT 'Suma total monetaria facturada',
    avg_ticket DECIMAL(18, 2) COMMENT 'Ticket promedio de venta (total_revenue / total_orders)',
    _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS chubb_lakehouse.customer_ltv (
    customer_id STRING COMMENT 'Identificador unico del cliente',
    first_order_ts TIMESTAMP COMMENT 'Marca temporal de la primera orden registrada',
    last_order_ts TIMESTAMP COMMENT 'Marca temporal de la ultima orden registrada',
    tenure_days INT COMMENT 'Antigüedad del cliente en dias entre primera y ultima orden',
    total_orders BIGINT COMMENT 'Cantidad acumulada de compras realizadas',
    ltv_amount DECIMAL(18, 2) COMMENT 'Monto monetario acumulado (Customer Lifetime Value)',
    preferred_segment STRING COMMENT 'Segmento de cliente predominante o vigente',
    _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo'
)
USING DELTA;

CREATE TABLE IF NOT EXISTS chubb_lakehouse.data_quality_summary (
    batch_id STRING COMMENT 'Identificador del lote evaluado',
    table_name STRING COMMENT 'Entidad o tabla origen (customers, orders)',
    total_received BIGINT COMMENT 'Total de registros recibidos en Capa Bronze',
    accepted_rows BIGINT COMMENT 'Total de registros conformes promovidos a Silver',
    quarantined_rows BIGINT COMMENT 'Total de registros anomalos desviados a Cuarentena',
    rejection_reasons STRING COMMENT 'Desglose detallado por codigo de rechazo en formato JSON',
    deduplicated_rows BIGINT COMMENT 'Total de registros descartados por deduplicacion',
    dq_pass_rate_pct DECIMAL(5, 2) COMMENT 'Tasa porcentual de aceptacion de calidad',
    _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de generacion del resumen'
)
USING DELTA;

-- Vistas de compatibilidad Gold
CREATE OR REPLACE VIEW gold.ventas_diarias_x_pais AS
SELECT * FROM chubb_lakehouse.ventas_diarias_x_pais;

CREATE OR REPLACE VIEW gold.customer_ltv AS
SELECT * FROM chubb_lakehouse.customer_ltv;

CREATE OR REPLACE VIEW gold.data_quality_summary AS
SELECT * FROM chubb_lakehouse.data_quality_summary;


