"""
==============================================================================
MÓDULO: src.setup.ddl.init_tables
PROPÓSITO:
Aprovisionamiento idempotente de esquemas, volúmenes de Unity Catalog y
definición DDL de tablas Delta y vistas unificadas para el Lakehouse Medallion.
Sirve como la Fuente Única de Verdad (SSOT) para las estructuras de datos.
==============================================================================
"""

import logging
from typing import Any, Dict, List

logger = logging.getLogger("init_tables")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def get_ddl_statements(catalog: str = "chubb_lakehouse", schema: str = "chubb_lakehouse") -> List[str]:
    """Genera la lista exhaustiva de sentencias DDL parametrizadas por catálogo y esquema."""
    return [
        # 1. Esquema principal y esquemas de la arquitectura Medallion
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'Esquema principal unificado de Chubb Lakehouse'",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze COMMENT 'Capa Bronze: Tablas Delta crudas append-only y metadatos tecnicos de auditoria'",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver COMMENT 'Capa Silver: Tablas conformadas, limpias, deduplicadas (SCD1 / SCD2)'",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.quarantine COMMENT 'Capa Quarantine: Dead-letter queue para aislamiento de registros anomalos'",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold COMMENT 'Capa Gold: Vistas y tablas analiticas con KPIs agregados de negocio'",
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.audit COMMENT 'Capa Audit: Bitacoras de ejecucion, ingesta y observabilidad operacional'",

        # 2. Volúmenes Unity Catalog para recepción landing
        f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw_landing COMMENT 'Landing zone principal'",
        f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.raw_landing COMMENT 'Landing zone Bronze'",

        # 3. Capa Bronze (Raw Lakehouse & Auditoría de Ingesta)
        # Tablas Delta físicas directas en el esquema unificado {catalog}.{schema}
        f"""
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
            _raw_hash BIGINT COMMENT 'Hash determinista xxhash64 de la fila cruda',
            _record_hash BIGINT COMMENT 'Hash determinista xxhash64 de campos de negocio',
            _source_file STRING COMMENT 'Nombre o ruta del archivo procesado',
            _source_system STRING COMMENT 'Identificador del sistema de origen emisor',
            _batch_id STRING COMMENT 'Identificador unico del lote de procesamiento',
            _ingestion_ts TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse',
            _ingested_at TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse'
        ) USING DELTA
        """,
        f"""
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
            _raw_hash BIGINT COMMENT 'Hash determinista xxhash64 de la fila cruda',
            _record_hash BIGINT COMMENT 'Hash determinista xxhash64 de campos de negocio',
            _source_file STRING COMMENT 'Nombre o ruta del archivo procesado',
            _source_system STRING COMMENT 'Identificador del sistema de origen emisor',
            _batch_id STRING COMMENT 'Identificador unico del lote de procesamiento',
            _ingestion_ts TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse',
            _ingested_at TIMESTAMP COMMENT 'Marca temporal UTC de ingesta en el lakehouse'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.bronze_ingestion_log (
            batch_id STRING COMMENT 'Identificador unico del lote ingerido',
            source_file STRING COMMENT 'Nombre o ruta del archivo de origen',
            entity STRING COMMENT 'Entidad procesada (customers, orders)',
            rows_read BIGINT COMMENT 'Total de filas leidas del archivo fuente',
            rows_ingested BIGINT COMMENT 'Total de filas insertadas con exito en Bronze',
            status STRING COMMENT 'Estado final de la ingesta (SUCCESS, FAILED, PARTIAL)',
            error_message STRING COMMENT 'Detalle del error en caso de fallo',
            ingested_at TIMESTAMP COMMENT 'Marca temporal UTC de registro del evento'
        ) USING DELTA
        """,
        # Vistas de retrocompatibilidad en esquema bronze hacia las tablas físicas
        f"CREATE OR REPLACE VIEW {catalog}.bronze.raw_customers AS SELECT * FROM {catalog}.{schema}.customers_bronze",
        f"CREATE OR REPLACE VIEW {catalog}.bronze.raw_orders AS SELECT * FROM {catalog}.{schema}.orders_bronze",
        f"CREATE OR REPLACE VIEW {catalog}.bronze.bronze_ingestion_log AS SELECT * FROM {catalog}.{schema}.bronze_ingestion_log",

        # 4. Capa Quarantine (Dead-Letter Queue / Aislamiento de Defectos)
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.quarantine.quarantine_records (
            quarantine_id STRING COMMENT 'Identificador UUID unico de la incidencia',
            source_entity STRING COMMENT 'Entidad de origen evaluada (customers, orders)',
            _batch_id STRING COMMENT 'Identificador del lote o correlacion',
            raw_payload STRING COMMENT 'Carga util completa original en formato JSON crudo',
            quarantine_reason STRING COMMENT 'Explicacion de la regla de calidad violada',
            error_code STRING COMMENT 'Codigo tipificado de la regla violada',
            failed_at TIMESTAMP COMMENT 'Marca temporal UTC de deteccion y aislamiento'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_quarantine (
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
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.quarantine.orders_quarantine (
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
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customers_quarantine (
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
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.quarantine.customers_quarantine (
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
        ) USING DELTA
        """,

        # 5. Capa Silver (Tablas Curadas, Deduplicadas & Modeladas SCD1 / SCD2)
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customers_silver (
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
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.dim_customers_scd2 (
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
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.orders_silver (
            order_id STRING,
            customer_id STRING,
            order_ts STRING,
            amount DOUBLE,
            currency STRING,
            status STRING,
            updated_at STRING,
            _batch_id STRING,
            _source_file STRING
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.silver.fct_orders_scd1 (
            order_id STRING,
            customer_id STRING,
            order_ts STRING,
            amount DOUBLE,
            currency STRING,
            status STRING,
            updated_at STRING,
            _batch_id STRING,
            _source_file STRING
        ) USING DELTA
        """,

        # 6. Capa Gold (Tablas Analíticas, KPIs & Resumen de Calidad)
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.agg_sales_daily (
            order_date DATE COMMENT 'Fecha calendario de la agregacion diaria',
            completed_orders_count BIGINT COMMENT 'Cantidad de ordenes completadas con exito',
            gross_revenue DECIMAL(18, 2) COMMENT 'Ingresos brutos monetarios totales del dia',
            avg_ticket DECIMAL(18, 2) COMMENT 'Ticket promedio de venta del dia',
            unique_customers_count BIGINT COMMENT 'Numero de clientes distintos que compraron en el dia',
            _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo de la agregacion'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.agg_customer_kpis (
            customer_id STRING COMMENT 'Identificador natural del cliente',
            total_orders BIGINT COMMENT 'Cantidad acumulada de ordenes procesadas',
            total_spent DECIMAL(18, 2) COMMENT 'Monto monetario acumulado facturado al cliente',
            avg_order_value DECIMAL(18, 2) COMMENT 'Ticket promedio historico por cliente',
            first_order_date DATE COMMENT 'Fecha de la primera compra registrada',
            last_order_date DATE COMMENT 'Fecha de la transaccion mas reciente',
            rfm_recency_days INT COMMENT 'Dias transcurridos desde la ultima compra',
            customer_lifetime_status STRING COMMENT 'Segmento de actividad dinamico (ACTIVE, CHURN_RISK, INACTIVE)',
            _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo de la metrica'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.gold.agg_dq_summary (
            metric_date DATE COMMENT 'Fecha calendario del resumen de calidad',
            source_entity STRING COMMENT 'Entidad evaluada (customers, orders)',
            total_records_evaluated BIGINT COMMENT 'Total de registros evaluados por el DQ Gate',
            valid_records_count BIGINT COMMENT 'Registros conformes promovidos a Silver',
            quarantined_records_count BIGINT COMMENT 'Registros no conformes desviados a cuarentena',
            dq_pass_rate_pct DECIMAL(5, 2) COMMENT 'Porcentaje de aprobacion de calidad de datos',
            primary_failure_reason STRING COMMENT 'Causa principal de rechazo o aislamiento',
            _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo del resumen'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.ventas_diarias_x_pais (
            order_date DATE COMMENT 'Fecha calendario de la venta',
            country STRING COMMENT 'Pais del cliente asociado a la orden',
            total_orders BIGINT COMMENT 'Conteo total de ordenes completadas o activas',
            total_revenue DECIMAL(18, 2) COMMENT 'Suma total monetaria facturada',
            avg_ticket DECIMAL(18, 2) COMMENT 'Ticket promedio de venta (total_revenue / total_orders)',
            _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.customer_ltv (
            customer_id STRING COMMENT 'Identificador unico del cliente',
            first_order_ts TIMESTAMP COMMENT 'Marca temporal de la primera orden registrada',
            last_order_ts TIMESTAMP COMMENT 'Marca temporal de la ultima orden registrada',
            tenure_days INT COMMENT 'Antigüedad del cliente en dias entre primera y ultima orden',
            total_orders BIGINT COMMENT 'Cantidad acumulada de compras realizadas',
            ltv_amount DECIMAL(18, 2) COMMENT 'Monto monetario acumulado (Customer Lifetime Value)',
            preferred_segment STRING COMMENT 'Segmento de cliente predominante o vigente',
            _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de calculo'
        ) USING DELTA
        """,
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema}.data_quality_summary (
            batch_id STRING COMMENT 'Identificador del lote evaluado',
            table_name STRING COMMENT 'Entidad o tabla origen (customers, orders)',
            total_received BIGINT COMMENT 'Total de registros recibidos en Capa Bronze',
            accepted_rows BIGINT COMMENT 'Total de registros conformes promovidos a Silver',
            quarantined_rows BIGINT COMMENT 'Total de registros anomalos desviados a Cuarentena',
            rejection_reasons STRING COMMENT 'Desglose detallado por codigo de rechazo en formato JSON',
            deduplicated_rows BIGINT COMMENT 'Total de registros descartados por deduplicacion',
            dq_pass_rate_pct DECIMAL(5, 2) COMMENT 'Tasa porcentual de aceptacion de calidad',
            _calculated_at TIMESTAMP COMMENT 'Marca temporal UTC de generacion del resumen'
        ) USING DELTA
        """,

        # 7. Vistas unificadas en esquema principal (chubb_lakehouse)
        f"CREATE OR REPLACE VIEW {catalog}.{schema}.dim_customers_scd2 AS SELECT * FROM {catalog}.{schema}.customers_silver",
        f"CREATE OR REPLACE VIEW {catalog}.{schema}.fct_orders_scd1 AS SELECT * FROM {catalog}.{schema}.orders_silver",
        f"CREATE OR REPLACE VIEW {catalog}.{schema}.quarantine_records AS SELECT * FROM {catalog}.quarantine.quarantine_records",
        f"CREATE OR REPLACE VIEW {catalog}.{schema}.agg_sales_daily AS SELECT * FROM {catalog}.gold.agg_sales_daily",
        f"CREATE OR REPLACE VIEW {catalog}.{schema}.agg_customer_kpis AS SELECT * FROM {catalog}.gold.agg_customer_kpis",
        f"CREATE OR REPLACE VIEW {catalog}.{schema}.agg_dq_summary AS SELECT * FROM {catalog}.gold.agg_dq_summary",

        # 8. Vistas de compatibilidad en capas Silver y Gold
        f"CREATE OR REPLACE VIEW {catalog}.silver.customers_silver AS SELECT * FROM {catalog}.{schema}.customers_silver",
        f"CREATE OR REPLACE VIEW {catalog}.silver.orders_silver AS SELECT * FROM {catalog}.{schema}.orders_silver",
        f"CREATE OR REPLACE VIEW {catalog}.gold.ventas_diarias_x_pais AS SELECT * FROM {catalog}.{schema}.ventas_diarias_x_pais",
        f"CREATE OR REPLACE VIEW {catalog}.gold.customer_ltv AS SELECT * FROM {catalog}.{schema}.customer_ltv",
        f"CREATE OR REPLACE VIEW {catalog}.gold.data_quality_summary AS SELECT * FROM {catalog}.{schema}.data_quality_summary",
        f"CREATE OR REPLACE VIEW {catalog}.gold.gold_ventas_daily AS SELECT * FROM {catalog}.gold.agg_sales_daily",
        f"CREATE OR REPLACE VIEW {catalog}.gold.gold_customer_ltv AS SELECT * FROM {catalog}.gold.agg_customer_kpis",
        f"CREATE OR REPLACE VIEW {catalog}.gold.gold_dq_summary AS SELECT * FROM {catalog}.gold.agg_dq_summary",
    ]


def create_all_tables(spark: Any, catalog: str = "chubb_lakehouse", schema: str = "chubb_lakehouse") -> Dict[str, Any]:
    """
    Ejecuta de forma secuencial e idempotente todas las sentencias DDL requeridas
    para inicializar el Lakehouse Medallion en Databricks con Unity Catalog.
    Si una sentencia falla, registra el error claramente y lanza una excepción fail-fast.
    """
    logger.info(
        '{"event": "start_ddl_provisioning", "catalog": "%s", "schema": "%s"}',
        catalog,
        schema,
    )

    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    statements = get_ddl_statements(catalog=catalog, schema=schema)
    executed_count = 0

    for stmt in statements:
        trimmed_stmt = stmt.strip()
        if not trimmed_stmt:
            continue
        try:
            spark.sql(trimmed_stmt)
            executed_count += 1
        except Exception as err:
            err_msg = str(err)
            logger.error(
                '{"event": "ddl_statement_failure", "statement": "%s", "error": "%s"}',
                trimmed_stmt.replace("\n", " "),
                err_msg,
            )
            raise RuntimeError(
                f"Fallo crítico en aprovisionamiento DDL para '{trimmed_stmt[:80]}...': {err_msg}"
            ) from err

    result = {
        "status": "SUCCESS",
        "catalog": catalog,
        "schema": schema,
        "total_statements": len(statements),
        "executed_count": executed_count,
        "warnings_count": 0,
    }

    logger.info(
        '{"event": "ddl_provisioning_completed", "executed": %d, "total": %d}',
        executed_count,
        len(statements),
    )
    return result
