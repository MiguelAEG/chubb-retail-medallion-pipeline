"""
==============================================================================
MÓDULO: src.bronze.customers_bronze
PROPÓSITO:
Ingesta cruda (raw) fidedigna e incremental para la entidad 'customers'
hacia la tabla Delta de la Capa Bronze en Unity Catalog:
- chubb_lakehouse.bronze.raw_customers

Control estricto de idempotencia y no-reprocesamiento mediante la tabla de auditoría:
- chubb_lakehouse.bronze.bronze_ingestion_log

Inyección determinista de metadatos técnicos de linaje:
- _raw_hash: Hash xxhash64 (BIGINT LongType de 8 bytes, optimizado en Databricks Photon)
- _record_hash: Alias idéntico para retrocompatibilidad downstream
- _source_file: Nombre/ruta del archivo procesado
- _source_system: Identificador del sistema de origen ('csv_landing')
- _batch_id: Identificador del lote (ej. 'initial_part1', 'batch_1', etc.)
- _ingestion_ts: Timestamp UTC de ingesta
- _ingested_at: Timestamp UTC de ingesta
==============================================================================
"""

import logging
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from src.utils.common import (
    compute_python_raw_hash,
    compute_raw_hash_col,
    extract_batch_id,
    filter_pending_files,
    get_ingested_files,
    list_landing_files,
    record_ingestion_log,
)

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("bronze.customers")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Columnas de negocio canónicas según contrato DC-2026-CUSTOMERS
CUSTOMER_BUSINESS_COLUMNS: List[str] = [
    "customer_id",
    "name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "country",
    "segment",
    "address",
    "updated_at",
    "transaction_type",
]


def ingest_customer_file(
    spark: Any,
    catalog_name: str,
    file_path: str,
    batch_id: Optional[str] = None,
    schema: str = "chubb_lakehouse",
) -> Dict[str, Any]:
    """
    Ejecuta la ingesta fidedigna de un archivo CSV individual de clientes hacia customers_bronze.
    Inyecta metadatos técnicos de linaje y registra el evento en bronze_ingestion_log.
    """
    from pyspark.sql import functions as F

    resolved_batch_id = batch_id or extract_batch_id(file_path)
    file_name = Path(file_path).name
    target_table = f"{catalog_name}.{schema}.customers_bronze"

    logger.info(
        '{"event": "start_customer_file_ingestion", "file": "%s", "batch_id": "%s"}',
        file_name,
        resolved_batch_id,
    )

    try:
        # Lectura cruda sin inferencia de esquemas para preservar integridad fidedigna
        df_raw = (
            spark.read.format("csv")
            .option("header", "true")
            .option("inferSchema", "false")
            .load(file_path)
        )

        rows_read = df_raw.count()

        # Inyección de metadatos de linaje y auditoría
        raw_hash_expr = compute_raw_hash_col(CUSTOMER_BUSINESS_COLUMNS)
        now_ts = F.current_timestamp()

        df_bronze = (
            df_raw
            .withColumn("_raw_hash", raw_hash_expr)
            .withColumn("_record_hash", raw_hash_expr)
            .withColumn("_source_file", F.lit(file_name))
            .withColumn("_source_system", F.lit("csv_landing"))
            .withColumn("_batch_id", F.lit(resolved_batch_id))
            .withColumn("_ingestion_ts", now_ts)
            .withColumn("_ingested_at", now_ts)
        )

        # Inserción append-only en la tabla Bronze Delta
        (
            df_bronze.write
            .format("delta")
            .mode("append")
            .saveAsTable(target_table)
        )

        rows_ingested = rows_read

        # Registro exitoso en tabla de auditoría
        record_ingestion_log(
            spark=spark,
            catalog_name=catalog_name,
            batch_id=resolved_batch_id,
            source_file=file_name,
            entity="customers",
            rows_read=rows_read,
            rows_ingested=rows_ingested,
            status="SUCCESS",
            error_message=None,
        )

        return {
            "file": file_name,
            "entity": "customers",
            "batch_id": resolved_batch_id,
            "rows_read": rows_read,
            "rows_ingested": rows_ingested,
            "status": "SUCCESS",
        }

    except Exception as exc:
        error_msg = str(exc)
        logger.error(
            '{"event": "customer_file_ingestion_error", "file": "%s", "error": "%s"}',
            file_name,
            error_msg,
        )
        try:
            record_ingestion_log(
                spark=spark,
                catalog_name=catalog_name,
                batch_id=resolved_batch_id,
                source_file=file_name,
                entity="customers",
                rows_read=0,
                rows_ingested=0,
                status="FAILED",
                error_message=error_msg,
            )
        except Exception as log_err:
            logger.error('{"event": "failed_to_log_customer_failure", "error": "%s"}', str(log_err))

        raise exc


def ingest_customers_bronze(
    spark: Any,
    catalog_name: str,
    schema: str = "chubb_lakehouse",
    landing_volume: Optional[str] = None,
    explicit_batch_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orquestador de ingesta incremental para la entidad Customers en Capa Bronze.
    1. Descubre archivos 'customers_*.csv' en el volumen/directorio de landing.
    2. Descarta archivos previamente procesados (idempotencia estricta vía bronze_ingestion_log).
    3. Ingesta secuencialmente cada archivo pendiente, calculando _raw_hash y linaje.
    """
    if landing_volume is None:
        landing_volume = f"/Volumes/{catalog_name}/{schema}/raw_landing"

    ingested_files = get_ingested_files(spark, catalog_name=catalog_name)
    available_files = list_landing_files(landing_volume, "customers")
    pending_files = filter_pending_files(available_files, ingested_files)

    summary: Dict[str, Any] = {
        "entity": "customers",
        "catalog_name": catalog_name,
        "schema": schema,
        "landing_volume": landing_volume,
        "available_count": len(available_files),
        "pending_count": len(pending_files),
        "files_ingested": 0,
        "rows_ingested": 0,
        "details": [],
    }

    logger.info(
        '{"event": "customers_bronze_start", "catalog": "%s", "available": %d, "pending": %d}',
        catalog_name,
        len(available_files),
        len(pending_files),
    )

    for file_path in pending_files:
        result = ingest_customer_file(
            spark=spark,
            catalog_name=catalog_name,
            file_path=file_path,
            batch_id=explicit_batch_id,
            schema=schema,
        )
        summary["details"].append(result)
        summary["files_ingested"] += 1
        summary["rows_ingested"] += result["rows_ingested"]
        ingested_files.add(result["file"])

    logger.info(
        '{"event": "customers_bronze_completed", "files_ingested": %d, "rows_ingested": %d}',
        summary["files_ingested"],
        summary["rows_ingested"],
    )

    return summary
