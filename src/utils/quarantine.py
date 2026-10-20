"""
==============================================================================
MÓDULO: src.utils.quarantine
PROPÓSITO:
Aislamiento sistemático y enrutamiento hacia la Capa Quarantine (Dead-Letter Queue):
- Desvía registros que no superan las reglas de calidad hacia:
  * chubb_lakehouse.quarantine.orders_quarantine (y chubb_lakehouse.orders_quarantine)
  * chubb_lakehouse.quarantine.customers_quarantine (y chubb_lakehouse.customers_quarantine)
- Inyecta metadatos técnicos de aislamiento:
  * error_code, rejection_reason, rejected_at, _batch_id, _source_file
- Retorna exclusivamente el conjunto de registros limpios (valid) para promoción a Silver
- Garantiza el principio de Zero Silent Drops y append-only en Delta Lake
==============================================================================
"""

import datetime
import logging
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

from src.utils.data_quality import validate_customer_record, validate_order_record

try:
    from delta.tables import DeltaTable  # type: ignore
except ImportError:
    class _DeltaTableFallback:  # type: ignore
        @staticmethod
        def forName(spark: Any, table_name: str) -> Any:
            raise NotImplementedError("DeltaTable requires delta-spark or Databricks runtime")
    DeltaTable = _DeltaTableFallback  # type: ignore

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("utils.quarantine")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def route_quarantine_records(
    records: List[Dict[str, Any]],
    entity: str,
    valid_customer_ids: Optional[Set[str]] = None,
    batch_id: str = "batch_default",
    source_file: str = "unknown_source",
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Evalúa en memoria una colección de registros crudos en formato diccionario.
    Separa los registros conformes de los defectuosos, inyectando metadatos de cuarentena.
    Retorna: (valid_records, quarantined_records)
    """
    valid_records: List[Dict[str, Any]] = []
    quarantined_records: List[Dict[str, Any]] = []
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    for row in records:
        row_copy = dict(row)
        if entity == "customers":
            is_valid, err_code, reason = validate_customer_record(row_copy)
        elif entity == "orders":
            is_valid, err_code, reason = validate_order_record(row_copy, valid_customer_ids=valid_customer_ids)
        else:
            raise ValueError(f"Entidad no soportada para cuarentena: '{entity}'")

        if is_valid:
            valid_records.append(row_copy)
        else:
            row_copy["error_code"] = err_code
            row_copy["rejection_reason"] = reason
            row_copy["rejected_at"] = now_iso
            row_copy["_batch_id"] = row_copy.get("_batch_id", batch_id)
            row_copy["_source_file"] = row_copy.get("_source_file", source_file)
            quarantined_records.append(row_copy)

    logger.info(
        '{"event": "route_quarantine_memory", "entity": "%s", "total": %d, "valid": %d, "quarantined": %d}',
        entity,
        len(records),
        len(valid_records),
        len(quarantined_records),
    )
    return valid_records, quarantined_records


def route_quarantine(
    spark: Any,
    df_evaluated: Any,
    entity: str,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
    default_batch_id: str = "batch_silver",
    default_source_file: str = "bronze_stream",
) -> Tuple[Any, int, int]:
    """
    Enruta registros evaluados por Data Quality hacia las tablas Delta de Cuarentena.
    Aplica proyección .select() estricta conforme al contrato DDL sin mergeSchema.
    Retorna:
    - df_valid: DataFrame de PySpark limpio con registros conformes (columnas DQ eliminadas)
    - valid_count: Conteo de registros válidos
    - quarantined_count: Conteo de registros desviados a cuarentena
    """
    from pyspark.sql import functions as F

    cat = catalog_name if catalog_name is not None else catalog
    quarantine_table = f"{cat}.{schema}.{entity}_quarantine"

    # Separación en dos streams lógicos
    df_quarantine = df_evaluated.filter(~F.col("is_valid"))
    df_valid = df_evaluated.filter(F.col("is_valid")).drop("is_valid", "error_code", "rejection_reason")

    quarantined_count = df_quarantine.count()
    valid_count = df_valid.count()

    if quarantined_count > 0:
        now_ts = F.current_timestamp()
        cols = set(df_quarantine.columns)

        df_to_quarantine = df_quarantine.withColumn("rejected_at", now_ts)
        if "_batch_id" not in cols:
            df_to_quarantine = df_to_quarantine.withColumn("_batch_id", F.lit(default_batch_id))
        if "_source_file" not in cols:
            df_to_quarantine = df_to_quarantine.withColumn("_source_file", F.lit(default_source_file))

        if entity == "orders":
            if "order_ts" not in cols:
                if "order_timestamp" in cols:
                    df_to_quarantine = df_to_quarantine.withColumn("order_ts", F.col("order_timestamp").cast("string"))
                elif "order_date" in cols:
                    df_to_quarantine = df_to_quarantine.withColumn("order_ts", F.col("order_date").cast("string"))
                else:
                    df_to_quarantine = df_to_quarantine.withColumn("order_ts", F.lit(None).cast("string"))
            if "amount" not in cols:
                if "total_amount" in cols:
                    df_to_quarantine = df_to_quarantine.withColumn("amount", F.col("total_amount").cast("string"))
                else:
                    df_to_quarantine = df_to_quarantine.withColumn("amount", F.lit(None).cast("string"))
            if "status" not in cols:
                if "order_status" in cols:
                    df_to_quarantine = df_to_quarantine.withColumn("status", F.col("order_status").cast("string"))
                else:
                    df_to_quarantine = df_to_quarantine.withColumn("status", F.lit(None).cast("string"))

            for c in ["order_id", "customer_id", "currency", "updated_at", "transaction_type", "error_code", "rejection_reason"]:
                if c not in cols:
                    df_to_quarantine = df_to_quarantine.withColumn(c, F.lit(None).cast("string"))

            quarantine_cols = [
                "order_id", "customer_id", "order_ts", "amount", "currency",
                "status", "updated_at", "transaction_type", "error_code",
                "rejection_reason", "rejected_at", "_batch_id", "_source_file"
            ]

        elif entity == "customers":
            if "name" not in cols:
                if "first_name" in cols and "last_name" in cols:
                    df_to_quarantine = df_to_quarantine.withColumn(
                        "name",
                        F.concat_ws(" ", F.coalesce(F.col("first_name"), F.lit("")), F.coalesce(F.col("last_name"), F.lit("")))
                    )
                elif "first_name" in cols:
                    df_to_quarantine = df_to_quarantine.withColumn("name", F.col("first_name").cast("string"))
                else:
                    df_to_quarantine = df_to_quarantine.withColumn("name", F.lit(None).cast("string"))
            if "country" not in cols:
                df_to_quarantine = df_to_quarantine.withColumn("country", F.lit("UNKNOWN"))

            for c in ["customer_id", "email", "segment", "updated_at", "transaction_type", "error_code", "rejection_reason"]:
                if c not in cols:
                    df_to_quarantine = df_to_quarantine.withColumn(c, F.lit(None).cast("string"))

            quarantine_cols = [
                "customer_id", "name", "email", "country", "segment",
                "updated_at", "transaction_type", "error_code",
                "rejection_reason", "rejected_at", "_batch_id", "_source_file"
            ]
        else:
            raise ValueError(f"Entidad no soportada para cuarentena: '{entity}'")

        df_target = df_to_quarantine.select(quarantine_cols)

        merge_cond = "target._batch_id = source._batch_id AND target.error_code = source.error_code"
        if "_raw_hash" in df_target.columns:
            merge_cond += " AND target._raw_hash = source._raw_hash"
        elif "order_id" in df_target.columns:
            merge_cond += " AND coalesce(target.order_id, '') = coalesce(source.order_id, '')"
        elif "customer_id" in df_target.columns:
            merge_cond += " AND coalesce(target.customer_id, '') = coalesce(source.customer_id, '')"

        def _merge_into_quarantine(tbl_name: str) -> None:
            delta_target = DeltaTable.forName(spark, tbl_name)
            (
                delta_target.alias("target")
                .merge(df_target.alias("source"), merge_cond)
                .whenNotMatchedInsertAll()
                .execute()
            )

        try:
            _merge_into_quarantine(quarantine_table)
            logger.info(
                '{"event": "quarantine_persisted", "table": "%s", "quarantined_rows": %d}',
                quarantine_table,
                quarantined_count,
            )
        except Exception as exc:
            alt_table = f"{cat}.quarantine.{entity}_quarantine"
            logger.warning(
                '{"event": "quarantine_retry_fallback", "primary": "%s", "fallback": "%s", "reason": "%s"}',
                quarantine_table,
                alt_table,
                str(exc),
            )
            try:
                _merge_into_quarantine(alt_table)
            except Exception:
                # Si la tabla no está creada como DeltaTable en entorno de testing, fallback a append
                df_target.write.format("delta").mode("append").saveAsTable(alt_table)

    return df_valid, valid_count, quarantined_count
