"""
==============================================================================
MÓDULO: src.utils.dq_summary
PROPÓSITO:
Reconciliación matemática y observabilidad de calidad de datos (Data Quality Summary):
- Grano analítico único por 'batch_id' y 'table_name'.
- Reconciliación de conservación de masa de datos (Zero Silent Drops):
  total_received = accepted_rows + quarantined_rows + deduplicated_rows
- Métricas calculadas:
  * total_received: Registros crudos recibidos en Bronze para el lote.
  * accepted_rows: Registros válidos promovidos exitosamente a Silver.
  * quarantined_rows: Registros anómalos desviados a la Capa Quarantine.
  * rejection_reasons: Desglose tipificado por error_code en formato JSON
    (ERR_NULL_PK, ERR_NEGATIVE_AMOUNT, ERR_INVALID_DATE, ERR_ORPHAN_CUSTOMER).
  * deduplicated_rows: Registros redundantes consolidados en la promoción.
  * dq_pass_rate_pct: Tasa porcentual de aceptación de calidad ((accepted_rows / total_received) * 100).
- Escritura y actualización idempotente vía MERGE INTO Delta Lake por (batch_id, table_name).
- Cero hints SQL manuales (optimización delegada a Catalyst / AQE).
==============================================================================
"""

from collections import Counter
import datetime
import json
import logging
import sys
from typing import Any, Dict, List, Optional

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("utils.dq_summary")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def compute_dq_summary_memory(
    bronze_records: List[Dict[str, Any]],
    silver_records: List[Dict[str, Any]],
    quarantine_records: List[Dict[str, Any]],
    table_name: str = "orders",
    calculated_at: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Calcula en memoria la reconciliación matemática y resumen de calidad de datos
    por lote (batch_id) y entidad (table_name) de forma determinista y quirúrgica.

    Parámetros:
        bronze_records: Registros brutos leídos e ingeridos en Bronze.
        silver_records: Registros válidos promovidos y almacenados en Silver.
        quarantine_records: Registros rechazados desviados a Cuarentena.
        table_name: Nombre de la tabla o entidad evaluada ('orders' o 'customers').
        calculated_at: Marca temporal UTC de cálculo opcional.

    Retorna:
        Lista de diccionarios con la reconciliación por batch_id.
    """
    now_iso = (
        calculated_at
        if calculated_at
        else datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )

    # Identificar todos los batch_id presentes en Bronze, Silver o Quarantine
    all_batches = set()
    for r in bronze_records:
        bid = str(r.get("_batch_id", "")).strip() or "batch_default"
        all_batches.add(bid)
    for r in silver_records:
        bid = str(r.get("_batch_id", "")).strip()
        if bid:
            all_batches.add(bid)
    for r in quarantine_records:
        bid = str(r.get("_batch_id", "")).strip()
        if bid:
            all_batches.add(bid)

    if not all_batches:
        all_batches.add("batch_default")

    results: List[Dict[str, Any]] = []

    for bid in sorted(all_batches):
        # 1. Total recibidos en Bronze para este lote
        b_rows = [
            r for r in bronze_records
            if (str(r.get("_batch_id", "")).strip() or "batch_default") == bid
        ]
        total_received = len(b_rows)

        # 2. Desviados a cuarentena para este lote
        q_rows = [
            r for r in quarantine_records
            if (str(r.get("_batch_id", "")).strip() or "batch_default") == bid
        ]
        quarantined_count = len(q_rows)

        # Desglose de motivos de rechazo por error_code
        err_counter = Counter()
        for qr in q_rows:
            code = str(qr.get("error_code", "")).strip() or "ERR_UNKNOWN"
            err_counter[code] += 1
        rejection_reasons_str = json.dumps(dict(err_counter), sort_keys=True)

        # 3. Registros aceptados en Silver para este lote
        s_rows = [
            r for r in silver_records
            if (str(r.get("_batch_id", "")).strip() or "batch_default") == bid
        ]
        accepted_count = len(s_rows)

        # 4. Registros deduplicados (reconciliación matemática de conservación)
        # total_received = accepted_rows + quarantined_rows + deduplicated_rows
        expected_clean = total_received - quarantined_count
        if expected_clean >= accepted_count:
            deduplicated_count = expected_clean - accepted_count
        else:
            deduplicated_count = 0

        # Tasa de aprobación
        if total_received > 0:
            pass_rate = round((accepted_count / total_received) * 100.0, 2)
        else:
            pass_rate = 100.0

        results.append({
            "batch_id": bid,
            "table_name": table_name,
            "total_received": total_received,
            "accepted_rows": accepted_count,
            "quarantined_rows": quarantined_count,
            "rejection_reasons": rejection_reasons_str,
            "deduplicated_rows": deduplicated_count,
            "dq_pass_rate_pct": float(pass_rate),
            "_calculated_at": now_iso,
        })

    logger.info(
        '{"event": "compute_dq_summary_memory_completed", "table": "%s", "batches_processed": %d}',
        table_name,
        len(results),
    )
    return results


def compute_dq_summary_df(
    spark: Any,
    df_bronze: Any,
    df_silver: Any,
    df_quarantine: Any,
    table_name: str = "orders",
) -> Any:
    """
    Calcula el resumen de calidad y reconciliación analítica usando DataFrames de PySpark.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, LongType, StringType

    # Resumen de Bronze por batch_id
    bronze_counts = (
        df_bronze.groupBy(F.coalesce(F.col("_batch_id"), F.lit("batch_default")).alias("batch_id"))
        .agg(F.count("*").alias("total_received"))
    )

    # Resumen de Silver por batch_id
    silver_counts = (
        df_silver.groupBy(F.coalesce(F.col("_batch_id"), F.lit("batch_default")).alias("batch_id"))
        .agg(F.count("*").alias("accepted_rows"))
    )

    # Resumen de Cuarentena por batch_id y error_code (dos fases para evitar DUPLICATED_MAP_KEY)
    q_by_code = (
        df_quarantine.groupBy(
            F.coalesce(F.col("_batch_id"), F.lit("batch_default")).alias("batch_id"),
            F.coalesce(F.col("error_code"), F.lit("ERR_UNKNOWN")).alias("error_code"),
        ).agg(F.count("*").alias("code_count"))
    )

    quarantine_counts = (
        q_by_code.groupBy("batch_id").agg(
            F.sum("code_count").cast(LongType()).alias("quarantined_rows"),
            F.to_json(
                F.map_from_entries(
                    F.collect_list(
                        F.struct(
                            F.col("error_code").alias("key"),
                            F.col("code_count").alias("val"),
                        )
                    )
                )
            ).alias("rejection_reasons"),
        )
    )

    # Consolidación relacional
    df_joined = (
        bronze_counts.join(silver_counts, on="batch_id", how="left")
        .join(quarantine_counts, on="batch_id", how="left")
        .select(
            F.col("batch_id"),
            F.lit(table_name).alias("table_name"),
            F.coalesce(F.col("total_received"), F.lit(0)).cast(LongType()).alias("total_received"),
            F.coalesce(F.col("accepted_rows"), F.lit(0)).cast(LongType()).alias("accepted_rows"),
            F.coalesce(F.col("quarantined_rows"), F.lit(0)).cast(LongType()).alias("quarantined_rows"),
            F.coalesce(F.col("rejection_reasons"), F.lit("{}")).cast(StringType()).alias("rejection_reasons"),
        )
        .withColumn(
            "deduplicated_rows",
            F.greatest(
                F.lit(0),
                F.col("total_received") - F.col("accepted_rows") - F.col("quarantined_rows"),
            ).cast(LongType()),
        )
        .withColumn(
            "dq_pass_rate_pct",
            F.when(
                F.col("total_received") > 0,
                F.round((F.col("accepted_rows") / F.col("total_received")) * 100.0, 2),
            ).otherwise(F.lit(100.00)).cast(DecimalType(5, 2)),
        )
        .withColumn("_calculated_at", F.current_timestamp())
    )

    return df_joined


def merge_dq_summary(
    spark: Any,
    df_dq_summary: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ejecuta la operación atómica e idempotente MERGE INTO en Delta Lake
    sobre la tabla {catalog}.{schema}.data_quality_summary por (batch_id, table_name).
    """
    from delta.tables import DeltaTable

    cat = catalog_name if catalog_name is not None else catalog
    target_table_name = f"{cat}.{schema}.data_quality_summary"
    logger.info(
        '{"event": "start_dq_summary_merge", "target": "%s"}',
        target_table_name,
    )

    delta_target = DeltaTable.forName(spark, target_table_name)

    (
        delta_target.alias("target")
        .merge(
            df_dq_summary.alias("source"),
            "target.batch_id = source.batch_id AND target.table_name = source.table_name",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        '{"event": "dq_summary_merge_complete", "target": "%s"}',
        target_table_name,
    )
    return {
        "target_table": target_table_name,
        "status": "SUCCESS",
    }
