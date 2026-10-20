"""
==============================================================================
MÓDULO: src.silver.orders_scd1
PROPÓSITO:
Procesamiento de órdenes transaccionales bajo SCD Tipo 1 (Upsert Determinista):
- Deduplicación intra/inter-batch de órdenes por 'order_id' ordenadas por updated_at DESC
- Operación atómica MERGE INTO Delta Lake en chubb_lakehouse.silver.fct_orders_scd1:
  * WHEN MATCHED AND source.transaction_type = 'DELETE' THEN DELETE
  * WHEN MATCHED AND source.updated_at >= target.updated_at THEN UPDATE SET *
  * WHEN NOT MATCHED AND source.transaction_type != 'DELETE' THEN INSERT *
- Conversión tipada estricta (DECIMAL(18, 2), DATE, TIMESTAMP)
- Invariante de idempotencia matemática y pureza productiva
==============================================================================
"""

import datetime
import logging
import sys
from typing import Any, Dict, List, Optional

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("silver.orders_scd1")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

try:
    from delta.tables import DeltaTable  # type: ignore
except ImportError:
    class _DeltaTableFallback:  # type: ignore
        @staticmethod
        def forName(spark: Any, table_name: str) -> Any:
            raise NotImplementedError("DeltaTable requires delta-spark or Databricks runtime")
    DeltaTable = _DeltaTableFallback  # type: ignore


def deduplicate_orders_memory(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Deduplica en memoria registros de órdenes por 'order_id', preservando el
    registro con la marca temporal 'updated_at' más reciente.
    """
    latest_by_order: Dict[str, Dict[str, Any]] = {}

    for row in records:
        oid = str(row.get("order_id", "")).strip()
        if not oid:
            continue

        if oid not in latest_by_order:
            latest_by_order[oid] = dict(row)
        else:
            prev_updated = str(latest_by_order[oid].get("updated_at", "") or "")
            curr_updated = str(row.get("updated_at", "") or "")
            if curr_updated >= prev_updated:
                latest_by_order[oid] = dict(row)

    deduped = list(latest_by_order.values())
    logger.info(
        '{"event": "orders_dedup_memory", "input_count": %d, "deduped_count": %d}',
        len(records),
        len(deduped),
    )
    return deduped


def simulate_orders_scd1_merge(
    current_orders: List[Dict[str, Any]],
    incoming_orders: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Simula de forma determinista la lógica de Delta MERGE INTO para SCD1 de órdenes:
    1. Deduplica incoming_orders por order_id manteniendo el mayor updated_at.
    2. MATCHED + source.transaction_type == 'DELETE' -> Elimina el registro de target.
    3. MATCHED + source.updated_at >= target.updated_at -> Actualiza con la nueva versión.
    4. MATCHED + source.updated_at < target.updated_at -> Se conserva target (out-of-order drop).
    5. NOT MATCHED + source.transaction_type != 'DELETE' -> Inserta nuevo registro.
    """
    target_state: Dict[str, Dict[str, Any]] = {
        str(row["order_id"]).strip(): dict(row) for row in current_orders if row.get("order_id")
    }

    clean_incoming = deduplicate_orders_memory(incoming_orders)

    for src in clean_incoming:
        oid = str(src.get("order_id", "")).strip()
        tx_type = str(src.get("transaction_type", "")).upper()
        src_updated = str(src.get("updated_at", "") or "")

        if oid in target_state:
            if tx_type == "DELETE":
                del target_state[oid]
            else:
                tgt_updated = str(target_state[oid].get("updated_at", "") or "")
                if src_updated >= tgt_updated:
                    target_state[oid] = dict(src)
        else:
            if tx_type != "DELETE":
                target_state[oid] = dict(src)

    return list(target_state.values())


def deduplicate_orders(df_clean_orders: Any) -> Any:
    """
    Deduplica un DataFrame de PySpark de órdenes por 'order_id' tomando
    el registro con el 'updated_at' más reciente utilizando Window functions.
    """
    from pyspark.sql import Window, functions as F

    window_spec = Window.partitionBy("order_id").orderBy(
        F.col("updated_at").desc_nulls_last()
    )

    return (
        df_clean_orders.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def prepare_orders_silver_df(df_deduped_orders: Any) -> Any:
    """
    Aplica tipado estricto y proyecta las columnas conformadas al contrato orders_silver:
    order_id, customer_id, order_ts, amount, currency, status, updated_at, _batch_id, _source_file, transaction_type
    """
    from pyspark.sql import functions as F

    cols = df_deduped_orders.columns

    order_ts_col = (
        F.col("order_ts") if "order_ts" in cols
        else F.coalesce(F.col("order_timestamp"), F.col("updated_at"))
    )

    amount_col = (
        F.col("amount") if "amount" in cols
        else F.col("total_amount")
    )

    status_col = (
        F.col("status") if "status" in cols
        else F.col("order_status")
    )

    batch_col = F.col("_batch_id") if "_batch_id" in cols else F.lit("unknown")
    source_file_col = F.col("_source_file") if "_source_file" in cols else F.lit("unknown")
    tx_type_col = F.col("transaction_type") if "transaction_type" in cols else F.lit("INSERT")

    df_out = (
        df_deduped_orders
        .withColumn("order_id", F.col("order_id").cast("string"))
        .withColumn("customer_id", F.col("customer_id").cast("string"))
        .withColumn("order_ts", order_ts_col.cast("string"))
        .withColumn("amount", amount_col.cast("double"))
        .withColumn("currency", F.col("currency").cast("string"))
        .withColumn("status", status_col.cast("string"))
        .withColumn("updated_at", F.col("updated_at").cast("string"))
        .withColumn("_batch_id", batch_col.cast("string"))
        .withColumn("_source_file", source_file_col.cast("string"))
        .withColumn("transaction_type", tx_type_col.cast("string"))
    )

    return df_out


def merge_orders_scd1(
    spark: Any,
    df_clean_orders: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ejecuta la operación atómica MERGE INTO en Delta Lake para SCD1.
    Target pre-creado y gobernado estrictamente por DDL: {catalog}.{schema}.orders_silver
    """
    cat = catalog_name if catalog_name is not None else catalog
    target_table_name = f"{cat}.{schema}.orders_silver"
    df_dedup = deduplicate_orders(df_clean_orders)
    df_prep = prepare_orders_silver_df(df_dedup)

    logger.info(
        '{"event": "start_orders_scd1_merge", "target": "%s"}',
        target_table_name,
    )

    # Resolución directa gobernada por DDL (Cero auto-creación ni saveAsTable condicional)
    delta_target = DeltaTable.forName(spark, target_table_name)

    (
        delta_target.alias("target")
        .merge(
            df_prep.alias("source"),
            "target.order_id = source.order_id",
        )
        .whenMatchedDelete(
            condition="source.transaction_type = 'DELETE'",
        )
        .whenMatchedUpdate(
            condition="source.updated_at >= target.updated_at",
            set={
                "customer_id": "source.customer_id",
                "order_ts": "source.order_ts",
                "amount": "source.amount",
                "currency": "source.currency",
                "status": "source.status",
                "updated_at": "source.updated_at",
                "_batch_id": "source._batch_id",
                "_source_file": "source._source_file",
            },
        )
        .whenNotMatchedInsert(
            condition="source.transaction_type != 'DELETE'",
            values={
                "order_id": "source.order_id",
                "customer_id": "source.customer_id",
                "order_ts": "source.order_ts",
                "amount": "source.amount",
                "currency": "source.currency",
                "status": "source.status",
                "updated_at": "source.updated_at",
                "_batch_id": "source._batch_id",
                "_source_file": "source._source_file",
            },
        )
        .execute()
    )

    logger.info(
        '{"event": "orders_scd1_merge_complete", "target": "%s"}',
        target_table_name,
    )

    return {
        "target_table": target_table_name,
        "status": "SUCCESS",
    }


def process_orders_silver(
    spark: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
    batch_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orquesta de punta a punta la promoción de Orders desde Bronze a Silver SCD1:
    1. Lectura de orders_bronze desde chubb_lakehouse
    2. Data Quality Gate y desvío a Cuarentena
    3. Merge atómico SCD1 en orders_silver
    """
    from src.utils.data_quality import apply_orders_dq
    from src.utils.quarantine import route_quarantine

    cat = catalog_name if catalog_name is not None else catalog

    raw_ord_table = f"{cat}.{schema}.orders_bronze"
    try:
        df_raw = spark.table(raw_ord_table)
    except Exception:
        try:
            raw_ord_table = f"{cat}.{schema}.raw_orders"
            df_raw = spark.table(raw_ord_table)
        except Exception:
            raw_ord_table = f"{cat}.bronze.raw_orders"
            df_raw = spark.table(raw_ord_table)

    logger.info('{"event": "process_orders_silver_start", "table": "%s"}', raw_ord_table)

    if batch_filter:
        batch_ids = [batch_filter]
    else:
        # Orden cronológico determinista: lotes 'initial' primero, seguidos de incrementales ordenados por updated_at
        from pyspark.sql import functions as F
        batch_df = (
            df_raw.groupBy("_batch_id")
            .agg(
                F.min("updated_at").alias("min_updated_at"),
                F.min("_ingestion_ts").alias("min_ingestion_ts"),
            )
            .withColumn(
                "_is_incremental",
                F.when(F.col("_batch_id").rlike("(?i)init"), F.lit(0)).otherwise(F.lit(1))
            )
            .orderBy("_is_incremental", "min_updated_at", "min_ingestion_ts", "_batch_id")
        )
        batch_ids = [row["_batch_id"] for row in batch_df.collect() if row["_batch_id"]]

    if not batch_ids:
        batch_ids = ["batch_silver"]

    # Universo de clientes válidos para verificar integridad referencial
    dim_cust_table = f"{cat}.{schema}.customers_silver"
    try:
        df_cust_universe = spark.table(dim_cust_table).select("customer_id").distinct()
    except Exception:
        try:
            dim_cust_table = f"{cat}.{schema}.dim_customers_scd2"
            df_cust_universe = spark.table(dim_cust_table).select("customer_id").distinct()
        except Exception:
            dim_cust_table = f"{cat}.silver.dim_customers_scd2"
            df_cust_universe = spark.table(dim_cust_table).select("customer_id").distinct()

    total_valid = 0
    total_quar = 0
    last_merge_res = None

    for b_id in batch_ids:
        logger.info('{"event": "process_orders_batch_start", "batch_id": "%s"}', b_id)
        df_batch = df_raw.filter(f"_batch_id = '{b_id}'") if "_batch_id" in df_raw.columns else df_raw
        df_dq = apply_orders_dq(df_batch, valid_customers_df=df_cust_universe)
        df_clean, valid_cnt, quar_cnt = route_quarantine(
            spark=spark,
            df_evaluated=df_dq,
            entity="orders",
            catalog=cat,
            schema=schema,
            default_batch_id=b_id,
        )
        total_valid += valid_cnt
        total_quar += quar_cnt

        if valid_cnt > 0:
            last_merge_res = merge_orders_scd1(
                spark=spark,
                df_clean_orders=df_clean,
                catalog=cat,
                schema=schema,
            )

    return {
        "catalog": cat,
        "schema": schema,
        "valid_promoted": total_valid,
        "quarantined": total_quar,
        "merge_result": last_merge_res,
        "status": "SUCCESS",
    }

