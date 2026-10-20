"""
==============================================================================
MÓDULO: src.gold.customer_ltv
PROPÓSITO:
Cálculo de métricas de valor de vida del cliente (Customer Lifetime Value - LTV):
- Grano analítico único por 'customer_id'.
- Métricas calculadas:
  * first_order_ts: Marca temporal de la primera orden registrada.
  * last_order_ts: Marca temporal de la última transacción registrada.
  * tenure_days: Antigüedad en días entre la primera y última orden.
  * total_orders: Conteo acumulado de compras válidas realizadas.
  * ltv_amount: Suma acumulada de compras (Customer Lifetime Value, DECIMAL(18, 2)).
  * preferred_segment: Segmento preferente o vigente del cliente (de customers_silver).
- Escritura y actualización idempotente vía MERGE INTO Delta Lake por 'customer_id'.
- Cero hints SQL manuales (optimización delegada a Catalyst / AQE).
==============================================================================
"""

import datetime
from decimal import Decimal, ROUND_HALF_UP
import logging
import sys
from typing import Any, Dict, List, Optional

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("gold.customer_ltv")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _parse_ts(val: Any) -> Optional[datetime.datetime]:
    """Parsea una marca temporal en un objeto datetime UTC determinista."""
    if val is None:
        return None
    if isinstance(val, datetime.datetime):
        return val
    if isinstance(val, datetime.date):
        return datetime.datetime(val.year, val.month, val.day)
    s = str(val).strip()
    if not s:
        return None

    # Normalizar separadores y formatos comunes
    cleaned = s.replace("T", " ")
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d",
    ):
        try:
            return datetime.datetime.strptime(cleaned, fmt)
        except ValueError:
            pass
    return None


def compute_customer_ltv_memory(
    orders: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    calculated_at: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Calcula en memoria las métricas de Customer LTV de forma determinista y quirúrgica.
    Útil para tests unitarios y validación estática (SLA < 150ms).

    Parámetros:
        orders: Lista de órdenes conformadas en Capa Silver.
        customers: Lista de clientes en Capa Silver (SCD2).
        calculated_at: Marca temporal UTC de corte opcional.

    Retorna:
        Lista de métricas por customer_id.
    """
    now_iso = (
        calculated_at
        if calculated_at
        else datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )

    # 1. Mapeo de clientes y su segmento actual / preferente
    cust_segment_map: Dict[str, str] = {}
    for c in customers:
        cid = str(c.get("customer_id", "")).strip()
        if not cid:
            continue
        seg = str(c.get("segment", "")).strip() or "STANDARD"
        is_cur = c.get("is_current")
        if is_cur is True or is_cur == 1 or str(is_cur).lower() == "true":
            cust_segment_map[cid] = seg
        elif cid not in cust_segment_map:
            cust_segment_map[cid] = seg

    # 2. Agrupación de órdenes por cliente (excluyendo canceladas y eliminadas)
    orders_by_cust: Dict[str, List[Dict[str, Any]]] = {}
    for ord_row in orders:
        status = str(ord_row.get("order_status", "")).strip().upper()
        tx_type = str(ord_row.get("transaction_type", "")).strip().upper()

        if status in ("CANCELLED", "CANCELED", "DELETED"):
            continue
        if tx_type == "DELETE":
            continue

        cid = str(ord_row.get("customer_id", "")).strip()
        if not cid:
            continue

        if cid not in orders_by_cust:
            orders_by_cust[cid] = []
        orders_by_cust[cid].append(ord_row)

    # 3. Cálculo de métricas por cada cliente con compras
    result: List[Dict[str, Any]] = []

    # Iterar sobre todos los clientes conocidos (o los que tienen órdenes)
    all_cids = sorted(set(list(cust_segment_map.keys()) + list(orders_by_cust.keys())))

    for cid in all_cids:
        c_orders = orders_by_cust.get(cid, [])
        if not c_orders:
            continue

        timestamps: List[datetime.datetime] = []
        total_rev = Decimal("0.00")

        for o in c_orders:
            ts_val = _parse_ts(
                o.get("order_timestamp") or o.get("order_date") or o.get("order_ts")
            )
            if ts_val:
                timestamps.append(ts_val)

            raw_amt = o.get("total_amount")
            if raw_amt is None:
                raw_amt = o.get("amount", 0.0)
            try:
                amt = Decimal(str(raw_amt))
                total_rev += amt
            except Exception:
                pass

        if not timestamps:
            continue

        timestamps.sort()
        first_ts = timestamps[0]
        last_ts = timestamps[-1]
        tenure = (last_ts.date() - first_ts.date()).days
        if tenure < 0:
            tenure = 0

        tot_orders = len(c_orders)
        tot_rev_quant = total_rev.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        pref_segment = cust_segment_map.get(cid, "STANDARD")

        result.append({
            "customer_id": cid,
            "first_order_ts": first_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "last_order_ts": last_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "tenure_days": tenure,
            "total_orders": tot_orders,
            "ltv_amount": float(tot_rev_quant),
            "preferred_segment": pref_segment,
            "_calculated_at": now_iso,
        })

    logger.info(
        '{"event": "compute_customer_ltv_memory_completed", "total_customers_processed": %d}',
        len(result),
    )
    return result


def compute_customer_ltv_df(
    spark: Any,
    df_orders: Any,
    df_customers: Any,
) -> Any:
    """
    Calcula el DataFrame analítico de Customer LTV utilizando transformaciones nativas de PySpark.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, IntegerType

    # Clientes vigentes con su segmento actual
    df_cust_segment = df_customers.filter(
        F.col("is_current") == F.lit(True)
    ).select(
        F.col("customer_id"),
        F.coalesce(F.col("segment"), F.lit("STANDARD")).alias("preferred_segment"),
    )

    cols_ord = df_orders.columns
    status_expr = F.col("status") if "status" in cols_ord else F.col("order_status")
    amount_expr = F.col("amount") if "amount" in cols_ord else F.col("total_amount")
    ts_expr = (
        F.to_timestamp(F.col("order_ts")) if "order_ts" in cols_ord
        else F.coalesce(F.to_timestamp(F.col("order_timestamp")), F.to_timestamp(F.col("order_date")))
    )
    tx_filter = (
        (F.coalesce(F.upper(F.col("transaction_type")), F.lit("")) != F.lit("DELETE"))
        if "transaction_type" in cols_ord
        else F.lit(True)
    )

    # Filtrar órdenes activas no canceladas
    df_ord_valid = df_orders.filter(
        (~F.upper(status_expr).isin("CANCELLED", "CANCELED", "DELETED"))
        & tx_filter
    ).withColumn(
        "ts_calc",
        ts_expr,
    )

    # Agregación por cliente
    df_agg = (
        df_ord_valid.groupBy("customer_id")
        .agg(
            F.min("ts_calc").alias("first_order_ts"),
            F.max("ts_calc").alias("last_order_ts"),
            F.count("order_id").alias("total_orders"),
            F.round(F.sum(amount_expr.cast(DecimalType(18, 2))), 2).alias("ltv_amount"),
        )
        .withColumn(
            "tenure_days",
            F.datediff(
                F.to_date(F.col("last_order_ts")),
                F.to_date(F.col("first_order_ts")),
            ).cast(IntegerType()),
        )
    )

    # Cruce con segmento preferente
    df_result = (
        df_agg.join(
            df_cust_segment,
            on="customer_id",
            how="left",
        )
        .withColumn(
            "preferred_segment",
            F.coalesce(F.col("preferred_segment"), F.lit("STANDARD")),
        )
        .select(
            F.col("customer_id"),
            F.col("first_order_ts"),
            F.col("last_order_ts"),
            F.coalesce(F.col("tenure_days"), F.lit(0)).alias("tenure_days"),
            F.col("total_orders"),
            F.col("ltv_amount"),
            F.col("preferred_segment"),
            F.current_timestamp().alias("_calculated_at"),
        )
    )

    return df_result


def merge_customer_ltv(
    spark: Any,
    df_customer_ltv: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ejecuta la operación atómica e idempotente MERGE INTO en Delta Lake
    sobre la tabla {catalog}.{schema}.customer_ltv por 'customer_id'.
    """
    from delta.tables import DeltaTable

    cat = catalog_name if catalog_name is not None else catalog
    target_table_name = f"{cat}.{schema}.customer_ltv"
    logger.info(
        '{"event": "start_customer_ltv_merge", "target": "%s"}',
        target_table_name,
    )

    delta_target = DeltaTable.forName(spark, target_table_name)

    (
        delta_target.alias("target")
        .merge(
            df_customer_ltv.alias("source"),
            "target.customer_id = source.customer_id",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        '{"event": "customer_ltv_merge_complete", "target": "%s"}',
        target_table_name,
    )
    return {
        "target_table": target_table_name,
        "status": "SUCCESS",
    }
