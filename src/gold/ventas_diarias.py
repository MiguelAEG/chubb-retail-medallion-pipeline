"""
==============================================================================
MÓDULO: src.gold.ventas_diarias
PROPÓSITO:
Cálculo de métricas agregadas de negocio en Capa Gold por día y país:
- Cruce relacional de órdenes conformadas (Silver) con clientes vigentes (SCD2).
- Filtrado estricto de transacciones canceladas y borradas (Zero Bad Records).
- Métricas calculadas:
  * total_orders: Conteo exacto de órdenes completadas / vigentes.
  * total_revenue: Suma total de importe monetario facturado (DECIMAL(18, 2)).
  * avg_ticket: Ticket promedio monetario por venta (total_revenue / total_orders).
- Escritura y actualización idempotente vía MERGE INTO Delta Lake por (order_date, country).
- Cero hints SQL manuales (optimización delegada a Catalyst / AQE).
==============================================================================
"""

import datetime
from decimal import Decimal, ROUND_HALF_UP
import logging
import sys
from typing import Any, Dict, List, Optional

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("gold.ventas_diarias")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _extract_date_str(val: Any) -> Optional[str]:
    """Extrae la representación YYYY-MM-DD de un valor de fecha o timestamp."""
    if val is None:
        return None
    s = str(val).strip()
    if not s:
        return None
    if "T" in s:
        s = s.split("T")[0]
    elif " " in s:
        s = s.split(" ")[0]
    return s


def compute_ventas_diarias_memory(
    orders: List[Dict[str, Any]],
    customers: List[Dict[str, Any]],
    calculated_at: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Computa la agregación diaria de ventas por país en memoria de forma determinista.
    Útil para pruebas unitarias de alta velocidad (SLA < 150ms) sin clúster JVM.

    Parámetros:
        orders: Lista de registros de órdenes en Capa Silver.
        customers: Lista de registros de clientes en Capa Silver (SCD2).
        calculated_at: Timestamp UTC opcional para reproducibilidad en pruebas.

    Retorna:
        Lista de diccionarios con el resumen (order_date, country, total_orders, total_revenue, avg_ticket).
    """
    now_iso = (
        calculated_at
        if calculated_at
        else datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    )

    # 1. Indexar clientes vigentes o más recientes por customer_id
    customer_country_map: Dict[str, str] = {}
    for c in customers:
        cid = str(c.get("customer_id", "")).strip()
        if not cid:
            continue
        is_cur = c.get("is_current")
        country = str(c.get("country", "")).strip() or "UNKNOWN"

        # Priorizar el registro vigente (is_current = True)
        if is_cur is True or is_cur == 1 or str(is_cur).lower() == "true":
            customer_country_map[cid] = country
        elif cid not in customer_country_map:
            customer_country_map[cid] = country

    # 2. Filtrar órdenes no canceladas / no borradas y agrupar por (order_date, country)
    aggregations: Dict[tuple, Dict[str, Any]] = {}

    for ord_row in orders:
        status = str(ord_row.get("order_status", "")).strip().upper()
        tx_type = str(ord_row.get("transaction_type", "")).strip().upper()

        if status in ("CANCELLED", "CANCELED", "DELETED"):
            continue
        if tx_type == "DELETE":
            continue

        date_val = _extract_date_str(
            ord_row.get("order_date") or ord_row.get("order_timestamp") or ord_row.get("order_ts")
        )
        if not date_val:
            continue

        cid = str(ord_row.get("customer_id", "")).strip()
        country = customer_country_map.get(cid, "UNKNOWN")

        # Parsear monto de la orden
        raw_amt = ord_row.get("total_amount")
        if raw_amt is None:
            raw_amt = ord_row.get("amount", 0.0)
        try:
            amt = Decimal(str(raw_amt))
        except Exception:
            continue

        key = (date_val, country)
        if key not in aggregations:
            aggregations[key] = {
                "order_date": date_val,
                "country": country,
                "total_orders": 0,
                "total_revenue": Decimal("0.00"),
            }

        aggregations[key]["total_orders"] += 1
        aggregations[key]["total_revenue"] += amt

    # 3. Formatear resultados finales con redondeo a 2 decimales
    result: List[Dict[str, Any]] = []
    for (d, cntry), item in sorted(aggregations.items(), key=lambda x: (x[0][0], x[0][1])):
        tot_orders = item["total_orders"]
        tot_rev = item["total_revenue"].quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        if tot_orders > 0:
            avg_t = (item["total_revenue"] / Decimal(tot_orders)).quantize(
                Decimal("0.01"), rounding=ROUND_HALF_UP
            )
        else:
            avg_t = Decimal("0.00")

        result.append({
            "order_date": d,
            "country": cntry,
            "total_orders": tot_orders,
            "total_revenue": float(tot_rev),
            "avg_ticket": float(avg_t),
            "_calculated_at": now_iso,
        })

    logger.info(
        '{"event": "compute_ventas_diarias_memory_completed", "aggregated_groups": %d}',
        len(result),
    )
    return result


def compute_ventas_diarias_df(
    spark: Any,
    df_orders: Any,
    df_customers: Any,
) -> Any:
    """
    Genera el DataFrame analítico de ventas diarias por país a partir de DataFrames PySpark.
    Optimizado para ejecución distribuida en Databricks con Catalyst y AQE.
    """
    from pyspark.sql import functions as F
    from pyspark.sql.types import DecimalType, TimestampType

    # Filtro de clientes activos
    df_cust_active = df_customers.filter(
        F.col("is_current") == F.lit(True)
    ).select(
        F.col("customer_id"),
        F.coalesce(F.col("country"), F.lit("UNKNOWN")).alias("country"),
    )

    cols_ord = df_orders.columns
    status_expr = F.col("status") if "status" in cols_ord else F.col("order_status")
    amount_expr = F.col("amount") if "amount" in cols_ord else F.col("total_amount")
    date_expr = (
        F.to_date(F.col("order_ts")) if "order_ts" in cols_ord
        else F.coalesce(F.to_date(F.col("order_date")), F.to_date(F.col("order_timestamp")))
    )
    tx_filter = (
        (F.coalesce(F.upper(F.col("transaction_type")), F.lit("")) != F.lit("DELETE"))
        if "transaction_type" in cols_ord
        else F.lit(True)
    )

    # Filtro de órdenes válidas (excluyendo canceladas y eliminadas)
    df_ord_valid = df_orders.filter(
        (~F.upper(status_expr).isin("CANCELLED", "CANCELED", "DELETED"))
        & tx_filter
    ).withColumn(
        "order_date_calc",
        date_expr,
    )

    # Cruce relacional sin hints manuales (delegado a AQE / CBO)
    df_joined = df_ord_valid.join(
        df_cust_active,
        on="customer_id",
        how="inner",
    )

    df_aggregated = (
        df_joined.groupBy("order_date_calc", "country")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.round(F.sum(amount_expr.cast(DecimalType(18, 2))), 2).alias("total_revenue"),
        )
        .withColumn(
            "avg_ticket",
            F.when(
                F.col("total_orders") > 0,
                F.round(F.col("total_revenue") / F.col("total_orders"), 2),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            F.col("order_date_calc").alias("order_date"),
            F.col("country"),
            F.col("total_orders"),
            F.col("total_revenue"),
            F.col("avg_ticket"),
            F.current_timestamp().alias("_calculated_at"),
        )
    )

    return df_aggregated


def merge_ventas_diarias(
    spark: Any,
    df_ventas_diarias: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
    df_ventas: Optional[Any] = None,
) -> Dict[str, Any]:
    """
    Ejecuta la operación atómica e idempotente MERGE INTO en Delta Lake
    por la clave compuesta (order_date, country).
    """
    from delta.tables import DeltaTable

    df_source = df_ventas_diarias if df_ventas_diarias is not None else df_ventas
    if df_source is None:
        raise ValueError("Se debe proporcionar 'df_ventas_diarias' o 'df_ventas'")

    cat = catalog_name if catalog_name is not None else catalog
    target_table_name = f"{cat}.{schema}.ventas_diarias_x_pais"
    logger.info(
        '{"event": "start_ventas_diarias_merge", "target": "%s"}',
        target_table_name,
    )

    delta_target = DeltaTable.forName(spark, target_table_name)

    (
        delta_target.alias("target")
        .merge(
            df_source.alias("source"),
            "target.order_date = source.order_date AND target.country = source.country",
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    logger.info(
        '{"event": "ventas_diarias_merge_complete", "target": "%s"}',
        target_table_name,
    )
    return {
        "target_table": target_table_name,
        "status": "SUCCESS",
    }
