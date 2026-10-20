"""
==============================================================================
MÓDULO: src.utils.assertions
PROPÓSITO:
Framework de aserciones sobre tablas reales sin duplicar código (DRY Data Quality Assertions).
Ejecuta queries SQL reales contra el catálogo y esquema configurados de Spark y valida
invariantes matemáticos, técnicos y de negocio con 'assert'.
==============================================================================
"""

import logging
import sys
from typing import Any, Dict

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("utils.assertions")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)


def _execute_scalar_sql(spark: Any, query: str) -> Any:
    """Ejecuta una consulta SQL en Spark y retorna el valor escalar de la primera celda."""
    row = spark.sql(query).collect()[0]
    return row[0]


def validate_bronze_layer(spark: Any, catalog: str, schema: str = "chubb_lakehouse") -> Dict[str, Any]:
    """
    Ejecuta aserciones de calidad sobre las tablas de la Capa Bronze:
    1. Verifica que orders_bronze y customers_bronze tengan registros > 0.
    2. Verifica que _raw_hash no sea nulo en ninguna fila (_raw_hash IS NULL == 0).
    3. Verifica que _source_file esté poblado (_source_file IS NULL OR trim(_source_file) = '' == 0).
    4. Verifica que bronze_ingestion_log tenga registros con status='SUCCESS'.
    
    Retorna dict con métricas e imprime: '[PASS] Aserciones de Capa Bronze validadas exitosamente'.
    """
    logger.info(
        '{"event": "start_bronze_assertions", "catalog": "%s", "schema": "%s"}',
        catalog,
        schema,
    )

    # 1. Conteo de registros > 0
    orders_cnt = int(_execute_scalar_sql(spark, f"SELECT count(*) FROM {catalog}.{schema}.orders_bronze"))
    assert orders_cnt > 0, f"Violación de invariante Bronze: orders_bronze está vacía (count={orders_cnt})"

    customers_cnt = int(_execute_scalar_sql(spark, f"SELECT count(*) FROM {catalog}.{schema}.customers_bronze"))
    assert customers_cnt > 0, f"Violación de invariante Bronze: customers_bronze está vacía (count={customers_cnt})"

    # 2. _raw_hash no nulo
    orders_null_raw_hash = int(_execute_scalar_sql(
        spark, f"SELECT count(*) FROM {catalog}.{schema}.orders_bronze WHERE _raw_hash IS NULL"
    ))
    assert orders_null_raw_hash == 0, (
        f"Violación de invariante Bronze: orders_bronze contiene {orders_null_raw_hash} filas con _raw_hash NULL"
    )

    customers_null_raw_hash = int(_execute_scalar_sql(
        spark, f"SELECT count(*) FROM {catalog}.{schema}.customers_bronze WHERE _raw_hash IS NULL"
    ))
    assert customers_null_raw_hash == 0, (
        f"Violación de invariante Bronze: customers_bronze contiene {customers_null_raw_hash} filas con _raw_hash NULL"
    )

    # 3. _source_file poblado
    orders_invalid_source = int(_execute_scalar_sql(
        spark,
        f"SELECT count(*) FROM {catalog}.{schema}.orders_bronze WHERE _source_file IS NULL OR trim(_source_file) = ''"
    ))
    assert orders_invalid_source == 0, (
        f"Violación de invariante Bronze: orders_bronze contiene {orders_invalid_source} filas sin _source_file"
    )

    customers_invalid_source = int(_execute_scalar_sql(
        spark,
        f"SELECT count(*) FROM {catalog}.{schema}.customers_bronze WHERE _source_file IS NULL OR trim(_source_file) = ''"
    ))
    assert customers_invalid_source == 0, (
        f"Violación de invariante Bronze: customers_bronze contiene {customers_invalid_source} filas sin _source_file"
    )

    # 4. bronze_ingestion_log con registros SUCCESS
    success_logs = int(_execute_scalar_sql(
        spark,
        f"SELECT count(*) FROM {catalog}.{schema}.bronze_ingestion_log WHERE status = 'SUCCESS'"
    ))
    assert success_logs > 0, (
        f"Violación de invariante Bronze: bronze_ingestion_log no registra cargas con status='SUCCESS' (count={success_logs})"
    )

    metrics = {
        "orders_count": orders_cnt,
        "customers_count": customers_cnt,
        "orders_null_raw_hash": orders_null_raw_hash,
        "customers_null_raw_hash": customers_null_raw_hash,
        "orders_invalid_source": orders_invalid_source,
        "customers_invalid_source": customers_invalid_source,
        "success_log_entries": success_logs,
        "status": "PASS",
    }

    print("[PASS] Aserciones de Capa Bronze validadas exitosamente")
    return metrics


def validate_silver_layer(spark: Any, catalog: str, schema: str = "chubb_lakehouse") -> Dict[str, Any]:
    """
    Ejecuta aserciones de calidad sobre las tablas de la Capa Silver y Cuarentena:
    1. Verifica unicidad en orders_silver (SCD1): GROUP BY order_id HAVING count(*) > 1 debe ser 0.
    2. Verifica que en orders_silver los montos válidos sean positivos (amount > 0).
    3. Verifica unicidad de vigencia activa en customers_silver (SCD2):
       WHERE is_current = true GROUP BY customer_id HAVING count(*) > 1 debe ser 0.
    4. Verifica que orders_quarantine y customers_quarantine contengan registros aislados con error_code IS NOT NULL.
    
    Retorna dict e imprime: '[PASS] Aserciones de Capa Silver validadas exitosamente (SCD1, SCD2 y Cuarentena)'.
    """
    logger.info(
        '{"event": "start_silver_assertions", "catalog": "%s", "schema": "%s"}',
        catalog,
        schema,
    )

    # 1. Unicidad de order_id en orders_silver (SCD1)
    orders_dup_count = int(_execute_scalar_sql(
        spark,
        f"""
        SELECT count(*) FROM (
            SELECT order_id FROM {catalog}.{schema}.orders_silver 
            GROUP BY order_id HAVING count(*) > 1
        )
        """
    ))
    assert orders_dup_count == 0, (
        f"Violación de invariante Silver: orders_silver contiene {orders_dup_count} order_ids duplicados"
    )

    # 2. Montos positivos en orders_silver (total_amount > 0)
    # Compatible tanto si la columna se llama total_amount o amount
    amount_col = "total_amount"
    try:
        spark.sql(f"SELECT total_amount FROM {catalog}.{schema}.orders_silver LIMIT 1")
    except Exception:
        amount_col = "amount"

    non_positive_amounts = int(_execute_scalar_sql(
        spark,
        f"SELECT count(*) FROM {catalog}.{schema}.orders_silver WHERE {amount_col} <= 0 OR {amount_col} IS NULL"
    ))
    assert non_positive_amounts == 0, (
        f"Violación de invariante Silver: orders_silver contiene {non_positive_amounts} órdenes con monto no positivo"
    )

    # 3. Unicidad de vigencia activa en customers_silver (SCD2)
    cust_active_dups = int(_execute_scalar_sql(
        spark,
        f"""
        SELECT count(*) FROM (
            SELECT customer_id FROM {catalog}.{schema}.customers_silver 
            WHERE is_current = true 
            GROUP BY customer_id HAVING count(*) > 1
        )
        """
    ))
    assert cust_active_dups == 0, (
        f"Violación de invariante Silver: customers_silver contiene {cust_active_dups} clientes con múltiples registros vigentes activos (is_current=true)"
    )

    # 4. Registros en cuarentena aislados con error_code IS NOT NULL
    # Validar orders_quarantine
    orders_quarantine_cnt = int(_execute_scalar_sql(
        spark, f"SELECT count(*) FROM {catalog}.{schema}.orders_quarantine"
    ))
    assert orders_quarantine_cnt > 0, "Violación de invariante Cuarentena: orders_quarantine está vacía"

    orders_quarantine_null_code = int(_execute_scalar_sql(
        spark,
        f"SELECT count(*) FROM {catalog}.{schema}.orders_quarantine WHERE error_code IS NULL OR trim(error_code) = ''"
    ))
    assert orders_quarantine_null_code == 0, (
        f"Violación de invariante Cuarentena: orders_quarantine tiene {orders_quarantine_null_code} filas sin error_code"
    )

    # Validar customers_quarantine
    customers_quarantine_cnt = int(_execute_scalar_sql(
        spark, f"SELECT count(*) FROM {catalog}.{schema}.customers_quarantine"
    ))
    assert customers_quarantine_cnt > 0, "Violación de invariante Cuarentena: customers_quarantine está vacía"

    customers_quarantine_null_code = int(_execute_scalar_sql(
        spark,
        f"SELECT count(*) FROM {catalog}.{schema}.customers_quarantine WHERE error_code IS NULL OR trim(error_code) = ''"
    ))
    assert customers_quarantine_null_code == 0, (
        f"Violación de invariante Cuarentena: customers_quarantine tiene {customers_quarantine_null_code} filas sin error_code"
    )

    metrics = {
        "orders_duplicate_pks": orders_dup_count,
        "orders_non_positive_amount": non_positive_amounts,
        "customers_active_duplicates": cust_active_dups,
        "orders_quarantined_total": orders_quarantine_cnt,
        "customers_quarantined_total": customers_quarantine_cnt,
        "status": "PASS",
    }

    print("[PASS] Aserciones de Capa Silver validadas exitosamente (SCD1, SCD2 y Cuarentena)")
    return metrics


def validate_gold_layer(spark: Any, catalog: str, schema: str = "chubb_lakehouse") -> Dict[str, Any]:
    """
    Ejecuta aserciones de calidad sobre las tablas de la Capa Gold:
    1. Verifica que ventas_diarias_x_pais tenga registros, total_revenue > 0 y order_count > 0 (total_orders > 0).
    2. Verifica reconciliación financiera: la suma de revenue en Gold es consistente con las órdenes activas en Silver.
    3. Verifica que customer_ltv tenga tenure_days >= 0 y ltv_amount >= 0.
    4. Verifica reconciliación de balance en data_quality_summary:
       total_received = accepted_rows + quarantined_rows + deduplicated_rows.
    
    Retorna dict e imprime: '[PASS] Aserciones de Capa Gold validadas exitosamente'.
    """
    logger.info(
        '{"event": "start_gold_assertions", "catalog": "%s", "schema": "%s"}',
        catalog,
        schema,
    )

    # 1. ventas_diarias_x_pais: registros > 0, total_revenue > 0, order_count > 0 (o total_orders > 0)
    sales_cnt = int(_execute_scalar_sql(spark, f"SELECT count(*) FROM {catalog}.{schema}.ventas_diarias_x_pais"))
    assert sales_cnt > 0, f"Violación de invariante Gold: ventas_diarias_x_pais está vacía (count={sales_cnt})"

    # Detectar nombre de columna de conteo de órdenes (total_orders o order_count)
    order_cnt_col = "total_orders"
    try:
        spark.sql(f"SELECT total_orders FROM {catalog}.{schema}.ventas_diarias_x_pais LIMIT 1")
    except Exception:
        order_cnt_col = "order_count"

    invalid_sales_rows = int(_execute_scalar_sql(
        spark,
        f"""
        SELECT count(*) FROM {catalog}.{schema}.ventas_diarias_x_pais 
        WHERE total_revenue <= 0 OR total_revenue IS NULL OR {order_cnt_col} <= 0 OR {order_cnt_col} IS NULL
        """
    ))
    assert invalid_sales_rows == 0, (
        f"Violación de invariante Gold: ventas_diarias_x_pais contiene {invalid_sales_rows} filas con métricas <= 0"
    )

    # 2. Reconciliación financiera Gold vs Silver
    # En Silver las órdenes activas/completadas que participan en ventas
    # Resolver nombre de columna de monto en Silver (amount o total_amount)
    amount_col = "total_amount"
    try:
        spark.sql(f"SELECT total_amount FROM {catalog}.{schema}.orders_silver LIMIT 1")
    except Exception:
        amount_col = "amount"

    # Resolver nombre de columna de estado en Silver (status o order_status)
    status_col = "status"
    try:
        spark.sql(f"SELECT status FROM {catalog}.{schema}.orders_silver LIMIT 1")
    except Exception:
        status_col = "order_status"

    # Detectar si existe transaction_type en orders_silver
    has_tx_type = False
    try:
        spark.sql(f"SELECT transaction_type FROM {catalog}.{schema}.orders_silver LIMIT 1")
        has_tx_type = True
    except Exception:
        has_tx_type = False

    gold_total_revenue = float(_execute_scalar_sql(
        spark, f"SELECT coalesce(sum(total_revenue), 0.0) FROM {catalog}.{schema}.ventas_diarias_x_pais"
    ))
    assert gold_total_revenue > 0, (
        f"Violación de invariante Gold: la suma de revenue en Gold debe ser positiva ({gold_total_revenue})"
    )

    # Órdenes activas en Silver (excluyendo CANCELLED o DELETE) asociadas a clientes vigentes
    tx_filter = f"AND upper(coalesce(o.transaction_type, '')) NOT IN ('DELETE')" if has_tx_type else ""
    silver_active_revenue = float(_execute_scalar_sql(
        spark,
        f"""
        SELECT coalesce(sum(o.{amount_col}), 0.0)
        FROM {catalog}.{schema}.orders_silver o
        JOIN {catalog}.{schema}.customers_silver c ON o.customer_id = c.customer_id AND c.is_current = true
        WHERE upper(coalesce(o.{status_col}, '')) NOT IN ('CANCELLED', 'CANCELED')
          {tx_filter}
        """
    ))

    # Margen de tolerancia de centavos para rounding aritmético
    revenue_diff = abs(gold_total_revenue - silver_active_revenue)
    assert revenue_diff < 0.05, (
        f"Violación de reconciliación financiera: Gold revenue ({gold_total_revenue}) "
        f"difiere de Silver active revenue ({silver_active_revenue}) por {revenue_diff}"
    )

    # 3. customer_ltv: tenure_days >= 0 y ltv_amount >= 0
    ltv_cnt = int(_execute_scalar_sql(spark, f"SELECT count(*) FROM {catalog}.{schema}.customer_ltv"))
    assert ltv_cnt > 0, f"Violación de invariante Gold: customer_ltv está vacía (count={ltv_cnt})"

    invalid_ltv_rows = int(_execute_scalar_sql(
        spark,
        f"""
        SELECT count(*) FROM {catalog}.{schema}.customer_ltv 
        WHERE tenure_days < 0 OR tenure_days IS NULL OR ltv_amount < 0 OR ltv_amount IS NULL
        """
    ))
    assert invalid_ltv_rows == 0, (
        f"Violación de invariante Gold: customer_ltv contiene {invalid_ltv_rows} filas con tenure_days < 0 o ltv_amount < 0"
    )

    # 4. Reconciliación de balance en data_quality_summary: total_received = accepted_rows + quarantined_rows + deduplicated_rows
    dq_summary_cnt = int(_execute_scalar_sql(spark, f"SELECT count(*) FROM {catalog}.{schema}.data_quality_summary"))
    assert dq_summary_cnt > 0, f"Violación de invariante Gold: data_quality_summary está vacía (count={dq_summary_cnt})"

    dq_unbalanced_rows = int(_execute_scalar_sql(
        spark,
        f"""
        SELECT count(*) FROM {catalog}.{schema}.data_quality_summary 
        WHERE total_received != (accepted_rows + quarantined_rows + deduplicated_rows)
        """
    ))
    assert dq_unbalanced_rows == 0, (
        f"Violación de balance DQ: data_quality_summary contiene {dq_unbalanced_rows} lotes donde "
        "total_received != accepted_rows + quarantined_rows + deduplicated_rows"
    )

    metrics = {
        "ventas_diarias_rows": sales_cnt,
        "gold_total_revenue": gold_total_revenue,
        "silver_active_revenue": silver_active_revenue,
        "revenue_reconciliation_diff": revenue_diff,
        "customer_ltv_rows": ltv_cnt,
        "dq_summary_rows": dq_summary_cnt,
        "dq_unbalanced_rows": dq_unbalanced_rows,
        "status": "PASS",
    }

    print("[PASS] Aserciones de Capa Gold validadas exitosamente")
    return metrics
