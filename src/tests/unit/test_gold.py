"""
==============================================================================
PRUEBA UNITARIA: src/tests/unit/test_gold.py
PROPÓSITO:
Verificación quirúrgica, determinista y de alta señal para la Capa Gold (Hito 3 / T-04):
1. Precisión matemática de agregaciones diarias de venta por país (ventas_diarias_x_pais).
   - Exclusión estricta de órdenes canceladas / borradas.
   - Cálculo exacto de total_orders, total_revenue y avg_ticket.
2. Cálculo analítico de valor de vida del cliente (customer_ltv).
   - Verificación de first_order_ts, last_order_ts y tenure_days (antigüedad en días).
   - Acumulación de compras (ltv_amount) y asignación de segmento vigente (preferred_segment).
3. Reconciliación matemática de observabilidad y conservación de masa (data_quality_summary).
   - Invariante: total_received == accepted_rows + quarantined_rows + deduplicated_rows.
   - Desglose estructurado por códigos de error (ERR_NULL_PK, ERR_NEGATIVE_AMOUNT, etc.).
==============================================================================
"""

import json
import pytest

from src.gold.ventas_diarias import compute_ventas_diarias_memory
from src.gold.customer_ltv import compute_customer_ltv_memory
from src.utils.dq_summary import compute_dq_summary_memory


# ==============================================================================
# 1. PRUEBAS DE VENTAS DIARIAS POR PAÍS
# ==============================================================================

def test_ventas_diarias_calculation() -> None:
    """Valida agregación por (fecha, país) y exclusión de cancelados/borrados."""
    customers = [
        {"customer_id": "C-01", "country": "MX", "is_current": True, "segment": "VIP"},
        {"customer_id": "C-02", "country": "CL", "is_current": True, "segment": "RETAIL"},
        {"customer_id": "C-03", "country": "CO", "is_current": True, "segment": "CORPORATE"},
    ]

    orders = [
        # Orden válida C-01 (MX)
        {"order_id": "O-101", "customer_id": "C-01", "order_date": "2026-03-01", "total_amount": 100.50, "order_status": "COMPLETED", "transaction_type": "INSERT"},
        # Otra orden válida C-01 (MX) mismo día
        {"order_id": "O-102", "customer_id": "C-01", "order_date": "2026-03-01", "total_amount": 199.50, "order_status": "COMPLETED", "transaction_type": "INSERT"},
        # Orden cancelada C-01 (debe ignorarse)
        {"order_id": "O-103", "customer_id": "C-01", "order_date": "2026-03-01", "total_amount": 500.00, "order_status": "CANCELLED", "transaction_type": "INSERT"},
        # Orden borrada C-01 (debe ignorarse)
        {"order_id": "O-104", "customer_id": "C-01", "order_date": "2026-03-01", "total_amount": 300.00, "order_status": "COMPLETED", "transaction_type": "DELETE"},
        # Orden válida C-02 (CL) mismo día
        {"order_id": "O-105", "customer_id": "C-02", "order_date": "2026-03-01", "total_amount": 250.00, "order_status": "SHIPPED", "transaction_type": "INSERT"},
        # Orden válida C-03 (CO) día siguiente
        {"order_id": "O-106", "customer_id": "C-03", "order_date": "2026-03-02", "total_amount": 50.00, "order_status": "COMPLETED", "transaction_type": "INSERT"},
    ]

    gold_sales = compute_ventas_diarias_memory(orders, customers, calculated_at="2026-03-03 00:00:00")

    assert len(gold_sales) == 3

    # Grupo 1: 2026-03-01 en CL
    cl_group = next(r for r in gold_sales if r["order_date"] == "2026-03-01" and r["country"] == "CL")
    assert cl_group["total_orders"] == 1
    assert cl_group["total_revenue"] == 250.00
    assert cl_group["avg_ticket"] == 250.00

    # Grupo 2: 2026-03-01 en MX (O-101 + O-102 = 300.00, excluyendo O-103 y O-104)
    mx_group = next(r for r in gold_sales if r["order_date"] == "2026-03-01" and r["country"] == "MX")
    assert mx_group["total_orders"] == 2
    assert mx_group["total_revenue"] == 300.00
    assert mx_group["avg_ticket"] == 150.00  # 300.00 / 2

    # Grupo 3: 2026-03-02 en CO
    co_group = next(r for r in gold_sales if r["order_date"] == "2026-03-02" and r["country"] == "CO")
    assert co_group["total_orders"] == 1
    assert co_group["total_revenue"] == 50.00
    assert co_group["avg_ticket"] == 50.00


# ==============================================================================
# 2. PRUEBAS DE CUSTOMER LIFETIME VALUE (LTV)
# ==============================================================================

def test_customer_ltv_calculation() -> None:
    """Valida métricas de LTV: first_order_ts, last_order_ts, tenure_days y preferred_segment."""
    customers = [
        {"customer_id": "CUST-VIP", "segment": "VIP", "is_current": True},
        {"customer_id": "CUST-NEW", "segment": "STANDARD", "is_current": True},
    ]

    orders = [
        # Compras de CUST-VIP distribuidas en 30 días
        {
            "order_id": "O-1",
            "customer_id": "CUST-VIP",
            "order_timestamp": "2026-01-01 10:00:00",
            "total_amount": 100.00,
            "order_status": "COMPLETED",
        },
        {
            "order_id": "O-2",
            "customer_id": "CUST-VIP",
            "order_timestamp": "2026-01-15 15:30:00",
            "total_amount": 250.50,
            "order_status": "COMPLETED",
        },
        {
            "order_id": "O-3",
            "customer_id": "CUST-VIP",
            "order_timestamp": "2026-01-31 18:00:00",
            "total_amount": 150.00,
            "order_status": "COMPLETED",
        },
        # Compra única de CUST-NEW (tenure = 0)
        {
            "order_id": "O-4",
            "customer_id": "CUST-NEW",
            "order_timestamp": "2026-02-10 12:00:00",
            "total_amount": 80.00,
            "order_status": "COMPLETED",
        },
    ]

    ltv_metrics = compute_customer_ltv_memory(orders, customers, calculated_at="2026-02-15 00:00:00")
    assert len(ltv_metrics) == 2

    # Validar CUST-VIP
    vip_res = next(r for r in ltv_metrics if r["customer_id"] == "CUST-VIP")
    assert vip_res["first_order_ts"] == "2026-01-01 10:00:00"
    assert vip_res["last_order_ts"] == "2026-01-31 18:00:00"
    assert vip_res["tenure_days"] == 30  # 31 de enero - 1 de enero = 30 días
    assert vip_res["total_orders"] == 3
    assert vip_res["ltv_amount"] == 500.50  # 100.00 + 250.50 + 150.00
    assert vip_res["preferred_segment"] == "VIP"

    # Validar CUST-NEW
    new_res = next(r for r in ltv_metrics if r["customer_id"] == "CUST-NEW")
    assert new_res["first_order_ts"] == "2026-02-10 12:00:00"
    assert new_res["last_order_ts"] == "2026-02-10 12:00:00"
    assert new_res["tenure_days"] == 0
    assert new_res["total_orders"] == 1
    assert new_res["ltv_amount"] == 80.00
    assert new_res["preferred_segment"] == "STANDARD"


# ==============================================================================
# 3. PRUEBAS DE OBSERVABILIDAD Y RECONCILIACIÓN MATEMÁTICA DQ
# ==============================================================================

def test_dq_summary_mathematical_reconciliation() -> None:
    """
    Valida la invariante fundamental de conservación de datos:
    total_received == accepted_rows + quarantined_rows + deduplicated_rows
    y la estructuración del desglose por código de error.
    """
    batch_id = "BATCH-2026-Q1"

    bronze_records = [
        {"order_id": "O-01", "_batch_id": batch_id, "amount": 100},
        {"order_id": "O-02", "_batch_id": batch_id, "amount": 200},
        {"order_id": "O-03", "_batch_id": batch_id, "amount": -50},  # Error negativo
        {"order_id": None,   "_batch_id": batch_id, "amount": 300},  # Error PK nula
        {"order_id": "O-01", "_batch_id": batch_id, "amount": 100},  # Duplicado
    ]

    quarantine_records = [
        {"_batch_id": batch_id, "source_entity": "orders", "error_code": "ERR_NEGATIVE_AMOUNT"},
        {"_batch_id": batch_id, "source_entity": "orders", "error_code": "ERR_NULL_PK"},
        {"_batch_id": batch_id, "source_entity": "orders", "error_code": "ERR_NEGATIVE_AMOUNT"},  # Duplicado de error code para probar agregación
    ]

    silver_records = [
        {"order_id": "O-01", "_batch_id": batch_id, "amount": 100},
        {"order_id": "O-02", "_batch_id": batch_id, "amount": 200},
    ]

    summary = compute_dq_summary_memory(
        bronze_records=bronze_records,
        silver_records=silver_records,
        quarantine_records=quarantine_records,
        table_name="orders",
        calculated_at="2026-03-01 12:00:00",
    )

    assert len(summary) == 1
    row = summary[0]

    # 1. Verificación de conteos
    assert row["batch_id"] == batch_id
    assert row["table_name"] == "orders"
    assert row["total_received"] == 5
    assert row["accepted_rows"] == 2
    assert row["quarantined_rows"] == 3
    assert row["deduplicated_rows"] == 0  # 5 recibidos - 3 cuarentena - 2 promovidos = 0 duplicado

    # 2. Verificación de la invariante de conservación de masa
    assert row["total_received"] == (
        row["accepted_rows"] + row["quarantined_rows"] + row["deduplicated_rows"]
    )

    # 3. Verificación de tasa de aprobación (2 / 5 = 40.0%)
    assert row["dq_pass_rate_pct"] == 40.0

    # 4. Verificación del desglose tipificado de errores en JSON
    reasons = json.loads(row["rejection_reasons"])
    assert reasons.get("ERR_NEGATIVE_AMOUNT") == 2
    assert reasons.get("ERR_NULL_PK") == 1

