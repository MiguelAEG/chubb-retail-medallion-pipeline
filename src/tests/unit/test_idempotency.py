"""
==============================================================================
PRUEBA UNITARIA: src/tests/unit/test_idempotency.py
PROPÓSITO:
Verificación quirúrgica, determinista y de alta señal para Idempotencia y Reentrabilidad (Hito 4 / T-05):
1. Bronze Control Log: Descarte determinista de archivos crudos repetidos (0 re-ingestas).
2. Silver Orders SCD1: MERGE atómico re-ejecutado con mismos registros no genera duplicados ni altera el conteo.
3. Silver Customers SCD2: Registros re-ejecutados con idéntico hash de atributos (change_hash) no abren nuevas versiones.
4. Gold Agregaciones: Re-computación de Ventas Diarias y Customer LTV produce resultados idénticos (0 deltas).
==============================================================================
"""

import pytest
from typing import Any, Dict, List

from src.utils.common import filter_pending_files
from src.silver.orders_scd1 import simulate_orders_scd1_merge
from src.silver.customers_scd2 import process_customers_scd2_records
from src.gold.ventas_diarias import compute_ventas_diarias_memory
from src.gold.customer_ltv import compute_customer_ltv_memory


# ==============================================================================
# 1. BRONZE CONTROL LOG IDEMPOTENCY
# ==============================================================================

def test_bronze_control_log_rejects_replayed_files() -> None:
    """Valida que la tabla de control Bronze descarte archivos repetidos al re-ejecutar."""
    all_discovered_files = [
        "/Volumes/landing/customers/customers_initial_part1.csv",
        "/Volumes/landing/customers/customers_initial_part2.csv",
        "/Volumes/landing/orders/orders_incremental_batch1.csv",
    ]

    # Simular primera pasada: no hay nada en el log de auditoría
    first_pass_log = set()
    pending_pass_1 = filter_pending_files(all_discovered_files, first_pass_log)
    assert len(pending_pass_1) == 3

    # Simular que todos los archivos se ingirieron y registraron en bronze_ingestion_log
    second_pass_log = {
        "customers_initial_part1.csv",
        "customers_initial_part2.csv",
        "orders_incremental_batch1.csv",
    }

    # Segunda pasada (Replay idéntico): la lista de pendientes DEBE estar completamente vacía
    pending_pass_2 = filter_pending_files(all_discovered_files, second_pass_log)
    assert len(pending_pass_2) == 0, "Idempotency violation: Bronze attempted to re-ingest processed files"


# ==============================================================================
# 2. SILVER ORDERS SCD1 MERGE IDEMPOTENCY
# ==============================================================================

def test_silver_orders_scd1_merge_idempotency() -> None:
    """Valida que el MERGE SCD1 de órdenes re-ejecutado no genere duplicados ni altere el estado."""
    incoming_batch = [
        {
            "order_id": "ORD-100",
            "customer_id": "CUST-01",
            "order_date": "2026-03-01",
            "order_timestamp": "2026-03-01 10:00:00",
            "order_status": "COMPLETED",
            "total_amount": 150.00,
            "currency": "USD",
            "updated_at": "2026-03-01 10:05:00",
            "transaction_type": "INSERT",
        },
        {
            "order_id": "ORD-200",
            "customer_id": "CUST-02",
            "order_date": "2026-03-01",
            "order_timestamp": "2026-03-01 11:00:00",
            "order_status": "SHIPPED",
            "total_amount": 250.00,
            "currency": "USD",
            "updated_at": "2026-03-01 11:05:00",
            "transaction_type": "INSERT",
        },
    ]

    # Ejecución 1: Tabla destino vacía
    target_table_run1: List[Dict[str, Any]] = []
    res_run1 = simulate_orders_scd1_merge(target_table_run1, incoming_batch)

    assert len(res_run1) == 2
    assert {r["order_id"] for r in res_run1} == {"ORD-100", "ORD-200"}

    # Ejecución 2 (Replay idéntico del mismo lote sobre la tabla resultante)
    res_run2 = simulate_orders_scd1_merge(res_run1, incoming_batch)

    # Invariantes de idempotencia:
    # 1. Conteo de filas idéntico (0 duplicados)
    assert len(res_run2) == 2, "Idempotency violation: SCD1 merge grew table size on replay"
    # 2. Claves primarias únicas
    pks = [r["order_id"] for r in res_run2]
    assert len(pks) == len(set(pks)), "Idempotency violation: duplicate primary keys detected"
    # 3. Contenido idéntico
    assert res_run2 == res_run1


# ==============================================================================
# 3. SILVER CUSTOMERS SCD2 CHANGE DETECTION IDEMPOTENCY
# ==============================================================================

def test_silver_customers_scd2_no_spurious_versions() -> None:
    """Valida que el SCD2 no abra nuevos registros cuando no hay cambios de atributos (change_hash idéntico)."""
    incoming_customers = [
        {
            "customer_id": "CUST-01",
            "first_name": "Ana",
            "last_name": "Gomez",
            "email": "ana.gomez@example.com",
            "phone": "+525512345678",
            "segment": "VIP",
            "address": "Av Reforma 100, CDMX",
            "country": "MX",
            "updated_at": "2026-01-01 10:00:00",
            "transaction_type": "INSERT",
        }
    ]

    # Pasada 1: Creación del registro activo inicial (effective_date = 2026-01-01, is_current = True)
    existing_scd2: List[Dict[str, Any]] = []
    updated_scd2_pass1 = process_customers_scd2_records(existing_scd2, incoming_customers)

    assert len(updated_scd2_pass1) == 1
    assert updated_scd2_pass1[0]["is_current"] is True
    assert updated_scd2_pass1[0]["end_date"] is None
    original_sk = updated_scd2_pass1[0]["customer_sk"]

    # Pasada 2 (Replay): Mismo cliente, mismos atributos, solo re-ejecución del batch
    replay_customers = [
        {
            "customer_id": "CUST-01",
            "first_name": "Ana",
            "last_name": "Gomez",
            "email": "ana.gomez@example.com",
            "phone": "+525512345678",
            "segment": "VIP",
            "address": "Av Reforma 100, CDMX",
            "country": "MX",
            "updated_at": "2026-01-01 10:00:00",
            "transaction_type": "INSERT",
        }
    ]

    updated_scd2_pass2 = process_customers_scd2_records(updated_scd2_pass1, replay_customers)

    # Invariantes de idempotencia SCD2:
    # 1. No debe existir más de 1 versión en la dimensión
    assert len(updated_scd2_pass2) == 1, "Idempotency violation: SCD2 created spurious version on replay"
    # 2. La surrogate key y vigencia permanecen intactas
    assert updated_scd2_pass2[0]["customer_sk"] == original_sk
    assert updated_scd2_pass2[0]["is_current"] is True
    assert updated_scd2_pass2[0]["end_date"] is None


def test_silver_customers_scd2_replay_older_batch_preserves_active() -> None:
    """Valida que al re-ejecutar un lote antiguo (enero), NO se cierre ni invalide el registro activo más reciente (febrero)."""
    # 1. Lote inicial de enero
    jan_batch = [
        {
            "customer_id": "CUST-02",
            "first_name": "Carlos",
            "country": "MX",
            "segment": "RETAIL",
            "email": "carlos@example.com",
            "updated_at": "2026-01-17 10:00:00",
            "transaction_type": "INSERT",
        }
    ]
    dim = process_customers_scd2_records([], jan_batch)
    assert len(dim) == 1
    assert dim[0]["is_current"] is True
    assert dim[0]["segment"] == "RETAIL"

    # 2. Lote incremental de febrero (cambio de segmento a VIP)
    feb_batch = [
        {
            "customer_id": "CUST-02",
            "first_name": "Carlos",
            "country": "MX",
            "segment": "VIP",
            "email": "carlos@example.com",
            "updated_at": "2026-02-07 10:00:00",
            "transaction_type": "UPDATE",
        }
    ]
    dim = process_customers_scd2_records(dim, feb_batch)
    assert len(dim) == 2
    feb_active = [r for r in dim if r["is_current"] is True][0]
    assert feb_active["segment"] == "VIP"
    assert feb_active["effective_date"] == "2026-02-07 10:00:00"

    # 3. Re-ejecución del lote inicial de enero sobre la dimensión ya poblada con febrero
    dim = process_customers_scd2_records(dim, jan_batch)

    # Invariante SCD2: Febrero debe seguir siendo la versión activa vigente
    active_records = [r for r in dim if r["is_current"] is True]
    assert len(active_records) == 1, "SCD2 Invariant violation: active version closed by older batch replay"
    assert active_records[0]["segment"] == "VIP"
    assert active_records[0]["effective_date"] == "2026-02-07 10:00:00"
    assert active_records[0]["end_date"] is None


# ==============================================================================
# 4. GOLD RECOMPUTATION DETERMINISM
# ==============================================================================

def test_gold_aggregations_recomputation_determinism() -> None:
    """Valida que la recomputación de Gold produzca métricas exactamente idénticas."""
    customers = [
        {"customer_id": "C-01", "country": "MX", "is_current": True, "segment": "VIP"},
        {"customer_id": "C-02", "country": "CO", "is_current": True, "segment": "RETAIL"},
    ]

    orders = [
        {
            "order_id": "O-01",
            "customer_id": "C-01",
            "order_date": "2026-03-01",
            "order_timestamp": "2026-03-01 10:00:00",
            "total_amount": 100.0,
            "order_status": "COMPLETED",
            "transaction_type": "INSERT",
        },
        {
            "order_id": "O-02",
            "customer_id": "C-01",
            "order_date": "2026-03-01",
            "order_timestamp": "2026-03-01 12:00:00",
            "total_amount": 200.0,
            "order_status": "COMPLETED",
            "transaction_type": "INSERT",
        },
        {
            "order_id": "O-03",
            "customer_id": "C-02",
            "order_date": "2026-03-02",
            "order_timestamp": "2026-03-02 09:00:00",
            "total_amount": 350.0,
            "order_status": "COMPLETED",
            "transaction_type": "INSERT",
        },
    ]

    calc_timestamp = "2026-03-05 00:00:00"

    # Computación inicial Gold
    sales_run1 = compute_ventas_diarias_memory(orders, customers, calculated_at=calc_timestamp)
    ltv_run1 = compute_customer_ltv_memory(orders, customers, calculated_at=calc_timestamp)

    # Recomputación Gold (Segunda corrida con los mismos insumos)
    sales_run2 = compute_ventas_diarias_memory(orders, customers, calculated_at=calc_timestamp)
    ltv_run2 = compute_customer_ltv_memory(orders, customers, calculated_at=calc_timestamp)

    # Invariantes de determinismo:
    assert sales_run1 == sales_run2, "Idempotency violation: Ventas diarias changed upon recalculation"
    assert ltv_run1 == ltv_run2, "Idempotency violation: Customer LTV changed upon recalculation"
    assert len(sales_run1) == 2
    assert len(ltv_run1) == 2
