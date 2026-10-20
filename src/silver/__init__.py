"""
MÓDULO: src.silver
PROPÓSITO:
Capa Silver del Lakehouse Medallion:
- SCD Tipo 1 para Órdenes Transaccionales
- SCD Tipo 2 para Dimensión de Clientes con Surrogate Keys xxhash64 y Enmascaramiento PII
"""

from src.silver.orders_scd1 import deduplicate_orders, merge_orders_scd1, simulate_orders_scd1_merge
from src.silver.customers_scd2 import (
    compute_customer_sk_py,
    merge_customers_scd2,
    process_customers_scd2_records,
)

__all__ = [
    "deduplicate_orders",
    "merge_orders_scd1",
    "simulate_orders_scd1_merge",
    "compute_customer_sk_py",
    "merge_customers_scd2",
    "process_customers_scd2_records",
]
