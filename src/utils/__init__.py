"""
MÓDULO: src.utils
PROPÓSITO:
Utilidades transversales del Lakehouse Medallion:
- Data Quality Gate y Reglas de Calidad
- Enrutamiento de Cuarentena (Dead-Letter Queue)
- Data Quality Summary (Reconciliación y Observabilidad)
"""

from src.utils.data_quality import (
    ERR_INVALID_DATE,
    ERR_NEGATIVE_AMOUNT,
    ERR_NULL_AMOUNT,
    ERR_NULL_PK,
    ERR_ORPHAN_CUSTOMER,
    apply_customers_dq,
    apply_orders_dq,
    validate_customer_record,
    validate_order_record,
)
from src.utils.quarantine import route_quarantine, route_quarantine_records
from src.utils.dq_summary import (
    compute_dq_summary_df,
    compute_dq_summary_memory,
    merge_dq_summary,
)

from src.utils.assertions import (
    validate_bronze_layer,
    validate_silver_layer,
    validate_gold_layer,
)

__all__ = [
    "ERR_NULL_PK",
    "ERR_NULL_AMOUNT",
    "ERR_NEGATIVE_AMOUNT",
    "ERR_INVALID_DATE",
    "ERR_ORPHAN_CUSTOMER",
    "validate_customer_record",
    "validate_order_record",
    "apply_customers_dq",
    "apply_orders_dq",
    "route_quarantine",
    "route_quarantine_records",
    "compute_dq_summary_df",
    "compute_dq_summary_memory",
    "merge_dq_summary",
    "validate_bronze_layer",
    "validate_silver_layer",
    "validate_gold_layer",
]
