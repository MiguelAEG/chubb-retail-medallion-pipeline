"""
Capa Bronze - Medallion Lakehouse (Prueba Tecnica Chubb 2026).
Modulos desacoplados de ingesta cruda append-only, linaje, auditoria e idempotencia:
- customers_bronze: Ingesta fidedigna de clientes
- orders_bronze: Ingesta fidedigna de ordenes
- common: Utilidades compartidas de linaje, hashing xxhash64 y auditoria
"""

from src.utils.common import (
    compute_python_raw_hash,
    compute_raw_hash_col,
    extract_batch_id,
    filter_pending_files,
    get_ingested_files,
    list_landing_files,
    record_ingestion_log,
)
from src.bronze.customers_bronze import (
    CUSTOMER_BUSINESS_COLUMNS,
    ingest_customer_file,
    ingest_customers_bronze,
)
from src.bronze.orders_bronze import (
    ORDER_BUSINESS_COLUMNS,
    ingest_order_file,
    ingest_orders_bronze,
)

__all__ = [
    "compute_python_raw_hash",
    "compute_raw_hash_col",
    "extract_batch_id",
    "filter_pending_files",
    "get_ingested_files",
    "list_landing_files",
    "record_ingestion_log",
    "CUSTOMER_BUSINESS_COLUMNS",
    "ingest_customer_file",
    "ingest_customers_bronze",
    "ORDER_BUSINESS_COLUMNS",
    "ingest_order_file",
    "ingest_orders_bronze",
]
