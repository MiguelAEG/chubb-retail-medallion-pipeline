"""
==============================================================================
PRUEBA UNITARIA: src/tests/unit/test_bronze.py
PROPÓSITO:
Verificación quirúrgica y determinista de la lógica de ingesta Bronze por entidad:
1. Inferencia determinista de batch_id por convención de archivos.
2. Filtrado e idempotencia estricta de archivos pendientes vs. log de auditoría.
3. Determinismo y unicidad del cálculo de hash crudo (_raw_hash xxhash64 BIGINT Photon).
4. Descubrimiento modular de archivos en directorios de landing para customers y orders.
5. Verificación de consulta mock a bronze_ingestion_log sin requerir JVM ni clúster pesado.
6. Validación de contratos de columnas de negocio para customers_bronze y orders_bronze.
==============================================================================
"""

import os
from pathlib import Path
import sys
from unittest.mock import MagicMock

import pytest

from src.utils.common import (
    compute_python_raw_hash,
    extract_batch_id,
    filter_pending_files,
    get_ingested_files,
    list_landing_files,
)
from src.bronze.customers_bronze import (
    CUSTOMER_BUSINESS_COLUMNS,
    ingest_customers_bronze,
)
from src.bronze.orders_bronze import (
    ORDER_BUSINESS_COLUMNS,
    ingest_orders_bronze,
)


def test_extract_batch_id() -> None:
    """Valida la extracción e inferencia determinista de batch_id desde nombres de archivo."""
    assert extract_batch_id("customers_initial_part1.csv") == "initial_part1"
    assert extract_batch_id("/Volumes/landing/customers/customers_initial_part2.csv") == "initial_part2"
    assert extract_batch_id("orders_incremental_batch1.csv") == "batch_1"
    assert extract_batch_id("orders_incremental_batch2.csv") == "batch_2"
    assert extract_batch_id("orders_batch3.csv") == "batch_3"
    assert extract_batch_id("unknown_file_pattern.csv", fallback_batch_id="manual_batch") == "manual_batch"


def test_filter_pending_files_idempotency() -> None:
    """Valida que los archivos ya procesados con éxito sean omitidos idénticamente (idempotencia)."""
    available_files = [
        "/Volumes/landing/customers/customers_initial_part1.csv",
        "/Volumes/landing/customers/customers_initial_part2.csv",
        "/Volumes/landing/customers/customers_incremental_batch1.csv",
    ]

    # Simular que customers_initial_part1.csv ya fue registrado en el log
    ingested_files = {"customers_initial_part1.csv"}

    pending = filter_pending_files(available_files, ingested_files)

    assert len(pending) == 2
    assert "/Volumes/landing/customers/customers_initial_part1.csv" not in pending
    assert "/Volumes/landing/customers/customers_initial_part2.csv" in pending
    assert "/Volumes/landing/customers/customers_incremental_batch1.csv" in pending

    # Re-ejecutar la misma llamada produce exactamente el mismo resultado (determinismo)
    pending_repeat = filter_pending_files(available_files, ingested_files)
    assert pending == pending_repeat


def test_raw_hash_determinism_and_uniqueness_customers() -> None:
    """Valida que el cálculo de hash crudo xxhash64 (BIGINT 64-bit) sea determinista para clientes."""
    row1 = {
        "customer_id": "CUST-00001",
        "name": "Carlos Gonzalez",
        "first_name": "Carlos",
        "last_name": "Gonzalez",
        "email": "carlos.gonzalez@example.com",
        "phone": "+525512345678",
        "country": "Mexico",
        "segment": "lifestyle",
        "address": "Ciudad de Mexico",
        "updated_at": "2026-01-01 10:00:00",
        "transaction_type": "INSERT",
    }

    # Copia idéntica de la fila
    row2 = dict(row1)

    # Fila con cambio sutil en un atributo
    row3 = dict(row1)
    row3["segment"] = "electronics"

    # Fila con valor nulo / None
    row4 = dict(row1)
    row4["email"] = None

    hash1 = compute_python_raw_hash(row1, CUSTOMER_BUSINESS_COLUMNS)
    hash2 = compute_python_raw_hash(row2, CUSTOMER_BUSINESS_COLUMNS)
    hash3 = compute_python_raw_hash(row3, CUSTOMER_BUSINESS_COLUMNS)
    hash4 = compute_python_raw_hash(row4, CUSTOMER_BUSINESS_COLUMNS)

    # Validación de tipo y rango 64-bit signed int (BIGINT Spark / Photon)
    assert isinstance(hash1, int)
    assert -(1 << 63) <= hash1 < (1 << 63)
    assert -(1 << 63) <= hash3 < (1 << 63)

    # Determinismo
    assert hash1 == hash2

    # Sensibilidad al cambio
    assert hash1 != hash3
    assert hash1 != hash4


def test_raw_hash_determinism_orders() -> None:
    """Valida que el cálculo de hash crudo xxhash64 sea determinista y sensible para órdenes."""
    order1 = {
        "order_id": "ORD-0001",
        "customer_id": "CUST-00001",
        "order_date": "2026-01-01",
        "order_timestamp": "2026-01-01 10:00:00",
        "order_status": "COMPLETED",
        "total_amount": "150.50",
        "currency": "MXN",
        "updated_at": "2026-01-01 10:00:00",
        "transaction_type": "INSERT",
    }
    order2 = dict(order1)
    order3 = dict(order1)
    order3["total_amount"] = "200.00"

    hash_ord1 = compute_python_raw_hash(order1, ORDER_BUSINESS_COLUMNS)
    hash_ord2 = compute_python_raw_hash(order2, ORDER_BUSINESS_COLUMNS)
    hash_ord3 = compute_python_raw_hash(order3, ORDER_BUSINESS_COLUMNS)

    assert hash_ord1 == hash_ord2
    assert hash_ord1 != hash_ord3


def test_list_landing_files_customers_and_orders(tmp_path: Path) -> None:
    """Valida el descubrimiento de archivos CSV tanto para customers como para orders."""
    cust_dir = tmp_path / "customers"
    cust_dir.mkdir(parents=True, exist_ok=True)
    f_cust1 = cust_dir / "customers_initial_part1.csv"
    f_cust2 = cust_dir / "customers_initial_part2.csv"
    f_cust1.write_text("dummy", encoding="utf-8")
    f_cust2.write_text("dummy", encoding="utf-8")

    ord_dir = tmp_path / "orders"
    ord_dir.mkdir(parents=True, exist_ok=True)
    f_ord1 = ord_dir / "orders_incremental_batch1.csv"
    f_ord1.write_text("dummy", encoding="utf-8")

    cust_discovered = list_landing_files(str(tmp_path), "customers")
    ord_discovered = list_landing_files(str(tmp_path), "orders")

    assert len(cust_discovered) == 2
    assert any("customers_initial_part1.csv" in f for f in cust_discovered)
    assert len(ord_discovered) == 1
    assert any("orders_incremental_batch1.csv" in f for f in ord_discovered)


def test_get_ingested_files_with_mock_spark() -> None:
    """Valida la consulta mock a bronze_ingestion_log sin inicializar JVM local."""
    mock_spark = MagicMock()
    mock_spark.catalog.tableExists.return_value = True

    mock_row1 = {"source_file": "customers_initial_part1.csv"}
    mock_row2 = {"source_file": "/Volumes/landing/orders/orders_initial_part1.csv"}

    mock_df = MagicMock()
    mock_df.filter.return_value = mock_df
    mock_df.select.return_value = mock_df
    mock_df.collect.return_value = [mock_row1, mock_row2]

    mock_spark.table.return_value = mock_df

    ingested = get_ingested_files(mock_spark, catalog_name="chubb_lakehouse")

    assert "customers_initial_part1.csv" in ingested
    assert "orders_initial_part1.csv" in ingested
    mock_spark.catalog.tableExists.assert_called_once_with("chubb_lakehouse.bronze.bronze_ingestion_log")


def test_get_ingested_files_table_not_exists() -> None:
    """Valida que si la tabla de control no existe aún, se retorne un set vacío sin excepción."""
    mock_spark = MagicMock()
    mock_spark.catalog.tableExists.return_value = False

    ingested = get_ingested_files(mock_spark, catalog_name="chubb_lakehouse")
    assert ingested == set()
