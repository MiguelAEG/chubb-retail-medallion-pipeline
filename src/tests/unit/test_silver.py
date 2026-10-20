"""
==============================================================================
PRUEBA UNITARIA: src/tests/unit/test_silver.py
PROPÓSITO:
Verificación quirúrgica, determinista y de alta señal para la Capa Silver (Hito 2 / T-03):
1. Validación de reglas de calidad y clasificación de fallos DQ (ERR_*).
2. Desvío y enriquecimiento de metadatos hacia Cuarentena (Dead-Letter Queue).
3. Deduplicación y lógica de merge SCD Tipo 1 en órdenes (INSERT, UPDATE, DELETE).
4. Apertura y cierre de vigencias en SCD Tipo 2 de clientes con surrogate key xxhash64.
5. Anonimización y enmascaramiento PII estricto sin dependencias de clúster JVM pesado.
==============================================================================
"""

import pytest

from src.utils.data_quality import (
    ERR_INVALID_DATE,
    ERR_NEGATIVE_AMOUNT,
    ERR_NULL_AMOUNT,
    ERR_NULL_PK,
    ERR_ORPHAN_CUSTOMER,
    validate_customer_record,
    validate_order_record,
)
from src.utils.quarantine import route_quarantine_records
from src.silver.orders_scd1 import deduplicate_orders_memory, simulate_orders_scd1_merge
from src.silver.customers_scd2 import (
    compute_change_hash_py,
    compute_customer_sk_py,
    mask_pii_py,
    process_customers_scd2_records,
)


# ==============================================================================
# 1. PRUEBAS DE DATA QUALITY GATE Y REGLAS DE NEGOCIO
# ==============================================================================

def test_customers_data_quality_rules() -> None:
    """Valida la detección de nulos en PK y fechas inválidas en clientes."""
    valid_customer = {
        "customer_id": "CUST-001",
        "first_name": "Ana",
        "last_name": "Lopez",
        "email": "ana.lopez@example.com",
        "segment": "VIP",
        "updated_at": "2026-01-15 10:30:00",
    }
    is_valid, err, reason = validate_customer_record(valid_customer)
    assert is_valid is True
    assert err is None

    # Caso 1: PK Nula o vacía
    null_pk_cust = dict(valid_customer, customer_id="  ")
    is_valid, err, _ = validate_customer_record(null_pk_cust)
    assert is_valid is False
    assert err == ERR_NULL_PK

    # Caso 2: Fecha / timestamp con formato inválido
    invalid_date_cust = dict(valid_customer, updated_at="15/01/2026_not_iso")
    is_valid, err, _ = validate_customer_record(invalid_date_cust)
    assert is_valid is False
    assert err == ERR_INVALID_DATE


def test_orders_data_quality_rules_and_referential_integrity() -> None:
    """Valida las reglas de órdenes: PKs, montos nulos, negativos e integridad referencial."""
    valid_order = {
        "order_id": "ORD-1001",
        "customer_id": "CUST-001",
        "order_date": "2026-01-15",
        "order_timestamp": "2026-01-15 10:30:00",
        "order_status": "COMPLETED",
        "total_amount": "150.50",
        "currency": "USD",
        "updated_at": "2026-01-15 10:30:00",
    }
    valid_customers = {"CUST-001", "CUST-002"}

    # Caso conforme
    is_valid, err, _ = validate_order_record(valid_order, valid_customer_ids=valid_customers)
    assert is_valid is True
    assert err is None

    # Caso 1: PK Nula
    null_pk_order = dict(valid_order, order_id=None)
    is_valid, err, _ = validate_order_record(null_pk_order, valid_customer_ids=valid_customers)
    assert is_valid is False
    assert err == ERR_NULL_PK

    # Caso 2: Monto Nulo o no numérico
    null_amt_order = dict(valid_order, total_amount="")
    is_valid, err, _ = validate_order_record(null_amt_order, valid_customer_ids=valid_customers)
    assert is_valid is False
    assert err == ERR_NULL_AMOUNT

    # Caso 3: Monto Negativo o Cero
    neg_amt_order = dict(valid_order, total_amount="-25.00")
    is_valid, err, _ = validate_order_record(neg_amt_order, valid_customer_ids=valid_customers)
    assert is_valid is False
    assert err == ERR_NEGATIVE_AMOUNT

    # Caso 4: Cliente Huérfano (Fallo de Integridad Referencial)
    orphan_order = dict(valid_order, customer_id="CUST-999_ORPHAN")
    is_valid, err, _ = validate_order_record(orphan_order, valid_customer_ids=valid_customers)
    assert is_valid is False
    assert err == ERR_ORPHAN_CUSTOMER


# ==============================================================================
# 2. PRUEBAS DE ENRUTAMIENTO A CUARENTENA (DEAD-LETTER QUEUE)
# ==============================================================================

def test_quarantine_routing_and_metadata_enrichment() -> None:
    """Valida que los registros que fallan DQ se desvíen a cuarentena con metadatos completos."""
    records = [
        {"order_id": "ORD-01", "customer_id": "C-1", "total_amount": "100.0", "updated_at": "2026-01-01"},
        {"order_id": "ORD-02", "customer_id": "C-1", "total_amount": "-50.0", "updated_at": "2026-01-01"},
        {"order_id": "ORD-03", "customer_id": "C-99", "total_amount": "200.0", "updated_at": "2026-01-01"},
    ]
    customer_universe = {"C-1"}

    clean, quarantined = route_quarantine_records(
        records=records,
        entity="orders",
        valid_customer_ids=customer_universe,
        batch_id="batch_test_01",
        source_file="orders_test.csv",
    )

    assert len(clean) == 1
    assert clean[0]["order_id"] == "ORD-01"

    assert len(quarantined) == 2
    # ORD-02 falló por monto negativo
    q_ord2 = next(r for r in quarantined if r["order_id"] == "ORD-02")
    assert q_ord2["error_code"] == ERR_NEGATIVE_AMOUNT
    assert q_ord2["_batch_id"] == "batch_test_01"
    assert q_ord2["_source_file"] == "orders_test.csv"
    assert "rejected_at" in q_ord2

    # ORD-03 falló por cliente huérfano
    q_ord3 = next(r for r in quarantined if r["order_id"] == "ORD-03")
    assert q_ord3["error_code"] == ERR_ORPHAN_CUSTOMER


# ==============================================================================
# 3. PRUEBAS DE DEDUPLICACIÓN Y MERGE SCD TIPO 1 (ORDERS)
# ==============================================================================

def test_orders_scd1_dedup_and_merge_lifecycle() -> None:
    """Valida la deduplicación intra-batch y la semántica completa de MERGE en órdenes."""
    # Deduplicación intra-batch: debe prevalecer la versión con updated_at más reciente
    batch = [
        {"order_id": "ORD-1", "total_amount": "100", "updated_at": "2026-01-01 10:00:00", "transaction_type": "INSERT"},
        {"order_id": "ORD-1", "total_amount": "120", "updated_at": "2026-01-01 12:00:00", "transaction_type": "UPDATE"},
    ]
    deduped = deduplicate_orders_memory(batch)
    assert len(deduped) == 1
    assert deduped[0]["total_amount"] == "120"

    # Simulación de ciclo de vida MERGE
    current_silver = [
        {"order_id": "ORD-1", "total_amount": "100", "updated_at": "2026-01-01 10:00:00"},
        {"order_id": "ORD-2", "total_amount": "200", "updated_at": "2026-01-01 10:00:00"},
    ]

    incoming = [
        # UPDATE reciente para ORD-1
        {"order_id": "ORD-1", "total_amount": "150", "updated_at": "2026-01-01 11:00:00", "transaction_type": "UPDATE"},
        # DELETE para ORD-2
        {"order_id": "ORD-2", "total_amount": "200", "updated_at": "2026-01-01 11:00:00", "transaction_type": "DELETE"},
        # INSERT nuevo para ORD-3
        {"order_id": "ORD-3", "total_amount": "300", "updated_at": "2026-01-01 11:00:00", "transaction_type": "INSERT"},
    ]

    merged = simulate_orders_scd1_merge(current_silver, incoming)
    merged_by_id = {r["order_id"]: r for r in merged}

    # ORD-1 actualizado
    assert merged_by_id["ORD-1"]["total_amount"] == "150"
    # ORD-2 eliminado
    assert "ORD-2" not in merged_by_id
    # ORD-3 insertado
    assert merged_by_id["ORD-3"]["total_amount"] == "300"


# ==============================================================================
# 4. PRUEBAS DE SCD TIPO 2 EN CLIENTES (SURROGATE KEY, VIGENCIAS & DELETE)
# ==============================================================================

def test_customers_scd2_vigencias_and_surrogate_key() -> None:
    """Valida la generación de surrogate key xxhash64 y la transición de estados SCD2."""
    # Verificación de surrogate key (int64 signed BIGINT)
    sk = compute_customer_sk_py("CUST-100", "2026-01-01 00:00:00")
    assert isinstance(sk, int)
    assert -(1 << 63) <= sk < (1 << 63)

    # 1. Inserción inicial de cliente (Versión 1)
    init_event = [
        {
            "customer_id": "CUST-100",
            "first_name": "Maria",
            "country": "Mexico",
            "segment": "RETAIL",
            "email": "maria@example.com",
            "updated_at": "2026-01-01 00:00:00",
            "transaction_type": "INSERT",
        }
    ]
    dim = process_customers_scd2_records([], init_event)
    assert len(dim) == 1
    v1 = dim[0]
    assert v1["is_current"] is True
    assert v1["end_date"] is None
    assert v1["segment"] == "RETAIL"
    assert v1["email_masked"] == mask_pii_py("maria@example.com")

    # 2. Actualización de segmento a VIP (Versión 2 abre vigencia, Versión 1 cierra)
    update_event = [
        {
            "customer_id": "CUST-100",
            "country": "Mexico",
            "segment": "VIP",
            "email": "maria@example.com",
            "updated_at": "2026-02-01 00:00:00",
            "transaction_type": "UPDATE",
        }
    ]
    dim = process_customers_scd2_records(dim, update_event)
    assert len(dim) == 2

    # V1 debe estar cerrada
    v1_closed = [r for r in dim if r["is_current"] is False][0]
    assert v1_closed["segment"] == "RETAIL"
    assert v1_closed["end_date"] == "2026-02-01 00:00:00"

    # V2 debe estar activa
    v2_active = [r for r in dim if r["is_current"] is True][0]
    assert v2_active["segment"] == "VIP"
    assert v2_active["effective_date"] == "2026-02-01 00:00:00"
    assert v2_active["end_date"] is None

    # 3. Evento DELETE: debe cerrar la versión activa conservando todo el histórico
    delete_event = [
        {
            "customer_id": "CUST-100",
            "updated_at": "2026-03-01 00:00:00",
            "transaction_type": "DELETE",
        }
    ]
    dim = process_customers_scd2_records(dim, delete_event)
    assert len(dim) == 2  # No se borran filas
    assert all(r["is_current"] is False for r in dim)
    v2_closed = [r for r in dim if r["segment"] == "VIP"][0]
    assert v2_closed["end_date"] == "2026-03-01 00:00:00"


# ==============================================================================
# 5. PRUEBAS DE GOBERNANZA DDL Y MERGE DELTATABLE DIRECTO
# ==============================================================================

def test_merge_customers_scd2_governed_merge() -> None:
    """Valida la resolución estricta de DeltaTable.forName y ejecución de MERGE en customers_silver."""
    from unittest.mock import MagicMock, patch
    from src.silver.customers_scd2 import merge_customers_scd2

    mock_spark = MagicMock()
    mock_delta_target = MagicMock()
    mock_df_active = MagicMock()
    mock_delta_target.toDF.return_value.filter.return_value = mock_df_active
    mock_joined = MagicMock()
    mock_df_staging = MagicMock()
    mock_df_staging.alias.return_value.join.return_value = mock_joined
    mock_rows = MagicMock()
    mock_joined.filter.return_value.select.return_value = mock_rows

    mock_functions = MagicMock()
    mock_functions.expr.side_effect = lambda expr_str: MagicMock(alias=lambda name: MagicMock())

    with patch("src.silver.customers_scd2.DeltaTable.forName", return_value=mock_delta_target) as mock_for_name, \
         patch("src.silver.customers_scd2.prepare_customers_staging_df", return_value=mock_df_staging), \
         patch("pyspark.sql.functions.col", side_effect=lambda col_str: MagicMock(alias=lambda name: MagicMock())), \
         patch("pyspark.sql.functions.expr", side_effect=lambda expr_str: MagicMock(alias=lambda name: MagicMock())):
        result = merge_customers_scd2(mock_spark, mock_df_staging, catalog="main", schema="chubb_lakehouse")
        assert result["status"] == "SUCCESS"
        assert result["target_table"] == "main.chubb_lakehouse.customers_silver"
        mock_for_name.assert_called_once_with(mock_spark, "main.chubb_lakehouse.customers_silver")
        assert mock_delta_target.alias.called


def test_merge_orders_scd1_governed_merge() -> None:
    """Valida la resolución estricta de DeltaTable.forName y ejecución de MERGE en orders_silver."""
    from unittest.mock import MagicMock, patch
    from src.silver.orders_scd1 import merge_orders_scd1

    mock_spark = MagicMock()
    mock_delta_target = MagicMock()
    mock_df = MagicMock()

    with patch("src.silver.orders_scd1.DeltaTable.forName", return_value=mock_delta_target) as mock_for_name, \
         patch("src.silver.orders_scd1.deduplicate_orders", return_value=mock_df), \
         patch("src.silver.orders_scd1.prepare_orders_silver_df", return_value=mock_df):
        result = merge_orders_scd1(mock_spark, mock_df, catalog="main", schema="chubb_lakehouse")
        assert result["status"] == "SUCCESS"
        assert result["target_table"] == "main.chubb_lakehouse.orders_silver"
        mock_for_name.assert_called_once_with(mock_spark, "main.chubb_lakehouse.orders_silver")
        assert mock_delta_target.alias.called
