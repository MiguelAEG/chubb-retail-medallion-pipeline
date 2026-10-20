"""
==============================================================================
PRUEBA UNITARIA: src/tests/unit/test_assertions.py
PROPÓSITO:
Verificación quirúrgica, determinista y de alta señal del framework de aserciones
sobre tablas reales sin duplicar código (src.utils.assertions):
1. Validación de aserciones de Capa Bronze con Mock Spark (éxito y fallos de invariantes).
2. Validación de aserciones de Capa Silver con Mock Spark (SCD1, SCD2, montos y cuarentena).
3. Validación de aserciones de Capa Gold con Mock Spark (Ventas, LTV y balance de conservación DQ).
4. Ejecución ultrarrápida (< 150ms) sin clúster JVM pesado ni instancias locales de Spark.
==============================================================================
"""

from unittest.mock import MagicMock
import pytest

from src.utils.assertions import (
    validate_bronze_layer,
    validate_silver_layer,
    validate_gold_layer,
)


def _build_mock_spark(query_map: dict) -> MagicMock:
    """Crea un Mock de SparkSession cuyo spark.sql().collect() responde según la query."""
    mock_spark = MagicMock()

    # Priorizar patterns que contienen WHERE sobre conteos simples sin WHERE
    def sort_key(item):
        pattern = item[0]
        has_where = 1 if "WHERE" in pattern or "where" in pattern else 0
        return (has_where, len(pattern))

    sorted_patterns = sorted(query_map.items(), key=sort_key, reverse=True)

    def sql_side_effect(query: str):
        cleaned_query = " ".join(query.strip().split())
        res_mock = MagicMock()
        for pattern, val in sorted_patterns:
            if pattern in cleaned_query:
                row_mock = MagicMock()
                row_mock.__getitem__.return_value = val
                res_mock.collect.return_value = [row_mock]
                return res_mock
        # Si no matchea nada, retornar 0
        row_mock = MagicMock()
        row_mock.__getitem__.return_value = 0
        res_mock.collect.return_value = [row_mock]
        return res_mock

    mock_spark.sql.side_effect = sql_side_effect
    return mock_spark


# ==============================================================================
# 1. PRUEBAS DE ASERCIONES DE CAPA BRONZE
# ==============================================================================

def test_validate_bronze_layer_success() -> None:
    """Valida que validate_bronze_layer apruebe cuando todas las invariantes se cumplen."""
    query_map = {
        "count(*) FROM main.chubb_lakehouse.orders_bronze WHERE _raw_hash IS NULL": 0,
        "count(*) FROM main.chubb_lakehouse.customers_bronze WHERE _raw_hash IS NULL": 0,
        "count(*) FROM main.chubb_lakehouse.orders_bronze WHERE _source_file IS NULL": 0,
        "count(*) FROM main.chubb_lakehouse.customers_bronze WHERE _source_file IS NULL": 0,
        "count(*) FROM main.chubb_lakehouse.orders_bronze": 150,
        "count(*) FROM main.chubb_lakehouse.customers_bronze": 50,
        "bronze_ingestion_log WHERE status = 'SUCCESS'": 4,
    }
    mock_spark = _build_mock_spark(query_map)

    metrics = validate_bronze_layer(mock_spark, catalog="main", schema="chubb_lakehouse")
    assert metrics["status"] == "PASS"
    assert metrics["orders_count"] == 150
    assert metrics["customers_count"] == 50
    assert metrics["orders_null_raw_hash"] == 0
    assert metrics["customers_null_raw_hash"] == 0


def test_validate_bronze_layer_fails_on_empty_table() -> None:
    """Valida que falle con AssertionError si orders_bronze está vacía."""
    query_map = {
        "count(*) FROM main.chubb_lakehouse.orders_bronze": 0,
        "count(*) FROM main.chubb_lakehouse.customers_bronze": 50,
    }
    mock_spark = _build_mock_spark(query_map)

    with pytest.raises(AssertionError, match="orders_bronze está vacía"):
        validate_bronze_layer(mock_spark, catalog="main", schema="chubb_lakehouse")


def test_validate_bronze_layer_fails_on_null_raw_hash() -> None:
    """Valida que falle con AssertionError si _raw_hash contiene valores NULL."""
    query_map = {
        "count(*) FROM main.chubb_lakehouse.orders_bronze WHERE _raw_hash IS NULL": 3,
        "count(*) FROM main.chubb_lakehouse.orders_bronze": 100,
        "count(*) FROM main.chubb_lakehouse.customers_bronze": 50,
    }
    mock_spark = _build_mock_spark(query_map)

    with pytest.raises(AssertionError, match="_raw_hash NULL"):
        validate_bronze_layer(mock_spark, catalog="main", schema="chubb_lakehouse")


# ==============================================================================
# 2. PRUEBAS DE ASERCIONES DE CAPA SILVER
# ==============================================================================

def test_validate_silver_layer_success() -> None:
    """Valida que validate_silver_layer apruebe con datos conformes (SCD1, SCD2, montos, cuarentena)."""
    query_map = {
        "GROUP BY order_id HAVING count(*) > 1": 0,
        "orders_silver WHERE total_amount <= 0": 0,
        "orders_silver WHERE amount <= 0": 0,
        "GROUP BY customer_id HAVING count(*) > 1": 0,
        "count(*) FROM main.chubb_lakehouse.orders_quarantine WHERE error_code IS NULL": 0,
        "count(*) FROM main.chubb_lakehouse.customers_quarantine WHERE error_code IS NULL": 0,
        "count(*) FROM main.chubb_lakehouse.orders_quarantine": 12,
        "count(*) FROM main.chubb_lakehouse.customers_quarantine": 5,
    }
    mock_spark = _build_mock_spark(query_map)

    metrics = validate_silver_layer(mock_spark, catalog="main", schema="chubb_lakehouse")
    assert metrics["status"] == "PASS"
    assert metrics["orders_duplicate_pks"] == 0
    assert metrics["orders_non_positive_amount"] == 0
    assert metrics["customers_active_duplicates"] == 0
    assert metrics["orders_quarantined_total"] == 12
    assert metrics["customers_quarantined_total"] == 5


def test_validate_silver_layer_fails_on_duplicate_scd1() -> None:
    """Valida que falle si orders_silver contiene duplicados en order_id."""
    query_map = {
        "GROUP BY order_id HAVING count(*) > 1": 2,
    }
    mock_spark = _build_mock_spark(query_map)

    with pytest.raises(AssertionError, match="order_ids duplicados"):
        validate_silver_layer(mock_spark, catalog="main", schema="chubb_lakehouse")


def test_validate_silver_layer_fails_on_duplicate_active_scd2() -> None:
    """Valida que falle si customers_silver contiene múltiples versiones activas por cliente."""
    query_map = {
        "GROUP BY order_id HAVING count(*) > 1": 0,
        "orders_silver WHERE total_amount <= 0": 0,
        "GROUP BY customer_id HAVING count(*) > 1": 1,
    }
    mock_spark = _build_mock_spark(query_map)

    with pytest.raises(AssertionError, match="múltiples registros vigentes activos"):
        validate_silver_layer(mock_spark, catalog="main", schema="chubb_lakehouse")


# ==============================================================================
# 3. PRUEBAS DE ASERCIONES DE CAPA GOLD
# ==============================================================================

def test_validate_gold_layer_success() -> None:
    """Valida que validate_gold_layer apruebe con KPIs y reconciliación balanceada."""
    query_map = {
        "ventas_diarias_x_pais WHERE total_revenue <= 0": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.ventas_diarias_x_pais": 15,
        "SELECT coalesce(sum(total_revenue), 0.0) FROM main.chubb_lakehouse.ventas_diarias_x_pais": 12500.50,
        "SELECT coalesce(sum(o.total_amount), 0.0) FROM main.chubb_lakehouse.orders_silver": 12500.50,
        "customer_ltv WHERE tenure_days < 0": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.customer_ltv": 45,
        "data_quality_summary WHERE total_received != (accepted_rows + quarantined_rows + deduplicated_rows)": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.data_quality_summary": 2,
    }
    mock_spark = _build_mock_spark(query_map)

    metrics = validate_gold_layer(mock_spark, catalog="main", schema="chubb_lakehouse")
    assert metrics["status"] == "PASS"
    assert metrics["ventas_diarias_rows"] == 15
    assert metrics["gold_total_revenue"] == 12500.50
    assert metrics["revenue_reconciliation_diff"] < 0.05
    assert metrics["dq_unbalanced_rows"] == 0


def test_validate_gold_layer_fails_on_financial_divergence() -> None:
    """Valida que falle si hay discrepancia financiera entre Gold y Silver."""
    query_map = {
        "ventas_diarias_x_pais WHERE total_revenue <= 0": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.ventas_diarias_x_pais": 15,
        "SELECT coalesce(sum(total_revenue), 0.0) FROM main.chubb_lakehouse.ventas_diarias_x_pais": 15000.00,
        "SELECT coalesce(sum(o.total_amount), 0.0) FROM main.chubb_lakehouse.orders_silver": 12000.00,
        "customer_ltv WHERE tenure_days < 0": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.customer_ltv": 45,
        "data_quality_summary WHERE total_received != (accepted_rows + quarantined_rows + deduplicated_rows)": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.data_quality_summary": 2,
    }
    mock_spark = _build_mock_spark(query_map)

    with pytest.raises(AssertionError, match="Violación de reconciliación financiera"):
        validate_gold_layer(mock_spark, catalog="main", schema="chubb_lakehouse")


def test_validate_gold_layer_fails_on_dq_mass_imbalance() -> None:
    """Valida que falle si la conservación de masa en data_quality_summary no cuadra."""
    query_map = {
        "ventas_diarias_x_pais WHERE total_revenue <= 0": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.ventas_diarias_x_pais": 15,
        "SELECT coalesce(sum(total_revenue), 0.0) FROM main.chubb_lakehouse.ventas_diarias_x_pais": 1000.00,
        "SELECT coalesce(sum(o.total_amount), 0.0) FROM main.chubb_lakehouse.orders_silver": 1000.00,
        "customer_ltv WHERE tenure_days < 0": 0,
        "SELECT count(*) FROM main.chubb_lakehouse.customer_ltv": 45,
        "data_quality_summary WHERE total_received != (accepted_rows + quarantined_rows + deduplicated_rows)": 1,
        "SELECT count(*) FROM main.chubb_lakehouse.data_quality_summary": 2,
    }
    mock_spark = _build_mock_spark(query_map)

    with pytest.raises(AssertionError, match="Violación de balance DQ"):
        validate_gold_layer(mock_spark, catalog="main", schema="chubb_lakehouse")
