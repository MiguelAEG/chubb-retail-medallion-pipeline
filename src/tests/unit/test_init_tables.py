"""
Pruebas unitarias para src.setup.ddl.init_tables y resolución dinámica de estructuras.
"""

from unittest.mock import MagicMock
import pytest
from src.setup.ddl.init_tables import create_all_tables, get_ddl_statements
from src.setup.generator.data_generator import generate_all_data, generate_all_datasets


def test_get_ddl_statements_parametrized():
    statements = get_ddl_statements(catalog="test_catalog", schema="chubb_lakehouse")
    assert len(statements) > 10
    
    # Verificar que el catálogo y esquema se inyectan correctamente
    schema_stmts = [s for s in statements if "CREATE SCHEMA IF NOT EXISTS test_catalog.chubb_lakehouse" in s]
    assert len(schema_stmts) == 1

    volume_stmts = [s for s in statements if "CREATE VOLUME IF NOT EXISTS test_catalog.chubb_lakehouse.raw_landing" in s]
    assert len(volume_stmts) == 1

    table_stmts = [s for s in statements if "CREATE TABLE IF NOT EXISTS test_catalog.chubb_lakehouse.customers_bronze" in s]
    assert len(table_stmts) == 1

    # Verificar que customers_silver y orders_silver son tablas Delta estándar sin CLUSTER BY
    cust_silver_stmts = [s for s in statements if "CREATE TABLE IF NOT EXISTS test_catalog.chubb_lakehouse.customers_silver" in s]
    assert len(cust_silver_stmts) == 1
    assert "USING DELTA" in cust_silver_stmts[0]
    assert "CLUSTER BY" not in cust_silver_stmts[0]

    orders_silver_stmts = [s for s in statements if "CREATE TABLE IF NOT EXISTS test_catalog.chubb_lakehouse.orders_silver" in s]
    assert len(orders_silver_stmts) == 1
    assert "USING DELTA" in orders_silver_stmts[0]
    assert "CLUSTER BY" not in orders_silver_stmts[0]

    # Ninguna sentencia DDL debe contener CLUSTER BY
    assert all("CLUSTER BY" not in s for s in statements)


def test_create_all_tables_mock_spark():
    mock_spark = MagicMock()
    mock_spark.sql.return_value = None

    result = create_all_tables(mock_spark, catalog="test_catalog", schema="chubb_lakehouse")
    assert result["status"] == "SUCCESS"
    assert result["catalog"] == "test_catalog"
    assert result["schema"] == "chubb_lakehouse"
    assert result["executed_count"] > 10
    assert mock_spark.sql.called


def test_create_all_tables_fails_on_ddl_error():
    """Valida que create_all_tables falle inmediatamente con RuntimeError si una sentencia DDL falla."""
    mock_spark = MagicMock()
    # Permitir CREATE CATALOG, USE CATALOG y CREATE SCHEMA, pero fallar en la primera sentencia de tabla
    mock_spark.sql.side_effect = [None, None, None, Exception("Delta table creation syntax error")]

    with pytest.raises(RuntimeError, match="Fallo crítico en aprovisionamiento DDL"):
        create_all_tables(mock_spark, catalog="test_catalog", schema="chubb_lakehouse")


def test_generate_all_data_alias():
    assert generate_all_data == generate_all_datasets
