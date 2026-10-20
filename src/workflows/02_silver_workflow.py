# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 02_silver_workflow: Capa Silver - Data Quality, Cuarentena & SCD1 / SCD2
# MAGIC 
# MAGIC Limpieza, Data Quality Gate, desvío a Cuarentena y procesamiento dimensional SCD2 (Clientes) y SCD1 (Órdenes).

# COMMAND ----------
# ==============================================================================
# 1.1 IMPORTS Y RESOLUCIÓN DE CONTEXTO
# ==============================================================================
import sys
sys.path.extend(['..', '../..'])

from src.silver.customers_scd2 import process_customers_silver
from src.silver.orders_scd1 import process_orders_silver
from src.utils.assertions import validate_silver_layer

# Catálogo y Esquema Dedicados del Lakehouse
catalog = "chubb_lakehouse"
schema = "chubb_lakehouse"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------
# ==============================================================================
# 1.2 TRANSFORMACIÓN CUSTOMERS SILVER (SCD2 & CUARENTENA)
# ==============================================================================
res_cust = process_customers_silver(spark=spark, catalog=catalog, schema=schema)
print(f"Customers Silver: {res_cust.get('valid_promoted', 0)} válidos | {res_cust.get('quarantined', 0)} en cuarentena")

# COMMAND ----------
# ==============================================================================
# 1.3 TRANSFORMACIÓN ORDERS SILVER (SCD1 & CUARENTENA)
# ==============================================================================
res_orders = process_orders_silver(spark=spark, catalog=catalog, schema=schema)
print(f"Orders Silver: {res_orders.get('valid_promoted', 0)} válidas | {res_orders.get('quarantined', 0)} en cuarentena")

# COMMAND ----------
# ==============================================================================
# 1.4 VERIFICACIÓN DE TABLAS SILVER Y CUARENTENA
# ==============================================================================
display(spark.sql(f"""
    SELECT 'customers_silver' AS tabla, is_current, COUNT(*) AS total FROM {catalog}.{schema}.customers_silver GROUP BY is_current
    UNION ALL
    SELECT 'orders_silver' AS tabla, (status = 'COMPLETED') AS is_current, COUNT(*) AS total FROM {catalog}.{schema}.orders_silver GROUP BY (status = 'COMPLETED')
"""))
display(spark.sql(f"SELECT error_code, rejection_reason, COUNT(*) AS fallos FROM {catalog}.{schema}.orders_quarantine GROUP BY error_code, rejection_reason"))

# COMMAND ----------
# ==============================================================================
# 2.5 ASERCIONES DE CALIDAD SOBRE TABLAS REALES
# ==============================================================================
validate_silver_layer(spark=spark, catalog=catalog, schema=schema)
