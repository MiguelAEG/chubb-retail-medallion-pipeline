# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 01_bronze_workflow: Ingesta Incremental y Linaje en Capa Bronze
# MAGIC 
# MAGIC Ingesta cruda, inyección de metadatos de auditoría y linaje técnico para Customers y Orders.

# COMMAND ----------
# ==============================================================================
# 1.1 IMPORTS Y RESOLUCIÓN DE CONTEXTO
# ==============================================================================
import sys
sys.path.extend(['..', '../..'])

from src.bronze.customers_bronze import ingest_customers_bronze
from src.bronze.orders_bronze import ingest_orders_bronze
from src.utils.assertions import validate_bronze_layer

# Catálogo y Esquema Dedicados del Lakehouse
catalog = "chubb_lakehouse"
schema = "chubb_lakehouse"
volume_path = f"/Volumes/{catalog}/{schema}/raw_landing"

spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------
# ==============================================================================
# 1.2 INGESTA BRONZE: CUSTOMERS (customers_bronze)
# ==============================================================================
res_cust = ingest_customers_bronze(spark=spark, catalog_name=catalog, schema=schema, landing_volume=volume_path)
print(f"Customers Bronze: {res_cust.get('rows_ingested', 0)} filas ({res_cust.get('files_ingested', 0)} archivos)")

# COMMAND ----------
# ==============================================================================
# 1.3 INGESTA BRONZE: ORDERS (orders_bronze)
# ==============================================================================
res_orders = ingest_orders_bronze(spark=spark, catalog_name=catalog, schema=schema, landing_volume=volume_path)
print(f"Orders Bronze: {res_orders.get('rows_ingested', 0)} filas ({res_orders.get('files_ingested', 0)} archivos)")

# COMMAND ----------
# ==============================================================================
# 1.4 VERIFICACIÓN Y BITÁCORA DE INGESTA
# ==============================================================================
display(spark.sql(f"""
    SELECT entity, batch_id, status, rows_read, rows_ingested, source_file, ingested_at 
    FROM {catalog}.{schema}.bronze_ingestion_log 
    ORDER BY ingested_at DESC LIMIT 20
"""))

# COMMAND ----------
# ==============================================================================
# 1.5 ASERCIONES DE CALIDAD SOBRE TABLAS REALES
# ==============================================================================
validate_bronze_layer(spark=spark, catalog=catalog, schema=schema)
