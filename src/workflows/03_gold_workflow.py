# Databricks notebook source
# COMMAND ----------
# MAGIC %md
# MAGIC # 03_gold_workflow: Capa Gold - Modelado Dimensional & Observabilidad DQ
# MAGIC 
# MAGIC Consolidación analítica de negocio: Ventas Diarias por País, Customer Lifetime Value y Resumen de Data Quality.

# COMMAND ----------
# ==============================================================================
# 1.1 IMPORTS Y RESOLUCIÓN DE CONTEXTO
# ==============================================================================
import sys
sys.path.extend(['..', '../..'])

from src.gold.ventas_diarias import compute_ventas_diarias_df, merge_ventas_diarias
from src.gold.customer_ltv import compute_customer_ltv_df, merge_customer_ltv
from src.utils.dq_summary import compute_dq_summary_df, merge_dq_summary
from src.utils.assertions import validate_gold_layer

# Catálogo y Esquema Dedicados del Lakehouse
catalog = "chubb_lakehouse"
schema = "chubb_lakehouse"
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------
# ==============================================================================
# 1.2 AGREGACIÓN VENTAS DIARIAS POR PAÍS
# ==============================================================================
df_orders = spark.table(f"{catalog}.{schema}.orders_silver")
df_customers = spark.table(f"{catalog}.{schema}.customers_silver")
df_ventas = compute_ventas_diarias_df(spark=spark, df_orders=df_orders, df_customers=df_customers)
res_ventas = merge_ventas_diarias(spark=spark, df_ventas_diarias=df_ventas, catalog=catalog, schema=schema)
print(f"Ventas Diarias por País actualizadas con éxito en {catalog}.{schema}.ventas_diarias_x_pais")

# COMMAND ----------
# ==============================================================================
# 1.3 COMPUTACIÓN CUSTOMER LIFETIME VALUE (LTV)
# ==============================================================================
df_ltv = compute_customer_ltv_df(spark=spark, df_orders=df_orders, df_customers=df_customers)
res_ltv = merge_customer_ltv(spark=spark, df_customer_ltv=df_ltv, catalog=catalog, schema=schema)
print(f"Customer LTV actualizado con éxito en {catalog}.{schema}.customer_ltv")

# COMMAND ----------
# ==============================================================================
# 1.4 GENERACIÓN DATA QUALITY SUMMARY (batch x table)
# ==============================================================================
# 1.4.1 Resumen de Órdenes
df_raw_orders = spark.table(f"{catalog}.{schema}.orders_bronze")
df_quar_orders = spark.table(f"{catalog}.{schema}.orders_quarantine")
df_dq_orders = compute_dq_summary_df(spark=spark, df_bronze=df_raw_orders, df_silver=df_orders, df_quarantine=df_quar_orders, table_name="orders")
merge_dq_summary(spark=spark, df_dq_summary=df_dq_orders, catalog=catalog, schema=schema)

# 1.4.2 Resumen de Clientes
df_raw_cust = spark.table(f"{catalog}.{schema}.customers_bronze")
df_quar_cust = spark.table(f"{catalog}.{schema}.customers_quarantine")
df_dq_cust = compute_dq_summary_df(spark=spark, df_bronze=df_raw_cust, df_silver=df_customers, df_quarantine=df_quar_cust, table_name="customers")
merge_dq_summary(spark=spark, df_dq_summary=df_dq_cust, catalog=catalog, schema=schema)

print(f"Data Quality Summary actualizado con éxito para orders y customers en {catalog}.{schema}.data_quality_summary")

# COMMAND ----------
# ==============================================================================
# 1.5 VERIFICACIÓN VISUAL DE TABLAS GOLD
# ==============================================================================
display(spark.sql(f"SELECT * FROM {catalog}.{schema}.ventas_diarias_x_pais ORDER BY order_date DESC LIMIT 10"))
display(spark.sql(f"SELECT * FROM {catalog}.{schema}.customer_ltv ORDER BY total_orders DESC LIMIT 10"))
display(spark.sql(f"SELECT * FROM {catalog}.{schema}.data_quality_summary ORDER BY _calculated_at DESC LIMIT 10"))

# COMMAND ----------
# ==============================================================================
# 3.5 ASERCIONES DE CALIDAD SOBRE TABLAS REALES
# ==============================================================================
validate_gold_layer(spark=spark, catalog=catalog, schema=schema)
