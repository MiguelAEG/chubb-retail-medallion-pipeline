# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 00_setup_and_seed: Setup Inicial, DDLs y Generación de Datos Sintéticos
# MAGIC
# MAGIC Aprovisionamiento idempotente de esquemas, volúmenes de Unity Catalog y generación de datos de prueba.

# COMMAND ----------

# ==============================================================================
# 1.1 IMPORTS Y RESOLUCIÓN DE CONTEXTO
# ==============================================================================
import sys
sys.path.extend(['..', '../..'])

from src.setup.ddl.init_tables import create_all_tables
from src.setup.generator.data_generator import generate_all_data

# Catálogo y Esquema Dedicados del Lakehouse
catalog = "chubb_lakehouse"
schema = "chubb_lakehouse"
volume_path = f"/Volumes/{catalog}/{schema}/raw_landing"

# Creación explícita e idempotente del Catálogo
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")

# COMMAND ----------

# ==============================================================================
# 1.2 APROVISIONAMIENTO DDL IDEMPOTENTE
# ==============================================================================
ddl_summary = create_all_tables(spark=spark, catalog=catalog, schema=schema)
print(f"Tablas y vistas inicializadas con éxito en {catalog}.{schema}")

# COMMAND ----------

# ==============================================================================
# 1.3 GENERACIÓN Y SIEMBRA DE DATOS EN VOLUME
# ==============================================================================
gen_result = generate_all_data(output_dir=volume_path, seed=42)
print(f"Archivos generados: {len(gen_result.get('files_generated', []))} | Registros: {gen_result.get('total_records_written', 0)}")

# COMMAND ----------

# ==============================================================================
# 1.4 VERIFICACIÓN DE TABLAS
# ==============================================================================
display(spark.sql(f"SHOW TABLES IN {catalog}.{schema}"))