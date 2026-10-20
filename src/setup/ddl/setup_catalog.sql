-- ==============================================================================
-- SCRIPT: src/setup/ddl/setup_catalog.sql
-- PROPÓSITO: Configuración idempotente de catálogo, esquemas y volúmenes en Unity Catalog
-- PROYECTO: Prueba_Tecnica — Chubb Regional 2026
-- ==============================================================================

-- 1. Catálogo principal en Unity Catalog
CREATE CATALOG IF NOT EXISTS chubb_lakehouse;
USE CATALOG chubb_lakehouse;

-- 2. Esquema principal unificado de Chubb Lakehouse
CREATE SCHEMA IF NOT EXISTS chubb_lakehouse.chubb_lakehouse
COMMENT 'Esquema principal unificado de Chubb Lakehouse';

-- Volumen landing zone bajo el esquema principal
CREATE VOLUME IF NOT EXISTS chubb_lakehouse.chubb_lakehouse.raw_landing
COMMENT 'Volumen landing zone para recepcion de archivos CSV/JSON crudos';

-- 3. Esquemas de arquitectura Medallion bajo catálogo chubb_lakehouse
CREATE SCHEMA IF NOT EXISTS chubb_lakehouse.bronze
COMMENT 'Capa Bronze: Tablas Delta crudas append-only y metadatos tecnicos de auditoria';

CREATE SCHEMA IF NOT EXISTS chubb_lakehouse.silver
COMMENT 'Capa Silver: Tablas conformadas, limpias, deduplicadas (SCD1 / SCD2)';

CREATE SCHEMA IF NOT EXISTS chubb_lakehouse.quarantine
COMMENT 'Capa Quarantine: Dead-letter queue para aislamiento de registros anomalos';

CREATE SCHEMA IF NOT EXISTS chubb_lakehouse.gold
COMMENT 'Capa Gold: Vistas y tablas analiticas con KPIs agregados de negocio';

CREATE SCHEMA IF NOT EXISTS chubb_lakehouse.audit
COMMENT 'Capa Audit: Bitacoras de ejecucion, ingesta y observabilidad operacional';

-- 4. Volumen para landing zone de ingesta cruda en Bronze
CREATE VOLUME IF NOT EXISTS chubb_lakehouse.bronze.raw_landing
COMMENT 'Volumen landing zone para recepcion de archivos CSV/JSON crudos de clientes y ordenes';
