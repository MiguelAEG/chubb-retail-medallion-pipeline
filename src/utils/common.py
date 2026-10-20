"""
==============================================================================
MÓDULO: src.utils.common
PROPÓSITO:
Funciones comunes y utilitarias para la Capa Bronze:
- Extracción e inferencia de batch_id desde nombres de archivo.
- Consulta de auditoría de archivos previamente procesados (get_ingested_files).
- Filtrado de archivos pendientes para garantizar idempotencia estricta.
- Descubrimiento de archivos en directorios y volúmenes de landing.
- Registro atómico de eventos de ingesta en bronze_ingestion_log.
- Cálculo determinista de hash crudo (_raw_hash / xxhash64).
==============================================================================
"""

import datetime
import hashlib
import logging
from pathlib import Path
import re
import struct
import sys
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("bronze.common")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

try:
    import xxhash  # type: ignore
    _HAS_XXHASH = True
except ImportError:
    _HAS_XXHASH = False


def extract_batch_id(file_name_or_path: str, fallback_batch_id: Optional[str] = None) -> str:
    """
    Infiere el identificador del lote (_batch_id) a partir del nombre del archivo.
    Si no se detecta un patrón conocido, recurre a fallback_batch_id o genera uno determinista.

    Ejemplos:
    - 'customers_initial_part1.csv' -> 'initial_part1'
    - 'orders_incremental_batch2.csv' -> 'batch_2'
    - 'customers_batch1.csv' -> 'batch_1'
    """
    clean_name = Path(file_name_or_path).name.lower()

    if "initial" in clean_name:
        match_part = re.search(r"part\d+", clean_name)
        if match_part:
            return f"initial_{match_part.group(0)}"
        return "initial"

    match_batch = re.search(r"batch_?(\d+)", clean_name)
    if match_batch:
        return f"batch_{match_batch.group(1)}"

    if fallback_batch_id:
        return fallback_batch_id

    return f"batch_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%d_%H%M%S')}"


def compute_python_raw_hash(row_dict: Dict[str, Any], business_columns: Sequence[str]) -> int:
    """
    Calcula el hash determinista de 64-bit (int64 signed / BIGINT) de una fila a partir de un diccionario.
    Utilizado para validaciones unitarias y trazabilidad sin dependencia de JVM.
    Usa la librería xxhash si está disponible, o simulación determinista 64-bit (int64 signed)
    vía hashlib.sha256 truncado a 8 bytes con struct.unpack.
    """
    tokens = [str(row_dict.get(col, "") if row_dict.get(col) is not None else "") for col in business_columns]
    payload = "||".join(tokens)
    if _HAS_XXHASH:
        raw_val = xxhash.xxh64(payload.encode("utf-8")).intdigest()
        if raw_val >= (1 << 63):
            return raw_val - (1 << 64)
        return raw_val
    else:
        digest = hashlib.sha256(payload.encode("utf-8")).digest()
        (val,) = struct.unpack(">q", digest[:8])
        return val


def compute_raw_hash_col(business_columns: Sequence[str]) -> Any:
    """
    Genera la expresión de columna PySpark para calcular el hash xxhash64
    de todos los campos de negocio fidedignos de la fila (BigInt / LongType de 8 bytes).
    Optimizado nativamente en Databricks Photon.
    """
    from pyspark.sql import functions as F

    return F.xxhash64(*[F.col(col_name) for col_name in business_columns])


def get_ingested_files(spark: Any, catalog_name: str, schema: str = "bronze") -> Set[str]:
    """
    Consulta la tabla de control bronze_ingestion_log para obtener el conjunto
    de archivos que ya fueron procesados con estado 'SUCCESS'.
    Garantiza idempotencia matemática y previene reprocesamientos.
    """
    if schema == "bronze":
        log_tables = [
            f"{catalog_name}.bronze.bronze_ingestion_log",
            f"{catalog_name}.chubb_lakehouse.bronze_ingestion_log",
        ]
    else:
        log_tables = [
            f"{catalog_name}.{schema}.bronze_ingestion_log",
            f"{catalog_name}.bronze.bronze_ingestion_log",
        ]
    df_log = None
    resolved_table = None

    for tbl in log_tables:
        try:
            if spark.catalog.tableExists(tbl):
                df_log = spark.table(tbl).filter("status = 'SUCCESS'").select("source_file")
                resolved_table = tbl
                break
        except Exception:
            continue

    if df_log is None:
        logger.info('{"event": "log_table_not_found", "checked": "%s"}', str(log_tables))
        return set()

    try:
        rows = df_log.collect()
        ingested = {str(r["source_file"]).strip() for r in rows if r["source_file"]}
        basenames = {Path(p).name for p in ingested}
        return ingested.union(basenames)
    except Exception as exc:
        logger.warning(
            '{"event": "get_ingested_files_warning", "table": "%s", "error": "%s"}',
            resolved_table,
            str(exc),
        )
        return set()


def list_landing_files(landing_dir: str, entity: str) -> List[str]:
    """
    Explora el directorio de landing para identificar archivos CSV correspondientes a la entidad.
    Soporta rutas POSIX/Windows locales y rutas de Databricks Volumes.
    """
    p = Path(landing_dir)
    entity_p = p / entity

    search_paths = [entity_p, p] if entity_p.is_dir() else [p]
    found_files: List[str] = []

    for target_dir in search_paths:
        if target_dir.exists() and target_dir.is_dir():
            for item in sorted(target_dir.iterdir()):
                if item.is_file() and item.name.lower().endswith(".csv"):
                    if entity in item.name.lower() or target_dir == entity_p:
                        found_files.append(str(item).replace("\\", "/"))

    def _landing_sort_key(f: str) -> Tuple[int, str]:
        fn = Path(f).name.lower()
        is_inc = 1 if ("incremental" in fn or "batch" in fn) and "initial" not in fn else 0
        return (is_inc, fn)

    return sorted(found_files, key=_landing_sort_key)


def filter_pending_files(all_files: Sequence[str], ingested_files: Set[str]) -> List[str]:
    """
    Filtra la lista de archivos detectados, excluyendo aquellos cuyo path completo
    o basename ya figure en el registro de ingesta previa exitosa.
    """
    pending: List[str] = []
    for f in all_files:
        f_norm = str(f).replace("\\", "/")
        f_name = Path(f).name
        if f_norm not in ingested_files and f_name not in ingested_files:
            pending.append(f_norm)
        else:
            logger.info('{"event": "file_skipped_idempotent", "file": "%s"}', f_name)
    return pending


def record_ingestion_log(
    spark: Any,
    catalog_name: str,
    batch_id: str,
    source_file: str,
    entity: str,
    rows_read: int,
    rows_ingested: int,
    status: str,
    error_message: Optional[str] = None,
    schema_name: str = "chubb_lakehouse",
) -> None:
    """
    Registra atómicamente el evento de ingesta en la tabla Delta de control bronze_ingestion_log.
    """
    from pyspark.sql.types import (
        LongType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    log_table = f"{catalog_name}.{schema_name}.bronze_ingestion_log"
    now_utc = datetime.datetime.now(datetime.timezone.utc)

    schema = StructType([
        StructField("batch_id", StringType(), False),
        StructField("source_file", StringType(), False),
        StructField("entity", StringType(), False),
        StructField("rows_read", LongType(), False),
        StructField("rows_ingested", LongType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
        StructField("ingested_at", TimestampType(), False),
    ])

    data = [(
        batch_id,
        source_file,
        entity,
        int(rows_read),
        int(rows_ingested),
        status,
        error_message,
        now_utc,
    )]

    try:
        df_log_entry = spark.createDataFrame(data, schema=schema)
        df_log_entry.write.format("delta").mode("append").saveAsTable(log_table)
        logger.info(
            '{"event": "log_recorded", "batch_id": "%s", "file": "%s", "status": "%s", "rows": %d}',
            batch_id,
            Path(source_file).name,
            status,
            rows_ingested,
        )
    except Exception as exc:
        logger.error(
            '{"event": "log_record_failed", "table": "%s", "error": "%s"}',
            log_table,
            str(exc),
        )
        raise exc
