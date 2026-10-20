"""
==============================================================================
MÓDULO: src.silver.customers_scd2
PROPÓSITO:
Gestión de la dimensión histórica de clientes bajo SCD Tipo 2:
- Generación determinista de surrogate key:
  customer_sk = xxhash64(customer_id, effective_date) (BIGINT)
- Detección precisa de cambios en atributos clave (country, segment, email):
  change_hash = xxhash64(country, segment, email) (BIGINT)
- Enmascaramiento PII estricto (email_masked, phone_masked vía SHA-256)
- Control de vigencias temporales (effective_date, end_date, valid_from, valid_to, is_current)
- Cierre de registro anterior (is_current=false, end_date=source.updated_at)
- Inserción idempotente de nueva versión (is_current=true, effective_date=source.updated_at, end_date=NULL)
- Manejo de DELETE cerrando la vigencia del registro activo sin eliminar historial
==============================================================================
"""

import datetime
import hashlib
import logging
import struct
import sys
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("silver.customers_scd2")
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

try:
    from delta.tables import DeltaTable  # type: ignore
except ImportError:
    class _DeltaTableFallback:  # type: ignore
        @staticmethod
        def forName(spark: Any, table_name: str) -> Any:
            raise NotImplementedError("DeltaTable requires delta-spark or Databricks runtime")
    DeltaTable = _DeltaTableFallback  # type: ignore


def _hash_64_int(tokens: Sequence[str]) -> int:
    """Calcula hash signed int64 (BIGINT) determinista para simulación local."""
    payload = "||".join(str(t or "") for t in tokens)
    if _HAS_XXHASH:
        raw_val = xxhash.xxh64(payload.encode("utf-8")).intdigest()
        if raw_val >= (1 << 63):
            return raw_val - (1 << 64)
        return raw_val
    else:
        digest = hashlib.sha256(payload.encode("utf-8")).digest()
        (val,) = struct.unpack(">q", digest[:8])
        return val


def compute_customer_sk_py(customer_id: str, effective_date: str) -> int:
    """Calcula surrogate key determinista xxhash64 para cliente y fecha efectiva."""
    return _hash_64_int([customer_id, effective_date])


def compute_change_hash_py(country: str, segment: str, email: str) -> int:
    """Calcula change_hash determinista xxhash64 para detección de cambios en atributos SCD2."""
    return _hash_64_int([country, segment, email])


def mask_pii_py(val: Optional[str]) -> str:
    """Enmascara PII mediante hash SHA-256 en hexadecimal."""
    if not val:
        return ""
    return hashlib.sha256(val.strip().lower().encode("utf-8")).hexdigest()


def process_customers_scd2_records(
    current_dim: List[Dict[str, Any]],
    incoming_records: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Simula de forma determinista la lógica de SCD Tipo 2 en memoria:
    - Cierre de versiones activas cuando hay cambios o borrado lógico.
    - Apertura de nueva versión con is_current=True y end_date=None.
    - Conservación de todo el historial inmutable.
    """
    dim_records = [dict(r) for r in current_dim]
    now_iso = datetime.datetime.now(datetime.timezone.utc).isoformat()

    # Ordenar incoming_records cronológicamente por updated_at
    sorted_incoming = sorted(
        incoming_records,
        key=lambda x: str(x.get("updated_at", "") or ""),
    )

    for src in sorted_incoming:
        cid = str(src.get("customer_id", "")).strip()
        if not cid:
            continue

        tx_type = str(src.get("transaction_type", "INSERT")).upper()
        updated_at = str(src.get("updated_at", "") or now_iso)
        country = str(src.get("country", "") or "UNKNOWN")
        segment = str(src.get("segment", "") or "")
        email = str(src.get("email", "") or "")
        phone = str(src.get("phone", "") or "")

        src_change_hash = compute_change_hash_py(country, segment, email)

        # Buscar el registro activo actual en la dimensión
        active_idx = None
        for idx, r in enumerate(dim_records):
            if str(r.get("customer_id", "")).strip() == cid and r.get("is_current") is True:
                active_idx = idx
                break

        if active_idx is not None:
            active_rec = dim_records[active_idx]
            current_change_hash = active_rec.get("change_hash")
            if current_change_hash is None:
                current_change_hash = compute_change_hash_py(
                    str(active_rec.get("country", "") or "UNKNOWN"),
                    str(active_rec.get("segment", "") or ""),
                    str(active_rec.get("email", "") or ""),
                )

            active_effective_date = str(active_rec.get("effective_date", "") or "")

            if tx_type == "DELETE":
                # Invariante SCD2: Solo cerrar si la fecha entrante es estrictamente posterior
                if not active_effective_date or updated_at > active_effective_date:
                    active_rec["is_current"] = False
                    active_rec["end_date"] = updated_at
                    active_rec["valid_to"] = updated_at
                    active_rec["_updated_at"] = now_iso
            elif src_change_hash != current_change_hash:
                # Invariante SCD2: Solo cerrar e insertar nueva versión si la fecha entrante es estrictamente posterior
                if not active_effective_date or updated_at > active_effective_date:
                    active_rec["is_current"] = False
                    active_rec["end_date"] = updated_at
                    active_rec["valid_to"] = updated_at
                    active_rec["_updated_at"] = now_iso

                    # Creación de nueva versión SCD2
                    new_sk = compute_customer_sk_py(cid, updated_at)
                    new_version: Dict[str, Any] = {
                        "customer_sk": new_sk,
                        "customer_id": cid,
                        "first_name": src.get("first_name", active_rec.get("first_name")),
                        "last_name": src.get("last_name", active_rec.get("last_name")),
                        "email": email,
                        "email_masked": mask_pii_py(email),
                        "phone": phone,
                        "phone_masked": mask_pii_py(phone),
                        "country": country,
                        "segment": segment,
                        "address": src.get("address", active_rec.get("address")),
                        "effective_date": updated_at,
                        "end_date": None,
                        "valid_from": updated_at,
                        "valid_to": "9999-12-31 23:59:59",
                        "is_current": True,
                        "change_hash": src_change_hash,
                        "_record_hash": src.get("_record_hash", src_change_hash),
                        "_batch_id": src.get("_batch_id", "batch_silver"),
                        "_updated_at": now_iso,
                    }
                    dim_records.append(new_version)
        else:
            if tx_type != "DELETE":
                new_sk = compute_customer_sk_py(cid, updated_at)
                new_rec: Dict[str, Any] = {
                    "customer_sk": new_sk,
                    "customer_id": cid,
                    "first_name": src.get("first_name", ""),
                    "last_name": src.get("last_name", ""),
                    "email": email,
                    "email_masked": mask_pii_py(email),
                    "phone": phone,
                    "phone_masked": mask_pii_py(phone),
                    "country": country,
                    "segment": segment,
                    "address": src.get("address", ""),
                    "effective_date": updated_at,
                    "end_date": None,
                    "valid_from": updated_at,
                    "valid_to": "9999-12-31 23:59:59",
                    "is_current": True,
                    "change_hash": src_change_hash,
                    "_record_hash": src.get("_record_hash", src_change_hash),
                    "_batch_id": src.get("_batch_id", "batch_silver"),
                    "_updated_at": now_iso,
                }
                dim_records.append(new_rec)

    return dim_records


def prepare_customers_staging_df(df_clean_customers: Any) -> Any:
    """
    Prepara el DataFrame de clientes limpios para SCD2:
    - Usa country directamente si viene en el DataFrame de Bronze (o asigna 'UNKNOWN' si falta o es nulo)
    - Usa name directamente si viene de Bronze o construye name a partir de first_name y last_name
    - Calcula change_hash = xxhash64(country, segment, email)
    - Inyecta marcas temporales y surrogate key determinista
    """
    from pyspark.sql import Window, functions as F

    cols = df_clean_customers.columns
    df_prep = df_clean_customers

    if "country" not in cols:
        df_prep = df_prep.withColumn("country", F.lit("UNKNOWN"))
    else:
        # Usar country directamente desde Bronze, con fallback preventivo ante nulos
        df_prep = df_prep.withColumn(
            "country",
            F.when(
                (F.col("country").isNull()) | (F.trim(F.col("country")) == ""),
                F.lit("UNKNOWN"),
            ).otherwise(F.col("country")),
        )

    if "name" not in cols:
        if "first_name" in cols and "last_name" in cols:
            df_prep = df_prep.withColumn(
                "name",
                F.trim(F.concat_ws(" ", F.coalesce(F.col("first_name"), F.lit("")), F.coalesce(F.col("last_name"), F.lit(""))))
            )
        elif "first_name" in cols:
            df_prep = df_prep.withColumn("name", F.col("first_name"))
        else:
            df_prep = df_prep.withColumn("name", F.lit(""))
    else:
        if "first_name" in cols and "last_name" in cols:
            df_prep = df_prep.withColumn(
                "name",
                F.when(
                    (F.col("name").isNull()) | (F.trim(F.col("name")) == ""),
                    F.trim(F.concat_ws(" ", F.coalesce(F.col("first_name"), F.lit("")), F.coalesce(F.col("last_name"), F.lit("")))),
                ).otherwise(F.col("name")),
            )

    if "email" not in cols:
        df_prep = df_prep.withColumn("email", F.lit(""))

    if "segment" not in cols:
        df_prep = df_prep.withColumn("segment", F.lit("STANDARD"))

    if "_source_file" not in cols:
        df_prep = df_prep.withColumn("_source_file", F.lit("unknown"))

    if "_batch_id" not in cols:
        df_prep = df_prep.withColumn("_batch_id", F.lit("unknown"))

    if "transaction_type" not in cols:
        df_prep = df_prep.withColumn("transaction_type", F.lit("INSERT"))

    # Deduplicación intra-batch por customer_id ordenada por updated_at DESC
    window_spec = Window.partitionBy("customer_id").orderBy(
        F.col("updated_at").desc_nulls_last()
    )
    df_prep = (
        df_prep.withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Cálculo determinista de change_hash con xxhash64
    change_hash_expr = F.xxhash64(
        F.coalesce(F.col("country"), F.lit("")),
        F.coalesce(F.col("segment"), F.lit("")),
        F.coalesce(F.col("email"), F.lit("")),
    )

    effective_ts = F.coalesce(F.expr("try_to_timestamp(updated_at)"), F.current_timestamp())
    sk_expr = F.xxhash64(F.col("customer_id"), effective_ts.cast("string"))

    df_out = (
        df_prep
        .withColumn("change_hash", change_hash_expr)
        .withColumn("customer_sk", sk_expr)
        .withColumn("updated_at", F.col("updated_at").cast("string"))
        .withColumn("effective_date", effective_ts)
        .withColumn("end_date", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .withColumn("_source_file", F.col("_source_file").cast("string"))
        .withColumn("_batch_id", F.col("_batch_id").cast("string"))
    )

    return df_out


def merge_customers_scd2(
    spark: Any,
    df_clean_customers: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Ejecuta el pipeline de SCD Tipo 2 en Delta Lake para la dimensión de clientes.
    Target pre-creado y gobernado estrictamente por DDL: {catalog}.{schema}.customers_silver
    """
    from pyspark.sql import functions as F

    cat = catalog_name if catalog_name is not None else catalog
    target_table_name = f"{cat}.{schema}.customers_silver"
    df_staging = prepare_customers_staging_df(df_clean_customers)

    logger.info(
        '{"event": "start_customers_scd2_merge", "target": "%s"}',
        target_table_name,
    )

    # Resolución directa gobernada por DDL (Cero auto-creación ni saveAsTable condicional)
    delta_target = DeltaTable.forName(spark, target_table_name)
    df_target_active = delta_target.toDF().filter("is_current = true")

    target_with_hash = df_target_active.select(
        F.col("customer_id"),
        F.col("effective_date").alias("tgt_effective_date"),
        F.expr("xxhash64(coalesce(country, ''), coalesce(segment, ''), coalesce(email, ''))").alias("tgt_change_hash"),
    )

    # Identificar cambios vs registros activos
    joined_staging = df_staging.alias("stg").join(
        target_with_hash.alias("tgt"),
        F.expr("stg.customer_id = tgt.customer_id"),
        "left",
    )

    # Registros para cerrar versión previa (cambio de atributos o DELETE)
    # Invariante SCD2: Solo cerrar si la fecha entrante es estrictamente posterior al registro activo
    rows_to_close = joined_staging.filter(
        """
        tgt.customer_id IS NOT NULL 
        AND stg.effective_date > tgt.tgt_effective_date
        AND (stg.transaction_type = 'DELETE' OR stg.change_hash != tgt.tgt_change_hash)
        """
    ).select(
        F.expr("stg.customer_id").alias("merge_key"),
        F.expr("stg.effective_date").alias("close_timestamp"),
    )

    # 1. Cierre de registros existentes en la tabla Delta
    (
        delta_target.alias("target")
        .merge(
            rows_to_close.alias("close_stg"),
            "target.customer_id = close_stg.merge_key AND target.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "is_current": "false",
                "end_date": "close_stg.close_timestamp",
            }
        )
        .execute()
    )

    # 2. Inserción de nuevas versiones activas (excluyendo DELETE y registros sin cambios)
    # Invariante SCD2: Solo insertar si es cliente nuevo o si la fecha entrante es estrictamente posterior con cambio de hash
    rows_to_insert = joined_staging.filter(
        """
        stg.transaction_type != 'DELETE' 
        AND (
            tgt.customer_id IS NULL 
            OR (stg.effective_date > tgt.tgt_effective_date AND stg.change_hash != tgt.tgt_change_hash)
        )
        """
    ).select(
        F.expr("stg.customer_sk").alias("customer_sk"),
        F.expr("stg.customer_id").alias("customer_id"),
        F.expr("stg.name").alias("name"),
        F.expr("stg.email").alias("email"),
        F.expr("stg.country").alias("country"),
        F.expr("stg.segment").alias("segment"),
        F.expr("stg.updated_at").alias("updated_at"),
        F.expr("stg.effective_date").alias("effective_date"),
        F.expr("stg.end_date").alias("end_date"),
        F.expr("stg.is_current").alias("is_current"),
        F.expr("stg._batch_id").alias("_batch_id"),
        F.expr("stg._source_file").alias("_source_file"),
    )

    (
        delta_target.alias("target")
        .merge(
            rows_to_insert.alias("src"),
            "target.customer_sk = src.customer_sk",
        )
        .whenNotMatchedInsert(
            values={
                "customer_sk": "src.customer_sk",
                "customer_id": "src.customer_id",
                "name": "src.name",
                "email": "src.email",
                "country": "src.country",
                "segment": "src.segment",
                "updated_at": "src.updated_at",
                "effective_date": "src.effective_date",
                "end_date": "src.end_date",
                "is_current": "src.is_current",
                "_batch_id": "src._batch_id",
                "_source_file": "src._source_file",
            }
        )
        .execute()
    )

    logger.info(
        '{"event": "customers_scd2_merge_complete", "target": "%s"}',
        target_table_name,
    )

    return {
        "target_table": target_table_name,
        "status": "SUCCESS",
    }


def process_customers_silver(
    spark: Any,
    catalog: str,
    schema: str = "chubb_lakehouse",
    catalog_name: Optional[str] = None,
    batch_filter: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Orquesta de punta a punta la promoción de Customers desde Bronze a Silver SCD2:
    1. Lectura de customers_bronze desde chubb_lakehouse
    2. Data Quality Gate y desvío a Cuarentena
    3. Merge atómico SCD2 en customers_silver
    """
    from src.utils.data_quality import apply_customers_dq
    from src.utils.quarantine import route_quarantine

    cat = catalog_name if catalog_name is not None else catalog

    raw_cust_table = f"{cat}.{schema}.customers_bronze"
    try:
        df_raw = spark.table(raw_cust_table)
    except Exception:
        try:
            raw_cust_table = f"{cat}.{schema}.raw_customers"
            df_raw = spark.table(raw_cust_table)
        except Exception:
            raw_cust_table = f"{cat}.bronze.raw_customers"
            df_raw = spark.table(raw_cust_table)

    logger.info('{"event": "process_customers_silver_start", "table": "%s"}', raw_cust_table)

    if batch_filter:
        batch_ids = [batch_filter]
    else:
        # Orden cronológico determinista: lotes 'initial' primero, seguidos de incrementales ordenados por updated_at
        from pyspark.sql import functions as F
        batch_df = (
            df_raw.groupBy("_batch_id")
            .agg(
                F.min("updated_at").alias("min_updated_at"),
                F.min("_ingestion_ts").alias("min_ingestion_ts"),
            )
            .withColumn(
                "_is_incremental",
                F.when(F.col("_batch_id").rlike("(?i)init"), F.lit(0)).otherwise(F.lit(1))
            )
            .orderBy("_is_incremental", "min_updated_at", "min_ingestion_ts", "_batch_id")
        )
        batch_ids = [row["_batch_id"] for row in batch_df.collect() if row["_batch_id"]]

    if not batch_ids:
        batch_ids = ["batch_silver"]

    total_valid = 0
    total_quar = 0
    last_merge_res = None

    for b_id in batch_ids:
        logger.info('{"event": "process_customers_batch_start", "batch_id": "%s"}', b_id)
        df_batch = df_raw.filter(f"_batch_id = '{b_id}'") if "_batch_id" in df_raw.columns else df_raw
        df_dq = apply_customers_dq(df_batch)
        df_clean, valid_cnt, quar_cnt = route_quarantine(
            spark=spark,
            df_evaluated=df_dq,
            entity="customers",
            catalog=cat,
            schema=schema,
            default_batch_id=b_id,
        )
        total_valid += valid_cnt
        total_quar += quar_cnt

        if valid_cnt > 0:
            last_merge_res = merge_customers_scd2(
                spark=spark,
                df_clean_customers=df_clean,
                catalog=cat,
                schema=schema,
            )

    return {
        "catalog": cat,
        "schema": schema,
        "valid_promoted": total_valid,
        "quarantined": total_quar,
        "merge_result": last_merge_res,
        "status": "SUCCESS",
    }

