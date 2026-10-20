"""
==============================================================================
MÓDULO: src.utils.data_quality
PROPÓSITO:
Motor de reglas de calidad de datos (Data Quality Gate) para la Capa Silver:
- Detección estricta de nulos en claves primarias (customer_id, order_id)
- Detección de nulos o montos no numéricos en total_amount
- Validación de montos positivos (total_amount > 0)
- Parseo y validez de marcas temporales y fechas de calendario
- Validación de integridad referencial (customer_id de órdenes en clientes)
- Clasificación tipificada de errores y causas de rechazo (ERR_*)
==============================================================================
"""

import datetime
import logging
import re
import sys
from typing import Any, Dict, List, Optional, Set, Tuple

# Configuración de Structured Logging (Google SRE / Zero Orphan print)
logger = logging.getLogger("utils.data_quality")
if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Códigos de error tipificados para Data Quality Gate
ERR_NULL_PK: str = "ERR_NULL_PK"
ERR_NULL_AMOUNT: str = "ERR_NULL_AMOUNT"
ERR_NEGATIVE_AMOUNT: str = "ERR_NEGATIVE_AMOUNT"
ERR_INVALID_DATE: str = "ERR_INVALID_DATE"
ERR_ORPHAN_CUSTOMER: str = "ERR_ORPHAN_CUSTOMER"


def _is_blank(val: Any) -> bool:
    """Verifica si un valor es nulo, None o cadena vacía tras strip."""
    if val is None:
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    return False


def _parse_iso_date_or_ts(date_str: Any) -> bool:
    """Valida si un valor puede ser interpretado como fecha o timestamp ISO válido."""
    if _is_blank(date_str):
        return False
    str_val = str(date_str).strip()
    patterns = [
        r"^\d{4}-\d{2}-\d{2}$",
        r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:?\d{2})?$",
    ]
    if not any(re.match(pat, str_val) for pat in patterns):
        return False

    formats = [
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    # Limpiar sufijo de zona horaria si existe para evaluación de parseo
    clean_val = re.sub(r"(Z|[+-]\d{2}:?\d{2})$", "", str_val)
    for fmt in formats:
        try:
            datetime.datetime.strptime(clean_val, fmt)
            return True
        except ValueError:
            continue
    return False


def validate_customer_record(record: Dict[str, Any]) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Evalúa las reglas de calidad para un registro individual de cliente.
    Retorna: (is_valid, error_code, rejection_reason)
    """
    customer_id = record.get("customer_id")
    if _is_blank(customer_id):
        return False, ERR_NULL_PK, "Primary key 'customer_id' is null or empty"

    updated_at = record.get("updated_at")
    if not _is_blank(updated_at) and not _parse_iso_date_or_ts(updated_at):
        return False, ERR_INVALID_DATE, f"Timestamp 'updated_at' is invalid: {updated_at}"

    return True, None, None


def validate_order_record(
    record: Dict[str, Any],
    valid_customer_ids: Optional[Set[str]] = None,
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Evalúa las reglas de calidad para un registro individual de orden transaccional.
    Retorna: (is_valid, error_code, rejection_reason)
    """
    order_id = record.get("order_id")
    if _is_blank(order_id):
        return False, ERR_NULL_PK, "Primary key 'order_id' is null or empty"

    customer_id = record.get("customer_id")
    if _is_blank(customer_id):
        return False, ERR_NULL_PK, "Foreign key 'customer_id' is null or empty"

    total_amount = record.get("total_amount")
    if _is_blank(total_amount):
        return False, ERR_NULL_AMOUNT, "Monto 'total_amount' is null or empty"

    try:
        amount_num = float(str(total_amount).strip())
        if amount_num <= 0.0:
            return False, ERR_NEGATIVE_AMOUNT, f"Amount must be strictly positive (> 0), got: {amount_num}"
    except (ValueError, TypeError):
        return False, ERR_NULL_AMOUNT, f"Monto 'total_amount' is not a valid number: {total_amount}"

    order_date = record.get("order_date")
    if not _is_blank(order_date) and not _parse_iso_date_or_ts(order_date):
        return False, ERR_INVALID_DATE, f"Date 'order_date' is invalid: {order_date}"

    order_ts = record.get("order_timestamp")
    if not _is_blank(order_ts) and not _parse_iso_date_or_ts(order_ts):
        return False, ERR_INVALID_DATE, f"Timestamp 'order_timestamp' is invalid: {order_ts}"

    updated_at = record.get("updated_at")
    if not _is_blank(updated_at) and not _parse_iso_date_or_ts(updated_at):
        return False, ERR_INVALID_DATE, f"Timestamp 'updated_at' is invalid: {updated_at}"

    # Integridad referencial con universo de clientes
    if valid_customer_ids is not None and str(customer_id).strip() not in valid_customer_ids:
        return False, ERR_ORPHAN_CUSTOMER, f"Referential integrity failure: customer_id '{customer_id}' not found"

    return True, None, None


def apply_customers_dq(df: Any) -> Any:
    """
    Aplica el Data Quality Gate sobre un DataFrame de PySpark para la entidad 'customers'.
    Inyecta columnas:
    - is_valid: BOOLEAN
    - error_code: STRING
    - rejection_reason: STRING
    """
    from pyspark.sql import functions as F

    null_pk_cond = F.col("customer_id").isNull() | (F.trim(F.col("customer_id")) == "")
    invalid_date_cond = (
        F.col("updated_at").isNotNull()
        & (F.trim(F.col("updated_at")) != "")
        & F.expr("try_to_timestamp(updated_at)").isNull()
    )

    error_code_expr = (
        F.when(null_pk_cond, F.lit(ERR_NULL_PK))
        .when(invalid_date_cond, F.lit(ERR_INVALID_DATE))
        .otherwise(F.lit(None).cast("string"))
    )

    rejection_reason_expr = (
        F.when(null_pk_cond, F.lit("Primary key 'customer_id' is null or empty"))
        .when(invalid_date_cond, F.lit("Timestamp 'updated_at' is unparseable"))
        .otherwise(F.lit(None).cast("string"))
    )

    return (
        df.withColumn("error_code", error_code_expr)
        .withColumn("rejection_reason", rejection_reason_expr)
        .withColumn("is_valid", F.col("error_code").isNull())
    )


def apply_orders_dq(df_orders: Any, valid_customers_df: Optional[Any] = None) -> Any:
    """
    Aplica el Data Quality Gate sobre un DataFrame de PySpark para la entidad 'orders'.
    Verifica no nulos, montos positivos, fechas y la integridad referencial si valid_customers_df está provisto.
    Inyecta columnas:
    - is_valid: BOOLEAN
    - error_code: STRING
    - rejection_reason: STRING
    """
    from pyspark.sql import functions as F

    null_pk_cond = (
        F.col("order_id").isNull()
        | (F.trim(F.col("order_id")) == "")
        | F.col("customer_id").isNull()
        | (F.trim(F.col("customer_id")) == "")
    )

    amount_raw = F.trim(F.col("total_amount"))
    amount_col = F.expr("try_cast(total_amount as double)")
    null_amount_cond = F.col("total_amount").isNull() | (amount_raw == "") | amount_col.isNull()
    negative_amount_cond = (~null_amount_cond) & (amount_col <= 0.0)

    invalid_date_cond = (
        (
            F.col("order_date").isNotNull()
            & (F.trim(F.col("order_date")) != "")
            & F.expr("try_to_date(order_date)").isNull()
        )
        | (
            F.col("order_timestamp").isNotNull()
            & (F.trim(F.col("order_timestamp")) != "")
            & F.expr("try_to_timestamp(order_timestamp)").isNull()
        )
        | (
            F.col("updated_at").isNotNull()
            & (F.trim(F.col("updated_at")) != "")
            & F.expr("try_to_timestamp(updated_at)").isNull()
        )
    )

    # Evaluación base de reglas intrínsecas
    evaluated = df_orders.withColumn(
        "error_code",
        F.when(null_pk_cond, F.lit(ERR_NULL_PK))
        .when(null_amount_cond, F.lit(ERR_NULL_AMOUNT))
        .when(negative_amount_cond, F.lit(ERR_NEGATIVE_AMOUNT))
        .when(invalid_date_cond, F.lit(ERR_INVALID_DATE))
        .otherwise(F.lit(None).cast("string")),
    ).withColumn(
        "rejection_reason",
        F.when(null_pk_cond, F.lit("Primary or foreign key is null or empty"))
        .when(null_amount_cond, F.lit("Amount 'total_amount' is null, empty or non-numeric"))
        .when(negative_amount_cond, F.lit("Amount must be strictly positive (> 0)"))
        .when(invalid_date_cond, F.lit("Date or timestamp field has invalid format"))
        .otherwise(F.lit(None).cast("string")),
    )

    # Integridad referencial opcional si se provee universo de clientes
    if valid_customers_df is not None:
        customers_ref = valid_customers_df.select(
            F.col("customer_id").alias("_ref_customer_id")
        ).distinct()
        joined = evaluated.join(
            customers_ref,
            evaluated["customer_id"] == customers_ref["_ref_customer_id"],
            "left",
        )
        orphan_cond = F.col("error_code").isNull() & F.col("_ref_customer_id").isNull()
        evaluated = (
            joined.withColumn("_is_orphan", orphan_cond)
            .withColumn(
                "error_code",
                F.when(F.col("_is_orphan"), F.lit(ERR_ORPHAN_CUSTOMER)).otherwise(F.col("error_code")),
            )
            .withColumn(
                "rejection_reason",
                F.when(
                    F.col("_is_orphan"),
                    F.lit("Referential integrity failure: customer_id does not exist in customer universe"),
                ).otherwise(F.col("rejection_reason")),
            )
            .drop("_ref_customer_id", "_is_orphan")
        )

    return evaluated.withColumn("is_valid", F.col("error_code").isNull())
