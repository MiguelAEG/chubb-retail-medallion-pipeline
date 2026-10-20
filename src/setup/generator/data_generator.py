"""
==============================================================================
MÓDULO: src.setup.generator.data_generator
PROPÓSITO:
Generador determinista de datos sintéticos para las entidades 'customers' y 'orders'
conforme a los contratos DC-2026-CUSTOMERS y DC-2026-ORDERS.

Inyecta de forma controlada y auditable los 6 defectos de calidad de datos
requeridos para la validación del Data Quality Gate y Cuarentena:
1. ~2% de nulos en claves primarias / campos críticos (customer_id o total_amount).
2. ~2% de nulos en campos opcionales (email o segment).
3. Duplicados exactos intra-batch (mismo contenido en el mismo archivo).
4. Duplicados inter-batch con diferente updated_at (actualizaciones temporales).
5. Huérfanos (órdenes con customer_id inexistente).
6. Valores inválidos (montos negativos < 0 y marcas temporales fuera de formato).

Soporta rutas del sistema de archivos local y Databricks Unity Catalog Volumes.
==============================================================================
"""

import argparse
import csv
import datetime
import logging
from pathlib import Path
import random
from typing import Any, Dict, List, Optional, Tuple, Union

# Configuración de Logging Estructurado (Google SRE / Zero Orphan print)
logger = logging.getLogger("data_generator")
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        '{"timestamp": "%(asctime)s", "level": "%(levelname)s", "logger": "%(name)s", "message": "%(message)s"}'
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# Encabezados exactos según contratos de interfaz de datos crudos
CUSTOMERS_HEADERS: List[str] = [
    "customer_id",
    "name",
    "first_name",
    "last_name",
    "email",
    "phone",
    "country",
    "segment",
    "address",
    "updated_at",
    "transaction_type",
]

ORDERS_HEADERS: List[str] = [
    "order_id",
    "customer_id",
    "order_date",
    "order_timestamp",
    "order_status",
    "total_amount",
    "currency",
    "updated_at",
    "transaction_type",
]

FIRST_NAMES = [
    "Carlos", "Maria", "Alejandro", "Sofia", "Diego", "Valentina", "Juan", "Camila",
    "Mateo", "Isabella", "Andres", "Lucia", "Javier", "Daniela", "Fernando", "Elena",
    "Gabriel", "Paula", "Ricardo", "Mariana", "Sebastian", "Gabriela", "Lucas", "Victoria",
    "Emilio", "Renata", "Martin", "Catalina", "Nicolas", "Florencia",
]

LAST_NAMES = [
    "Gonzalez", "Rodriguez", "Hernandez", "Lopez", "Martinez", "Perez", "Garcia", "Sanchez",
    "Ramirez", "Torres", "Flores", "Diaz", "Vargas", "Castillo", "Morales", "Romero",
    "Alvarez", "Mendoza", "Rojas", "Silva", "Medina", "Vega", "Castro", "Ortiz", "Rubio",
]

SEGMENTS = ["lifestyle", "electronics", "miscellaneous", "CPG"]
COUNTRIES = ["Mexico", "Chile", "Colombia", "Brazil", "Argentina", "Panama"]
CURRENCIES = ["USD", "MXN", "CLP", "EUR"]
ORDER_STATUSES = ["COMPLETED", "PENDING", "CANCELLED", "REFUNDED"]
TRANSACTION_TYPES = ["INSERT", "UPDATE", "DELETE"]

CITIES = [
    "Ciudad de Mexico", "Bogota", "Santiago", "Buenos Aires", "Lima",
    "Sao Paulo", "Monterrey", "Medellin", "Guadalajara", "Montevideo",
]


def _format_iso(dt: datetime.datetime) -> str:
    """Formatea datetime en estándar ISO-8601 UTC."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _format_date(dt: datetime.datetime) -> str:
    """Formatea fecha en formato YYYY-MM-DD."""
    return dt.strftime("%Y-%m-%d")


def generate_customer_pool(
    total_customers: int = 5000,
    base_date: Optional[datetime.datetime] = None,
) -> Tuple[List[Dict[str, str]], List[str]]:
    """
    Genera el universo de clientes base para la partición física y generación de órdenes.
    Retorna la lista de diccionarios de clientes y la lista de customer_ids válidos.
    """
    if base_date is None:
        base_date = datetime.datetime(2026, 1, 15, 8, 0, 0)

    customers: List[Dict[str, str]] = []
    valid_customer_ids: List[str] = []

    for i in range(1, total_customers + 1):
        cid = f"CUST-{i:05d}"
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        email = f"{fname.lower()}.{lname.lower()}{i}@example.com"
        phone = f"+5255{random.randint(10000000, 99999999)}"
        segment = random.choice(SEGMENTS)
        city = random.choice(CITIES)
        address = f"Calle {random.randint(1, 200)} #{random.randint(10, 99)}, {city}"
        delta_days = random.randint(0, 30)
        delta_sec = random.randint(0, 86400)
        updated_at = _format_iso(base_date + datetime.timedelta(days=delta_days, seconds=delta_sec))

        record = {
            "customer_id": cid,
            "name": f"{fname} {lname}",
            "first_name": fname,
            "last_name": lname,
            "email": email,
            "phone": phone,
            "country": random.choice(COUNTRIES),
            "segment": segment,
            "address": address,
            "updated_at": updated_at,
            "transaction_type": "INSERT",
        }
        customers.append(record)
        valid_customer_ids.append(cid)

    return customers, valid_customer_ids


def inject_customer_defects(
    records: List[Dict[str, str]],
    is_initial: bool = True,
) -> List[Dict[str, str]]:
    """
    Aplica inyección de defectos controlados sobre un lote de clientes:
    - ~2% nulos en PK (customer_id = "")
    - ~2% nulos en opcionales (email = "" o segment = "")
    - Fechas malformadas en ~1%
    - Duplicados exactos intra-batch (~1%)
    """
    modified = [dict(r) for r in records]
    n = len(modified)
    if n == 0:
        return modified

    # Defecto 1: ~2% nulos en PK (customer_id nulo/vacío)
    num_null_pk = max(1, int(n * 0.02))
    null_pk_indices = random.sample(range(n), num_null_pk)
    for idx in null_pk_indices:
        modified[idx]["customer_id"] = ""

    # Defecto 2: ~2% nulos en opcionales (email o segment)
    num_null_opt = max(1, int(n * 0.02))
    null_opt_indices = random.sample([i for i in range(n) if i not in null_pk_indices], num_null_opt)
    for i, idx in enumerate(null_opt_indices):
        if i % 2 == 0:
            modified[idx]["email"] = ""
        else:
            modified[idx]["segment"] = ""

    # Defecto 6: Fechas malformadas en ~1%
    num_bad_dates = max(1, int(n * 0.01))
    bad_date_indices = random.sample(
        [i for i in range(n) if i not in null_pk_indices and i not in null_opt_indices],
        num_bad_dates,
    )
    for idx in bad_date_indices:
        modified[idx]["updated_at"] = "2026-99-99 99:99:99"

    # Defecto 3: Duplicados exactos intra-batch (~1%)
    num_exact_dups = max(1, int(n * 0.01))
    source_dups = random.sample(
        [i for i in range(n) if i not in null_pk_indices],
        num_exact_dups,
    )
    duplicates = [dict(modified[i]) for i in source_dups]
    modified.extend(duplicates)

    return modified


def generate_incremental_customers(
    base_customers: List[Dict[str, str]],
    batch_size: int = 500,
    base_date: Optional[datetime.datetime] = None,
) -> List[Dict[str, str]]:
    """
    Genera un lote incremental de clientes con transacciones INSERT, UPDATE y DELETE,
    incluyendo duplicados inter-batch con diferente updated_at.
    """
    if base_date is None:
        base_date = datetime.datetime(2026, 2, 1, 10, 0, 0)

    records: List[Dict[str, str]] = []
    # 1. UPDATEs sobre clientes existentes (cambio de dirección/segmento y nuevo updated_at)
    num_updates = int(batch_size * 0.5)
    candidates = random.sample(base_customers[:2000], min(num_updates, len(base_customers)))
    for cand in candidates:
        new_record = dict(cand)
        new_record["transaction_type"] = "UPDATE"
        new_record["address"] = f"Avenida Siempre Viva #{random.randint(100, 999)}, {random.choice(CITIES)}"
        new_record["segment"] = random.choice(SEGMENTS)
        new_record["updated_at"] = _format_iso(base_date + datetime.timedelta(days=random.randint(5, 20)))
        if "name" not in new_record:
            new_record["name"] = f"{new_record.get('first_name', '')} {new_record.get('last_name', '')}".strip()
        if "country" not in new_record:
            new_record["country"] = random.choice(COUNTRIES)
        records.append(new_record)

    # 2. INSERTs de nuevos clientes (nuevos IDs)
    num_inserts = int(batch_size * 0.4)
    start_id = len(base_customers) + 1
    for i in range(start_id, start_id + num_inserts):
        cid = f"CUST-{i:05d}"
        fname = random.choice(FIRST_NAMES)
        lname = random.choice(LAST_NAMES)
        records.append({
            "customer_id": cid,
            "name": f"{fname} {lname}",
            "first_name": fname,
            "last_name": lname,
            "email": f"{fname.lower()}.{lname.lower()}{i}@example.com",
            "phone": f"+5255{random.randint(10000000, 99999999)}",
            "country": random.choice(COUNTRIES),
            "segment": random.choice(SEGMENTS),
            "address": f"Boulevard Central #{random.randint(10, 500)}, {random.choice(CITIES)}",
            "updated_at": _format_iso(base_date + datetime.timedelta(days=random.randint(1, 10))),
            "transaction_type": "INSERT",
        })

    # 3. DELETEs (bajas lógicas)
    num_deletes = batch_size - num_updates - num_inserts
    delete_candidates = random.sample(base_customers[2000:3500], min(num_deletes, 1500))
    for cand in delete_candidates:
        del_record = dict(cand)
        del_record["transaction_type"] = "DELETE"
        del_record["updated_at"] = _format_iso(base_date + datetime.timedelta(days=random.randint(21, 28)))
        if "name" not in del_record:
            del_record["name"] = f"{del_record.get('first_name', '')} {del_record.get('last_name', '')}".strip()
        if "country" not in del_record:
            del_record["country"] = random.choice(COUNTRIES)
        records.append(del_record)

    return records


def generate_orders_batch(
    valid_customer_ids: List[str],
    start_order_id: int,
    count: int,
    base_date: datetime.datetime,
    transaction_type: str = "INSERT",
    inject_defects: bool = True,
) -> Tuple[List[Dict[str, str]], int]:
    """
    Genera un lote de órdenes con campos conformes a DC-2026-ORDERS e inyección
    de los 6 defectos según aplique.
    """
    orders: List[Dict[str, str]] = []

    for i in range(count):
        order_idx = start_order_id + i
        oid = f"ORD-{order_idx:06d}"
        cid = random.choice(valid_customer_ids) if valid_customer_ids else f"CUST-{random.randint(1, 5000):05d}"
        order_dt = base_date + datetime.timedelta(
            days=random.randint(0, 15),
            seconds=random.randint(0, 86400),
        )
        status = random.choice(ORDER_STATUSES)
        amount = round(random.uniform(10.0, 3500.0), 2)
        curr = random.choice(CURRENCIES)

        record = {
            "order_id": oid,
            "customer_id": cid,
            "order_date": _format_date(order_dt),
            "order_timestamp": _format_iso(order_dt),
            "order_status": status,
            "total_amount": f"{amount:.2f}",
            "currency": curr,
            "updated_at": _format_iso(order_dt),
            "transaction_type": transaction_type,
        }
        orders.append(record)

    if not inject_defects:
        return orders, start_order_id + count

    n = len(orders)
    # Defecto 1: ~2% nulos en llaves o montos críticos (order_id, customer_id o total_amount)
    num_null_critical = max(1, int(n * 0.02))
    null_crit_indices = random.sample(range(n), num_null_critical)
    for idx_num, idx in enumerate(null_crit_indices):
        mod = idx_num % 3
        if mod == 0:
            orders[idx]["order_id"] = ""
        elif mod == 1:
            orders[idx]["customer_id"] = ""
        else:
            orders[idx]["total_amount"] = ""

    # Defecto 5: ~2% órdenes huérfanas (customer_id inexistente)
    available_indices = [i for i in range(n) if i not in null_crit_indices]
    num_orphans = max(1, int(n * 0.02))
    orphan_indices = random.sample(available_indices, num_orphans)
    for i, idx in enumerate(orphan_indices):
        orders[idx]["customer_id"] = f"CUST-ORPHAN-{i + 1:04d}"

    # Defecto 6: Valores inválidos (montos negativos < 0 y fechas fuera de formato)
    available_indices2 = [i for i in available_indices if i not in orphan_indices]
    num_neg_amounts = max(1, int(n * 0.02))
    neg_indices = random.sample(available_indices2, num_neg_amounts)
    for idx in neg_indices:
        neg_val = -round(random.uniform(15.0, 850.0), 2)
        orders[idx]["total_amount"] = f"{neg_val:.2f}"

    available_indices3 = [i for i in available_indices2 if i not in neg_indices]
    num_bad_dates = max(1, int(n * 0.01))
    bad_date_indices = random.sample(available_indices3, num_bad_dates)
    for idx in bad_date_indices:
        orders[idx]["order_date"] = "2026/13/45"
        orders[idx]["order_timestamp"] = "bad_timestamp_format"

    # Defecto 3: Duplicados exactos intra-batch (~1%)
    num_dups = max(1, int(n * 0.01))
    dup_candidates = random.sample([i for i in available_indices3 if i not in bad_date_indices], num_dups)
    for idx in dup_candidates:
        orders.append(dict(orders[idx]))

    return orders, start_order_id + count


def generate_incremental_orders_batch(
    existing_orders: List[Dict[str, str]],
    valid_customer_ids: List[str],
    start_order_id: int,
    count: int,
    base_date: datetime.datetime,
) -> Tuple[List[Dict[str, str]], int]:
    """
    Genera un lote incremental de órdenes que combina:
    - UPDATEs de órdenes existentes (Defecto 4: duplicados inter-batch con diferente updated_at y status actualizado).
    - INSERTs de nuevas órdenes transaccionales.
    - DELETEs o cancelaciones explícitas.
    """
    records: List[Dict[str, str]] = []

    # 1. UPDATEs: órdenes previas que cambiaron de PENDING a COMPLETED o REFUNDED
    num_updates = int(count * 0.35)
    candidates = random.sample(existing_orders, min(num_updates, len(existing_orders)))
    for cand in candidates:
        rec = dict(cand)
        rec["transaction_type"] = "UPDATE"
        rec["order_status"] = "COMPLETED" if rec["order_status"] == "PENDING" else "REFUNDED"
        rec["updated_at"] = _format_iso(base_date + datetime.timedelta(days=random.randint(1, 5)))
        records.append(rec)

    # 2. INSERTs de nuevas órdenes
    num_inserts = count - num_updates
    new_orders, next_id = generate_orders_batch(
        valid_customer_ids=valid_customer_ids,
        start_order_id=start_order_id,
        count=num_inserts,
        base_date=base_date,
        transaction_type="INSERT",
        inject_defects=True,
    )
    records.extend(new_orders)

    return records, next_id


def write_csv_file(target_path: Path, headers: List[str], rows: List[Dict[str, str]]) -> int:
    """Escribe un archivo CSV de forma determinista y segura."""
    target_path.parent.mkdir(parents=True, exist_ok=True)
    with open(target_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    logger.info(
        '{"event": "file_written", "path": "%s", "rows_count": %d}',
        str(target_path).replace("\\", "/"),
        len(rows),
    )
    return len(rows)


def generate_all_datasets(
    output_dir: Union[str, Path] = "data/raw",
    seed: int = 42,
    num_customers: int = 5000,
    num_orders: int = 10000,
) -> Dict[str, Any]:
    """
    Orquestador principal de generación de datasets sintéticos crudos.
    Crea las particiones físicas requeridas tanto para clientes como para órdenes.
    """
    random.seed(seed)
    base_path = Path(output_dir)
    logger.info(
        '{"event": "start_generation", "output_dir": "%s", "seed": %d, "customers": %d, "orders": %d}',
        str(base_path).replace("\\", "/"),
        seed,
        num_customers,
        num_orders,
    )

    customers_dir = base_path / "customers"
    orders_dir = base_path / "orders"

    summary: Dict[str, Any] = {
        "output_dir": str(base_path).replace("\\", "/"),
        "seed": seed,
        "files_generated": {},
        "total_records_written": 0,
    }

    # ---------------------------------------------------------
    # 1. Generación de Clientes (~5,000 base + incremental)
    # ---------------------------------------------------------
    all_base_customers, valid_customer_ids = generate_customer_pool(
        total_customers=num_customers,
        base_date=datetime.datetime(2026, 1, 1, 8, 0, 0),
    )

    half_cust = num_customers // 2
    cust_part1_raw = all_base_customers[:half_cust]
    cust_part2_raw = all_base_customers[half_cust:]

    # Inyección de defectos controlados en las particiones iniciales
    cust_part1 = inject_customer_defects(cust_part1_raw, is_initial=True)
    cust_part2 = inject_customer_defects(cust_part2_raw, is_initial=True)

    # Lote incremental con INSERT/UPDATE/DELETE
    cust_incremental = generate_incremental_customers(
        base_customers=all_base_customers,
        batch_size=max(500, int(num_customers * 0.1)),
        base_date=datetime.datetime(2026, 2, 1, 10, 0, 0),
    )
    cust_incremental = inject_customer_defects(cust_incremental, is_initial=False)

    f_cust_p1 = customers_dir / "customers_initial_part1.csv"
    f_cust_p2 = customers_dir / "customers_initial_part2.csv"
    f_cust_inc = customers_dir / "customers_incremental_batch1.csv"

    r1 = write_csv_file(f_cust_p1, CUSTOMERS_HEADERS, cust_part1)
    r2 = write_csv_file(f_cust_p2, CUSTOMERS_HEADERS, cust_part2)
    r3 = write_csv_file(f_cust_inc, CUSTOMERS_HEADERS, cust_incremental)

    summary["files_generated"]["customers_initial_part1"] = {"path": str(f_cust_p1), "rows": r1}
    summary["files_generated"]["customers_initial_part2"] = {"path": str(f_cust_p2), "rows": r2}
    summary["files_generated"]["customers_incremental_batch1"] = {"path": str(f_cust_inc), "rows": r3}

    # ---------------------------------------------------------
    # 2. Generación de Órdenes (~10,000 iniciales + 3 incrementales)
    # ---------------------------------------------------------
    half_orders = num_orders // 2
    order_id_seq = 1

    orders_part1, order_id_seq = generate_orders_batch(
        valid_customer_ids=valid_customer_ids,
        start_order_id=order_id_seq,
        count=half_orders,
        base_date=datetime.datetime(2026, 1, 10, 9, 0, 0),
        transaction_type="INSERT",
        inject_defects=True,
    )

    orders_part2, order_id_seq = generate_orders_batch(
        valid_customer_ids=valid_customer_ids,
        start_order_id=order_id_seq,
        count=half_orders,
        base_date=datetime.datetime(2026, 1, 20, 9, 0, 0),
        transaction_type="INSERT",
        inject_defects=True,
    )

    f_ord_p1 = orders_dir / "orders_initial_part1.csv"
    f_ord_p2 = orders_dir / "orders_initial_part2.csv"
    ro1 = write_csv_file(f_ord_p1, ORDERS_HEADERS, orders_part1)
    ro2 = write_csv_file(f_ord_p2, ORDERS_HEADERS, orders_part2)

    summary["files_generated"]["orders_initial_part1"] = {"path": str(f_ord_p1), "rows": ro1}
    summary["files_generated"]["orders_initial_part2"] = {"path": str(f_ord_p2), "rows": ro2}

    # 3 Batches incrementales de órdenes
    existing_all_orders = orders_part1 + orders_part2
    for batch_num in range(1, 4):
        batch_date = datetime.datetime(2026, 2, batch_num * 5, 12, 0, 0)
        inc_batch, order_id_seq = generate_incremental_orders_batch(
            existing_orders=existing_all_orders,
            valid_customer_ids=valid_customer_ids,
            start_order_id=order_id_seq,
            count=1000,
            base_date=batch_date,
        )
        f_ord_inc = orders_dir / f"orders_incremental_batch{batch_num}.csv"
        ro_inc = write_csv_file(f_ord_inc, ORDERS_HEADERS, inc_batch)
        summary["files_generated"][f"orders_incremental_batch{batch_num}"] = {
            "path": str(f_ord_inc),
            "rows": ro_inc,
        }

    total_rows = sum(item["rows"] for item in summary["files_generated"].values())
    summary["total_records_written"] = total_rows

    logger.info(
        '{"event": "generation_completed", "total_files": %d, "total_records": %d}',
        len(summary["files_generated"]),
        total_rows,
    )
    return summary


def main() -> None:
    """Punto de entrada de línea de comandos para el generador de datos."""
    parser = argparse.ArgumentParser(
        description="Generador determinista de datos sintéticos Medallion Lakehouse (Chubb Regional 2026)"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="data/raw",
        help="Directorio destino (local o ruta a Databricks Volume)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Semilla determinista de números aleatorios (default: 42)",
    )
    parser.add_argument(
        "--num-customers",
        type=int,
        default=5000,
        help="Cantidad base de clientes a generar (default: 5000)",
    )
    parser.add_argument(
        "--num-orders",
        type=int,
        default=10000,
        help="Cantidad base de órdenes iniciales a generar (default: 10000)",
    )

    args = parser.parse_args()
    summary = generate_all_datasets(
        output_dir=args.output_dir,
        seed=args.seed,
        num_customers=args.num_customers,
        num_orders=args.num_orders,
    )
    logger.info(
        '{"event": "cli_execution_finished", "output_dir": "%s", "status": "SUCCESS"}',
        summary["output_dir"],
    )


generate_all_data = generate_all_datasets


if __name__ == "__main__":
    main()
