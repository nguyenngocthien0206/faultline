"""The POS source database: the tables a coffee shop's till writes to.

`apply_day` replays one day of generated changes. Orders are upserted, because
a refund is an update of an existing order row (new status, new `updated_at`),
exactly what an incremental extractor keyed on `updated_at` has to notice.
Lines and payments are append-only, as on a real till.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from pathlib import Path

from faultline.connectors.postgres import SourceDatabase
from faultline.connectors.workspace import validate_workspace_name
from faultline.datagen.writer import partition_dir


@dataclass(frozen=True)
class SourceTable:
    name: str
    ddl: str  # column definitions, in generator column order
    key: tuple[str, ...]
    cursor: str  # the column an incremental extractor watches
    upsert: bool  # True: a later row with the same key replaces the earlier one


TABLES: tuple[SourceTable, ...] = (
    SourceTable(
        name="orders",
        ddl="""
            order_id      text        PRIMARY KEY,
            store_id      text        NOT NULL,
            member_id     text,
            channel       text        NOT NULL,
            status        text        NOT NULL,
            ordered_at    timestamptz NOT NULL,
            updated_at    timestamptz NOT NULL,
            subtotal_vnd  bigint      NOT NULL,
            discount_vnd  bigint      NOT NULL,
            total_vnd     bigint      NOT NULL,
            promo_code    text,
            item_count    integer     NOT NULL
        """,
        key=("order_id",),
        cursor="updated_at",
        upsert=True,
    ),
    SourceTable(
        name="order_items",
        ddl="""
            order_id        text        NOT NULL,
            line_no         integer     NOT NULL,
            item_id         text        NOT NULL,
            qty             integer     NOT NULL,
            unit_price_vnd  bigint      NOT NULL,
            created_at      timestamptz NOT NULL,
            PRIMARY KEY (order_id, line_no)
        """,
        key=("order_id", "line_no"),
        cursor="created_at",
        upsert=False,
    ),
    SourceTable(
        name="payments",
        ddl="""
            payment_id  text        PRIMARY KEY,
            order_id    text        NOT NULL,
            kind        text        NOT NULL,
            method      text        NOT NULL,
            amount_vnd  bigint      NOT NULL,
            paid_at     timestamptz NOT NULL
        """,
        key=("payment_id",),
        cursor="paid_at",
        upsert=False,
    ),
)


def create_tables(db: SourceDatabase, workspace: str) -> None:
    """Create the POS tables (and an index on each cursor) in the workspace schema."""
    ws = validate_workspace_name(workspace)
    for t in TABLES:
        db.execute(f'CREATE TABLE "{ws}".{t.name} ({t.ddl})')
        db.execute(f'CREATE INDEX ON "{ws}".{t.name} ({t.cursor})')


def day_files(generated_root: Path, table: str, dt: date) -> list[Path]:
    """Every store's extract of `table` for day `dt`, in a stable order."""
    directory = partition_dir(generated_root, "pos", table, {"dt": dt.isoformat()})
    return sorted(directory.glob(f"store=*/{table}.csv"))


def upsert_sql(workspace: str, table: SourceTable, columns: list[str]) -> str:
    """INSERT from the staging table; for upsert tables a key conflict updates the row."""
    cols = ", ".join(columns)
    sql = f'INSERT INTO "{workspace}".{table.name} ({cols}) SELECT {cols} FROM staging_{table.name}'
    if table.upsert:
        updates = ", ".join(f"{c} = EXCLUDED.{c}" for c in columns if c not in table.key)
        sql += f" ON CONFLICT ({', '.join(table.key)}) DO UPDATE SET {updates}"
    return sql


def apply_day(db: SourceDatabase, workspace: str, generated_root: Path, dt: date) -> dict[str, int]:
    """Replay day `dt` into the source tables, all or nothing. Returns rows per table."""
    ws = validate_workspace_name(workspace)
    counts: dict[str, int] = {}
    with db.conn.transaction(), db.conn.cursor() as cur:
        for t in TABLES:
            cur.execute(f'CREATE TEMP TABLE staging_{t.name} (LIKE "{ws}".{t.name}) ON COMMIT DROP')
            columns: list[str] | None = None
            for path in day_files(generated_root, t.name, dt):
                content = path.read_bytes()
                header = content.split(b"\n", 1)[0].decode("utf-8").split(",")
                if columns is None:
                    columns = header
                elif header != columns:
                    raise ValueError(f"{path}: columns {header} differ from {columns}")
                columns_sql = ", ".join(header)
                copy_sql = f"COPY staging_{t.name} ({columns_sql}) FROM STDIN (FORMAT csv, HEADER)"
                with cur.copy(copy_sql) as copy:
                    copy.write(content)
            if columns is None:
                counts[t.name] = 0
                continue
            cur.execute(upsert_sql(ws, t, columns))
            counts[t.name] = cur.rowcount
    return counts
