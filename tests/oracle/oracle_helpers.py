"""Build small Parquet snapshots for oracle tests."""

from pathlib import Path

import duckdb

from faultline.adapters.base import TableContract

SALES = TableContract(
    "gold_sales",
    key=("order_date", "store_id"),
    measures=("orders", "net_vnd"),
    ignore=("loaded_at",),
)
COLUMNS = "order_date DATE, store_id VARCHAR, orders BIGINT, net_vnd BIGINT, loaded_at VARCHAR"

Row = tuple[str, str, int, int, str]


def write(path: Path, rows: list[Row], columns: str = COLUMNS) -> Path:
    con = duckdb.connect()
    con.execute(f"CREATE TABLE t ({columns})")
    if rows:
        placeholders = ", ".join("?" for _ in rows[0])
        con.executemany(f"INSERT INTO t VALUES ({placeholders})", rows)
    con.execute(f"COPY t TO '{path.as_posix()}' (FORMAT parquet)")
    return path
