"""Small SQL helpers shared by the oracle modules."""

from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import duckdb

from faultline.adapters.base import TableContract

HASH_MODULUS = 2**64


def ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def parquet(path: Path) -> str:
    return f"read_parquet({literal(Path(path).as_posix())})"


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("SET TimeZone = 'UTC'")  # canonical: never depend on the host's zone
    return con


def compared_columns(
    con: duckdb.DuckDBPyConnection, path: Path, contract: TableContract
) -> list[str]:
    """Columns that take part in comparisons: all but ignored ones, sorted by name."""
    described = con.execute(f"DESCRIBE SELECT * FROM {parquet(path)}").fetchall()
    names = [row[0] for row in described]
    missing = [c for c in (*contract.key, *contract.measures) if c not in names]
    if missing:
        raise ValueError(f"{contract.name}: snapshot {path} lacks columns {missing}")
    return sorted(c for c in names if c not in contract.ignore)


def key_expr(columns: Iterable[str]) -> str:
    """One VARCHAR per key, readable in reports: 'HCM-001|2026-11-01'."""
    return " || '|' || ".join(f"coalesce(CAST({ident(c)} AS VARCHAR), '<null>')" for c in columns)


def row_hash_expr(columns: Iterable[str]) -> str:
    return f"hash({', '.join(ident(c) for c in columns)})"
