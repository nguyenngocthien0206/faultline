"""Fingerprints: a cheap summary that tells whether two snapshots hold the same rows.

The row hash is the SUM of per-row hashes modulo 2^64, not their XOR. XOR is
the tempting choice and the wrong one: two identical rows XOR to zero, so a
duplicated row would leave the fingerprint unchanged, and duplicates are one
of the main things faultline exists to catch. A sum is order independent and
still counts every copy.

DuckDB's hash() may change between DuckDB versions. Fingerprints are only
compared within one run of faultline, never stored and compared later.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from faultline.adapters.base import TableContract
from faultline.oracle._sql import (
    HASH_MODULUS,
    compared_columns,
    connect,
    ident,
    key_expr,
    parquet,
    row_hash_expr,
)


@dataclass(frozen=True)
class Fingerprint:
    table: str
    rows: int
    distinct_keys: int
    row_hash: int
    measures: tuple[tuple[str, int], ...]

    @property
    def duplicate_rows(self) -> int:
        """Rows beyond the first for each key (0 when the key is unique)."""
        return self.rows - self.distinct_keys

    def measure(self, name: str) -> int:
        return dict(self.measures)[name]


def fingerprint(path: Path, contract: TableContract) -> Fingerprint:
    con = connect()
    columns = compared_columns(con, path, contract)
    measure_sums = [f"coalesce(sum({ident(m)}), 0)::HUGEINT" for m in contract.measures]
    select = ", ".join(
        [
            "count(*)",
            f"count(DISTINCT {key_expr(contract.key)})",
            f"coalesce(sum({row_hash_expr(columns)}::HUGEINT), 0) % {HASH_MODULUS}",
            *measure_sums,
        ]
    )
    rows, distinct, row_hash, *sums = con.execute(
        f"SELECT {select} FROM {parquet(path)}"
    ).fetchone()
    return Fingerprint(
        table=contract.name,
        rows=int(rows),
        distinct_keys=int(distinct),
        row_hash=int(row_hash),
        measures=tuple((m, int(s)) for m, s in zip(contract.measures, sums, strict=True)),
    )
