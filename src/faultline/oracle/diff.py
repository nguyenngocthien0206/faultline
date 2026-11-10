"""Diffs: where two snapshots of the same table disagree, sorted into buckets.

Each key falls into at most one bucket:

- missing:    in the expected snapshot, absent from the actual one (data loss)
- extra:      absent from the expected snapshot, present in the actual one
- duplicated: present in both, with more rows in the actual one (exactly-once broken)
- changed:    present in both, same or fewer rows, different content (silent corruption)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from faultline.adapters.base import TableContract
from faultline.oracle._sql import (
    compared_columns,
    connect,
    ident,
    key_expr,
    parquet,
    row_hash_expr,
)

SAMPLE_SIZE = 5
BUCKETS = ("missing", "extra", "duplicated", "changed")


@dataclass(frozen=True)
class MeasureDelta:
    name: str
    expected: int
    actual: int

    @property
    def delta(self) -> int:
        return self.actual - self.expected


@dataclass(frozen=True)
class Diff:
    table: str
    counts: dict[str, int]
    samples: dict[str, tuple[str, ...]] = field(default_factory=dict)
    measures: tuple[MeasureDelta, ...] = ()

    @property
    def is_empty(self) -> bool:
        return not any(self.counts.values())

    def summary(self) -> str:
        """One line a human can read in a scorecard."""
        if self.is_empty:
            return f"{self.table}: identical"
        parts = [f"{n:,} {b} keys" for b in BUCKETS if (n := self.counts[b])]
        for m in self.measures:
            if m.delta:
                pct = f" ({m.delta / m.expected:+.1%})" if m.expected else ""
                parts.append(f"{m.name} {m.expected:,} -> {m.actual:,}{pct}")
        return f"{self.table}: " + ", ".join(parts)


def diff(expected: Path, actual: Path, contract: TableContract) -> Diff:
    con = connect()
    columns = compared_columns(con, expected, contract)
    actual_columns = compared_columns(con, actual, contract)
    if columns != actual_columns:
        raise ValueError(
            f"{contract.name}: snapshots have different columns: {columns} vs {actual_columns}"
        )

    def per_key(path: Path) -> str:
        return (
            f"SELECT {key_expr(contract.key)} AS k, count(*) AS n, "
            f"sum({row_hash_expr(columns)}::HUGEINT) AS h FROM {parquet(path)} GROUP BY k"
        )

    con.execute(
        f"""
        CREATE TEMP TABLE keyed AS
        SELECT coalesce(e.k, a.k) AS k,
               CASE
                   WHEN a.k IS NULL THEN 'missing'
                   WHEN e.k IS NULL THEN 'extra'
                   WHEN a.n > e.n THEN 'duplicated'
                   WHEN a.n <> e.n OR a.h <> e.h THEN 'changed'
               END AS bucket
        FROM ({per_key(expected)}) AS e
        FULL OUTER JOIN ({per_key(actual)}) AS a ON e.k = a.k
        """
    )
    counts = dict.fromkeys(BUCKETS, 0)
    counts.update(
        dict(
            con.execute(
                "SELECT bucket, count(*) FROM keyed WHERE bucket IS NOT NULL GROUP BY 1"
            ).fetchall()
        )
    )
    samples = {
        b: tuple(
            r[0]
            for r in con.execute(
                "SELECT k FROM keyed WHERE bucket = ? ORDER BY k LIMIT ?", [b, SAMPLE_SIZE]
            ).fetchall()
        )
        for b in BUCKETS
        if counts[b]
    }

    measures = []
    for m in contract.measures:
        total = f"SELECT coalesce(sum({ident(m)}), 0)::HUGEINT FROM"
        exp_sum = con.execute(f"{total} {parquet(expected)}").fetchone()[0]
        act_sum = con.execute(f"{total} {parquet(actual)}").fetchone()[0]
        measures.append(MeasureDelta(m, int(exp_sum), int(act_sum)))

    return Diff(table=contract.name, counts=counts, samples=samples, measures=tuple(measures))
