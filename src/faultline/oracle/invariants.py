"""Key invariants: checks that need no knowledge of the platform's business logic.

They validate the baseline itself. The differential oracle assumes a clean run
is correct; these checks catch a platform that is wrong even when nothing was
broken, before any fault is injected (the steady-state hypothesis).
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from faultline.adapters.base import TableContract
from faultline.oracle._sql import connect, key_expr, literal, parquet

SAMPLE_SIZE = 5


@dataclass(frozen=True)
class InvariantResult:
    name: str
    table: str
    violations: int
    samples: tuple[str, ...] = ()

    @property
    def holds(self) -> bool:
        return self.violations == 0

    def summary(self) -> str:
        if self.holds:
            return f"{self.table}: {self.name} holds"
        examples = ", ".join(self.samples)
        return f"{self.table}: {self.name} violated by {self.violations:,} keys, e.g. {examples}"


def unique_keys(path: Path, contract: TableContract) -> InvariantResult:
    """Every key appears exactly once."""
    con = connect()
    rows = con.execute(
        f"SELECT {key_expr(contract.key)} AS k FROM {parquet(path)} "
        "GROUP BY k HAVING count(*) > 1 ORDER BY k"
    ).fetchall()
    return InvariantResult(
        "unique_keys", contract.name, len(rows), tuple(r[0] for r in rows[:SAMPLE_SIZE])
    )


def complete_keys(path: Path, contract: TableContract, generated_root: Path) -> InvariantResult:
    """Every key the generator produced for `contract.truth_dataset` is present."""
    if contract.truth_dataset is None:
        raise ValueError(f"{contract.name} declares no truth_dataset")
    source, dataset = contract.truth_dataset.split("/", 1)
    pattern = (
        Path(generated_root) / "landing" / source / dataset / "**" / f"{dataset}.csv"
    ).as_posix()
    con = connect()
    rows = con.execute(
        f"""
        WITH generated AS (
            SELECT DISTINCT {key_expr(contract.key)} AS k
            FROM read_csv({literal(pattern)}, header = true, all_varchar = true)
        ),
        present AS (SELECT DISTINCT {key_expr(contract.key)} AS k FROM {parquet(path)})
        SELECT k FROM generated ANTI JOIN present USING (k) ORDER BY k
        """
    ).fetchall()
    return InvariantResult(
        "complete_keys", contract.name, len(rows), tuple(r[0] for r in rows[:SAMPLE_SIZE])
    )
