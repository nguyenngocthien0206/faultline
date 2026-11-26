"""The property interface and what a property gets to look at.

A property never drives the platform. It reads snapshots taken at a
checkpoint, compares them with the clean baseline and the generator's truth,
and says whether its promise held there, with evidence a human can read.

A property may add a probe: actions of its own (a rerun, say, with snapshots
around it) that exercise its promise. The runner runs them after the fault
phase's schedule, in the same sandbox, and before checkpoint F; their runs
count as the fault phase's, so a probe run that fails is a signal.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from itertools import pairwise
from pathlib import Path
from typing import ClassVar

from faultline.adapters.base import Capability, RunResult, TableContract
from faultline.schedule import Schedule


@dataclass(frozen=True)
class Round:
    """One round of recovery: every affected partition rerun once, then compared."""

    runs: int
    keys_off: int  # keys that differ from the baseline afterwards, over every table
    # Rows behind those keys (oracle.diff): a rerun that appends one more copy
    # of the same keys leaves keys_off where it was and adds to this.
    rows_off: int = 0
    differences: tuple[str, ...] = ()  # one line per table that differs


@dataclass(frozen=True)
class Recovery:
    """How recovery went: rounds until the outputs matched the baseline, at most `max_rounds`."""

    max_rounds: int
    rounds: tuple[Round, ...]

    @property
    def converged(self) -> bool:
        return bool(self.rounds) and self.rounds[-1].keys_off == 0

    @property
    def runs(self) -> int:
        return sum(r.runs for r in self.rounds)

    def summary(self) -> str:
        n, runs = len(self.rounds), self.runs
        spent = f"{_count(n, 'round')} ({_count(runs, 'run')})"
        if self.converged:
            return f"converged after {spent}"
        keys = ", ".join(f"{r.keys_off:,}" for r in self.rounds)
        rows = ", ".join(f"{r.rows_off:,}" for r in self.rounds)
        off = f"{keys} keys and {rows} rows off the baseline{self._trend()}"
        if n < self.max_rounds:  # stopped early: the baseline it compares with is not usable
            return f"stopped after {spent}: {off}"
        return f"not converged within {spent}: {off}"

    def _trend(self) -> str:
        rows = [r.rows_off for r in self.rounds]
        if len(rows) < 2:
            return ""
        if all(a < b for a, b in pairwise(rows)):
            return ", growing"
        if all(a > b for a, b in pairwise(rows)):
            return ", shrinking"
        return ""


def _count(n: int, word: str) -> str:
    return f"{n} {word}" if n == 1 else f"{n} {word}s"


@dataclass(frozen=True)
class Checkpoint:
    name: str  # "F": end of the fault phase, "R": after recovery
    snapshots: dict[str, Path]
    runs: tuple[tuple[date, RunResult], ...]  # the runs of the phase that led here
    # Snapshots taken on the way here, by name (Snapshot actions in the phase's
    # schedule). R carries the fault phase's too, so a property built on a probe
    # judges R as it judged F, and F's own snapshots as "F", for properties
    # about what recovery changed.
    marks: dict[str, dict[str, Path]] = field(default_factory=dict)
    # R only: the rounds recovery took. None at F, and when there was nothing
    # to recover (an experiment without a fault).
    recovery: Recovery | None = None


@dataclass(frozen=True)
class PropertyContext:
    contracts: tuple[TableContract, ...]
    baseline: dict[str, Path]  # clean run, same input: the differential answer key
    truth_root: Path  # the generated data before any fault touched it


@dataclass(frozen=True)
class PropertyResult:
    name: str
    checkpoint: str
    holds: bool
    evidence: tuple[str, ...] = ()


class Property(ABC):
    name: ClassVar[str]
    # What its probe needs from the adapter; the experiment is SKIPPED without it.
    requires: ClassVar[frozenset[Capability]] = frozenset()

    @abstractmethod
    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult: ...

    def probe(self) -> Schedule:
        """Actions this property adds to exercise itself; most add none."""
        return ()
