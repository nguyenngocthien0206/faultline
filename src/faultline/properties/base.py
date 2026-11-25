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
from pathlib import Path
from typing import ClassVar

from faultline.adapters.base import Capability, RunResult, TableContract
from faultline.schedule import Schedule


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
