"""The property interface and what a property gets to look at.

A property never drives the platform. It reads snapshots taken at a
checkpoint, compares them with the clean baseline and the generator's truth,
and says whether its promise held there, with evidence a human can read.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import ClassVar

from faultline.adapters.base import RunResult, TableContract


@dataclass(frozen=True)
class Checkpoint:
    name: str  # "F": end of the fault phase, "R": after recovery
    snapshots: dict[str, Path]
    runs: tuple[tuple[date, RunResult], ...]  # the runs of the phase that led here
    # Snapshots taken on the way here, by name (Snapshot actions in the phase's
    # schedule); R also carries F's, as "F", for properties about what changed.
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

    @abstractmethod
    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult: ...
