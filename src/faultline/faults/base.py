"""The fault interface.

A fault acts on what faultline controls, never on the platform's internals:
the input it delivers (a private copy of the generated data), the schedule of
deliveries and runs, and later the platform's surroundings through optional
adapter capabilities. That keeps every fault usable on any platform whose
adapter offers what the fault `requires`.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from enum import StrEnum
from pathlib import Path
from typing import ClassVar

from faultline.adapters.base import Capability
from faultline.schedule import Action, Schedule


class InjectionPoint(StrEnum):
    SOURCE = "source"  # the systems ingestion reads from
    LANDING = "landing"  # files between ingestion and the warehouse
    EXECUTION = "execution"  # the runs themselves


class Fault(ABC):
    name: ClassVar[str]
    injection_point: ClassVar[InjectionPoint]
    requires: ClassVar[frozenset[Capability]] = frozenset()

    def prepare_input(self, input_root: Path) -> None:  # noqa: B027 - optional hook
        """Rewrite the faulted run's private copy of the generated input."""

    def apply(self, schedule: Schedule) -> Schedule:
        """Change the order or content of the schedule. Default: unchanged."""
        return schedule

    def heal(self) -> list[Action]:
        """Actions that remove the cause before recovery runs. Default: none."""
        return []

    @abstractmethod
    def affected_partitions(self) -> set[date]:
        """Partitions an operator would rerun after this incident."""

    @abstractmethod
    def describe(self) -> str:
        """One line for reports, e.g. 'POS of HN-001 offline 2026-11-02..03, synced 04'."""
