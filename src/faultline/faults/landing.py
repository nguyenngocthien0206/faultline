"""Landing faults (injection point 2): files between ingestion and the warehouse misbehave.

DuplicateFile: a day's extract lands a second time, under a later partition and
a new name, the way a retried upload or a supplier resending yesterday's files
would. The rows are byte for byte the ones already loaded. A loader that takes
every file it finds loads them twice; nothing fails, and the numbers grow.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from faultline.adapters.base import Capability
from faultline.faults.base import Fault, FaultSetupError, InjectionPoint
from faultline.schedule import CopyLandingFiles, Run, Schedule

POS_DATASETS = ("pos/orders", "pos/order_items", "pos/payments")


@dataclass
class DuplicateFile(Fault):
    original_day: date
    redelivered_on: date
    datasets: tuple[str, ...] = POS_DATASETS  # the whole extract, by default

    name: ClassVar[str] = "duplicate_file"
    injection_point: ClassVar[InjectionPoint] = InjectionPoint.LANDING
    requires: ClassVar[frozenset[Capability]] = frozenset({Capability.LANDING_FILES})

    def __post_init__(self) -> None:
        if self.redelivered_on <= self.original_day:
            raise FaultSetupError("redelivered_on must come after original_day")
        if not self.datasets:
            raise FaultSetupError("name at least one dataset to deliver again")

    def apply(self, schedule: Schedule) -> Schedule:
        """Copy the files right before the run of `redelivered_on`, which then finds them."""
        runs = [a.partition for a in schedule if isinstance(a, Run)]
        for day in (self.original_day, self.redelivered_on):
            if day not in runs:
                raise FaultSetupError(f"{day} is never run in this schedule")
        if runs.index(self.original_day) > runs.index(self.redelivered_on):
            raise FaultSetupError(f"{self.original_day} must be run before {self.redelivered_on}")

        copies = tuple(
            CopyLandingFiles(d, self.original_day, self.redelivered_on) for d in self.datasets
        )
        at = schedule.index(Run(self.redelivered_on))
        return schedule[:at] + copies + schedule[at:]

    def affected_partitions(self) -> set[date]:
        return {self.redelivered_on}

    def describe(self) -> str:
        what = "the extract" if self.datasets == POS_DATASETS else ", ".join(self.datasets)
        return (
            f"{what} of {self.original_day} lands again under {self.redelivered_on}, "
            "as a retried upload"
        )
