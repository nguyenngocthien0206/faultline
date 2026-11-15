"""Landing faults (injection point 2): files between ingestion and the warehouse misbehave.

DuplicateFile: a day's extract lands a second time, under a later partition and
a new name, the way a retried upload or a supplier resending yesterday's files
would. The rows are byte for byte the ones already loaded. A loader that takes
every file it finds loads them twice; nothing fails, and the numbers grow.

LateFile: one file of a day's extract arrives after that day was loaded (a slow
upload, a partner's late drop). The day runs as ingest and load with the file
held back in between, and the file lands, under its own partition, days later.
A loader that only reads today's partition never looks back; nothing fails,
and the rows are simply not there.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from faultline.adapters.base import Capability, Stage
from faultline.faults.base import Fault, FaultSetupError, InjectionPoint
from faultline.schedule import (
    CopyLandingFiles,
    HoldLandingFiles,
    ReleaseLandingFiles,
    Run,
    Schedule,
)

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


@dataclass
class LateFile(Fault):
    partition: date
    lands_before: date  # the file lands right before this day's run
    datasets: tuple[str, ...] = ("pos/order_items",)  # one file, by default

    name: ClassVar[str] = "late_file"
    injection_point: ClassVar[InjectionPoint] = InjectionPoint.LANDING
    requires: ClassVar[frozenset[Capability]] = frozenset(
        {Capability.LANDING_FILES, Capability.RUN_STAGES}
    )

    def __post_init__(self) -> None:
        if self.lands_before <= self.partition:
            raise FaultSetupError("lands_before must come after the late file's partition")
        if not self.datasets:
            raise FaultSetupError("name at least one dataset whose file is late")

    def apply(self, schedule: Schedule) -> Schedule:
        """Split the partition's run around the held files; release them before `lands_before`."""
        runs = [a.partition for a in schedule if isinstance(a, Run) and a.stage is None]
        for day in (self.partition, self.lands_before):
            if day not in runs:
                raise FaultSetupError(f"{day} is never run in this schedule")
        if runs.index(self.partition) > runs.index(self.lands_before):
            raise FaultSetupError(f"{self.partition} must be run before {self.lands_before}")

        holds = tuple(HoldLandingFiles(d, self.partition) for d in self.datasets)
        split = (Run(self.partition, Stage.INGEST), *holds, Run(self.partition, Stage.LOAD))
        at = schedule.index(Run(self.partition))
        schedule = schedule[:at] + split + schedule[at + 1 :]

        releases = tuple(ReleaseLandingFiles(d, self.partition) for d in self.datasets)
        at = schedule.index(Run(self.lands_before))
        return schedule[:at] + releases + schedule[at:]

    def affected_partitions(self) -> set[date]:
        return {self.partition}  # where the late file now sits

    def describe(self) -> str:
        what = ", ".join(self.datasets)
        return (
            f"the {what} file of {self.partition} lands late, "
            f"just before the run of {self.lands_before}"
        )
