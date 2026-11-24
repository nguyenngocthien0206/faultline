"""Execution faults (injection point 3): the runs themselves misbehave.

CrashAfterLanding: a day's run dies, hard, right after ingestion landed its
files and saved its cursor, before anything was loaded (an out-of-memory kill,
a lost node). The cursor lives with the landing files, the data was meant to go
to the warehouse: the crash leaves the first ahead of the second. A platform
that only loads the partition it runs, from a cursor that has already moved
on, never loads that day. The crash itself is faultline's (INTERRUPTED), so it
does not count as the platform noticing anything.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from faultline.adapters.base import Capability
from faultline.faults.base import Fault, FaultSetupError, InjectionPoint
from faultline.schedule import Run, Schedule

AFTER_LANDING = "after_landing"


@dataclass
class CrashAfterLanding(Fault):
    partition: date

    name: ClassVar[str] = "crash_after_landing"
    injection_point: ClassVar[InjectionPoint] = InjectionPoint.EXECUTION
    requires: ClassVar[frozenset[Capability]] = frozenset({Capability.FAILPOINTS})

    def apply(self, schedule: Schedule) -> Schedule:
        """The day's run dies at the failpoint after landing; every other run is untouched."""
        if Run(self.partition) not in schedule:
            raise FaultSetupError(f"{self.partition} is never run in this schedule")
        at = schedule.index(Run(self.partition))
        crash = Run(self.partition, failpoint=AFTER_LANDING)
        return (*schedule[:at], crash, *schedule[at + 1 :])

    def affected_partitions(self) -> set[date]:
        return {self.partition}  # the operator reruns the day that crashed

    def describe(self) -> str:
        return (
            f"the run of {self.partition} dies right after landing its files, "
            "before anything is loaded"
        )
