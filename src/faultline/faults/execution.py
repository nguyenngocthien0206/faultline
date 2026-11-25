"""Execution faults (injection point 3): the runs themselves misbehave.

CrashAfterLanding: a day's run dies, hard, right after ingestion landed its
files and saved its cursor, before anything was loaded (an out-of-memory kill,
a lost node). The cursor lives with the landing files, the data was meant to go
to the warehouse: the crash leaves the first ahead of the second. A platform
that only loads the partition it runs, from a cursor that has already moved
on, never loads that day. The crash itself is faultline's (INTERRUPTED), so it
does not count as the platform noticing anything.

CrashMidWrite: a day's run dies, hard, in the middle of refreshing a gold
table: after its old rows for the day's keys are gone and before the new ones
are in. A refresh that deletes, then inserts, leaves readers with rows missing
that were there a moment ago (and still missing after later days, unless a
later run happens to touch those keys again). One that builds the new rows
aside and swaps them in leaves the old ones in place. Snapshots right before
and right after the crash let `no_vanishing_rows` see the difference.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import ClassVar

from faultline.adapters.base import Capability
from faultline.faults.base import Fault, FaultSetupError, InjectionPoint
from faultline.schedule import Run, Schedule, Snapshot

AFTER_LANDING = "after_landing"
GOLD_MID_WRITE = "gold_mid_write"
BEFORE_CRASH, AFTER_CRASH = "before_crash", "after_crash"  # snapshot names


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


@dataclass
class CrashMidWrite(Fault):
    partition: date

    name: ClassVar[str] = "crash_mid_write"
    injection_point: ClassVar[InjectionPoint] = InjectionPoint.EXECUTION
    requires: ClassVar[frozenset[Capability]] = frozenset({Capability.FAILPOINTS})

    def apply(self, schedule: Schedule) -> Schedule:
        """The day's run dies inside the gold refresh, with a snapshot on either side."""
        if Run(self.partition) not in schedule:
            raise FaultSetupError(f"{self.partition} is never run in this schedule")
        at = schedule.index(Run(self.partition))
        crash = (
            Snapshot(BEFORE_CRASH),
            Run(self.partition, failpoint=GOLD_MID_WRITE),
            Snapshot(AFTER_CRASH),
        )
        return (*schedule[:at], *crash, *schedule[at + 1 :])

    def affected_partitions(self) -> set[date]:
        return {self.partition}  # the operator reruns the day that crashed

    def describe(self) -> str:
        return (
            f"the run of {self.partition} dies while refreshing gold, "
            "between taking the old rows out and putting the new ones in"
        )
