"""Schedules: an experiment as plain data before anything executes.

A schedule is the ordered list of things that happen to the platform: input
arriving for a day, a run of a partition, a landing file appearing again.
Faults transform schedules (and the input they deliver), so an experiment can
be printed, compared and tested without starting a platform.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from faultline.datagen.spec import DatasetSpec


@dataclass(frozen=True)
class Deliver:
    """The platform receives day `day`'s input (adapter.deliver)."""

    day: date


@dataclass(frozen=True)
class Run:
    """The platform runs partition `partition` (adapter.run)."""

    partition: date


@dataclass(frozen=True)
class CopyLandingFiles:
    """`dataset`'s landing files of one partition land again under another one.

    The copies are named `prefix` + the original name, as a retried upload or
    a resent extract would be. Needs the adapter's landing files capability.
    """

    dataset: str  # as in the generated data, e.g. "pos/order_items"
    from_partition: date
    to_partition: date
    prefix: str = "redelivered-"


Action = Deliver | Run | CopyLandingFiles
Schedule = tuple[Action, ...]


def clean_schedule(spec: DatasetSpec) -> Schedule:
    """The steady state: each day's input arrives, then that day is processed."""
    actions: list[Action] = []
    for day in spec.dates():
        actions += [Deliver(day), Run(day)]
    return tuple(actions)
