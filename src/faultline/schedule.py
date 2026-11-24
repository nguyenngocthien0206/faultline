"""Schedules: an experiment as plain data before anything executes.

A schedule is the ordered list of things that happen to the platform: input
arriving for a day, a run of a partition (or of one half of it), a landing
file appearing again, held back, arriving late, cut short or renamed. Faults transform schedules
(and the input they deliver), so an experiment can be printed, compared and
tested without starting a platform.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from faultline.adapters.base import Stage
from faultline.datagen.spec import DatasetSpec


@dataclass(frozen=True)
class Deliver:
    """The platform receives day `day`'s input (adapter.deliver)."""

    day: date


@dataclass(frozen=True)
class Run:
    """The platform runs partition `partition` (adapter.run), or one stage of it."""

    partition: date
    stage: Stage | None = None  # None: ingest and load together, as every day
    failpoint: str | None = None  # die at this named point (adapter.run's failpoint)


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


@dataclass(frozen=True)
class HoldLandingFiles:
    """`dataset`'s landing files of `partition` disappear from the landing zone.

    faultline keeps them (in the same sandbox, for a later ReleaseLandingFiles
    in the same phase): put between the ingest and load stages, the load never
    sees them.
    """

    dataset: str
    partition: date


@dataclass(frozen=True)
class ReleaseLandingFiles:
    """The files held back for `dataset` and `partition` land, late, where they belong."""

    dataset: str
    partition: date


@dataclass(frozen=True)
class TruncateLandingFiles:
    """`dataset`'s landing files of `partition` lose their last rows, cut on a row boundary.

    Each file keeps its first `keep` share of rows and still reads fine. The
    originals are kept (as by HoldLandingFiles), so a ReleaseLandingFiles in
    the same sandbox puts the full files back.
    """

    dataset: str
    partition: date
    keep: float = 0.8


@dataclass(frozen=True)
class RenameLandingColumn:
    """In `dataset`'s landing files of `partition`, column `old` is now called `new`.

    Values and types stay; with `move_to_end` the column also moves last. The
    originals are kept, so a ReleaseLandingFiles in the same sandbox puts the
    files with the agreed schema back.
    """

    dataset: str
    partition: date
    old: str
    new: str
    move_to_end: bool = False


Action = (
    Deliver
    | Run
    | CopyLandingFiles
    | HoldLandingFiles
    | ReleaseLandingFiles
    | TruncateLandingFiles
    | RenameLandingColumn
)
Schedule = tuple[Action, ...]


def clean_schedule(spec: DatasetSpec) -> Schedule:
    """The steady state: each day's input arrives, then that day is processed."""
    actions: list[Action] = []
    for day in spec.dates():
        actions += [Deliver(day), Run(day)]
    return tuple(actions)
