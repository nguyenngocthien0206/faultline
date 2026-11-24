"""Built-in experiments, until experiments are read from YAML specs."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from faultline.datagen.spec import DatasetSpec
from faultline.faults.landing import DuplicateFile, LateFile, TruncatedFile
from faultline.faults.source import PosOfflineSync
from faultline.properties.core import NoDataLoss, NoSilentCorruption
from faultline.runner import Experiment


def pos_offline_sync(spec: DatasetSpec) -> Experiment:
    """A store's till is offline on days 2 and 3 and syncs on day 4, old timestamps intact."""
    if spec.days < 4:
        raise ValueError("pos-offline-sync needs at least 4 days of data")
    start = spec.start_date
    return Experiment(
        name="pos-offline-sync",
        description=(
            "A store's POS goes offline for two days. When it reconnects, its orders reach "
            "the source with their real (older) timestamps, behind the extractor's cursor."
        ),
        spec=spec,
        faults=(
            PosOfflineSync(
                offline_from=start + timedelta(days=1), sync_on=start + timedelta(days=3)
            ),
        ),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def duplicate_file(spec: DatasetSpec) -> Experiment:
    """Day 2's whole POS extract lands a second time, under day 3, as a retried upload."""
    if spec.days < 3:
        raise ValueError("duplicate-file needs at least 3 days of data")
    start = spec.start_date
    return Experiment(
        name="duplicate-file",
        description=(
            "The files of one day's extract land again a day later under new names, "
            "as a retried upload or a supplier resending yesterday's files would."
        ),
        spec=spec,
        faults=(
            DuplicateFile(
                original_day=start + timedelta(days=1), redelivered_on=start + timedelta(days=2)
            ),
        ),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def late_file(spec: DatasetSpec) -> Experiment:
    """Day 2's order lines file lands late: after day 3 was processed, before day 4 runs."""
    if spec.days < 4:
        raise ValueError("late-file needs at least 4 days of data")
    start = spec.start_date
    return Experiment(
        name="late-file",
        description=(
            "One file of a day's extract (the order lines) arrives after that day was "
            "loaded, under its own partition, two days late."
        ),
        spec=spec,
        faults=(
            LateFile(partition=start + timedelta(days=1), lands_before=start + timedelta(days=3)),
        ),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def truncated_file(spec: DatasetSpec) -> Experiment:
    """The last day's order lines file is cut to 80% of its rows; it still reads fine."""
    return Experiment(
        name="truncated-file",
        description=(
            "One file of the last day's extract (the order lines) lands short: cut on a row "
            "boundary, so it parses and nothing in it is invalid. Recovery redelivers it in full."
        ),
        spec=spec,
        faults=(TruncatedFile(partition=spec.end_date),),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


BUILTIN: dict[str, Callable[[DatasetSpec], Experiment]] = {
    "pos-offline-sync": pos_offline_sync,
    "duplicate-file": duplicate_file,
    "late-file": late_file,
    "truncated-file": truncated_file,
}
