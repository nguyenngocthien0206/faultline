"""Built-in experiments, until experiments are read from YAML specs."""

from __future__ import annotations

from collections.abc import Callable
from datetime import timedelta

from faultline.datagen.spec import DatasetSpec
from faultline.faults.execution import CrashAfterLanding, CrashMidWrite, RetryAfterPartialCommit
from faultline.faults.landing import ColumnDrift, DuplicateFile, LateFile, TruncatedFile
from faultline.faults.source import PosOfflineSync
from faultline.properties.core import (
    Idempotency,
    NoDataLoss,
    NoSilentCorruption,
    NoVanishingRows,
)
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


def column_rename(spec: DatasetSpec) -> Experiment:
    """The last day's payments file arrives with amount_vnd renamed total_amount_vnd."""
    return Experiment(
        name="column-rename",
        description=(
            "One file of the last day's extract (the payments) arrives with a column renamed, "
            "amount_vnd to total_amount_vnd. Recovery redelivers it with the agreed schema."
        ),
        spec=spec,
        faults=(ColumnDrift(partition=spec.end_date),),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def crash_after_landing(spec: DatasetSpec) -> Experiment:
    """Day 2's run dies right after landing its files, before loading anything."""
    if spec.days < 3:
        raise ValueError("crash-after-landing needs at least 3 days of data")
    return Experiment(
        name="crash-after-landing",
        description=(
            "The run of day 2 is killed, hard, between ingestion and loading: the files and "
            "the ingestion cursor are saved, the warehouse has nothing of the day."
        ),
        spec=spec,
        faults=(CrashAfterLanding(partition=spec.start_date + timedelta(days=1)),),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def crash_mid_write(spec: DatasetSpec) -> Experiment:
    """Day 3's run dies inside the gold refresh, after the delete, before the insert.

    Day 3, not day 2: a refresh only rewrites the keys its new data touches,
    and day 3 is the first extract to touch an older day (refunds of day 1
    orders). Crash on a day that only touches itself and the vanished rows
    are new ones a reader never saw.
    """
    if spec.days < 3:
        raise ValueError("crash-mid-write needs at least 3 days of data")
    return Experiment(
        name="crash-mid-write",
        description=(
            "The run of day 3 is killed, hard, in the middle of refreshing the gold sales "
            "table: the old rows of the days it touches are out, the new ones not yet in."
        ),
        spec=spec,
        faults=(CrashMidWrite(partition=spec.start_date + timedelta(days=2)),),
        properties=(NoDataLoss(), NoSilentCorruption(), NoVanishingRows()),
    )


def retry_after_partial_commit(spec: DatasetSpec) -> Experiment:
    """Day 2's run dies after writing bronze; the orchestrator retries it from that step."""
    if spec.days < 3:
        raise ValueError("retry-after-partial-commit needs at least 3 days of data")
    return Experiment(
        name="retry-after-partial-commit",
        description=(
            "The run of day 2 is killed, hard, right after bronze wrote the day's order lines "
            "and before Dagster recorded the step as done. Dagster retries the run from the "
            "failed step, as its retry policy says."
        ),
        spec=spec,
        faults=(RetryAfterPartialCommit(partition=spec.start_date + timedelta(days=1)),),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def idempotency(spec: DatasetSpec) -> Experiment:
    """No fault: every day runs clean, then day 2 is run again, as an operator would."""
    if spec.days < 3:
        raise ValueError("idempotency needs at least 3 days of data")
    return Experiment(
        name="idempotency",
        description=(
            "Nothing breaks. Every day runs as usual, then day 2, long processed, is run "
            "again, as an operator does after an incident or a fix. A reader should see "
            "no difference."
        ),
        spec=spec,
        faults=(),
        properties=(Idempotency(rerun=spec.start_date + timedelta(days=1)), NoSilentCorruption()),
    )


BUILTIN: dict[str, Callable[[DatasetSpec], Experiment]] = {
    "pos-offline-sync": pos_offline_sync,
    "duplicate-file": duplicate_file,
    "late-file": late_file,
    "truncated-file": truncated_file,
    "column-rename": column_rename,
    "crash-after-landing": crash_after_landing,
    "crash-mid-write": crash_mid_write,
    "retry-after-partial-commit": retry_after_partial_commit,
    "idempotency": idempotency,
}
