from datetime import date

import pytest

from faultline.adapters.base import Capability
from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN, retry_after_partial_commit
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.execution import RetryAfterPartialCommit
from faultline.schedule import Deliver, Run, clean_schedule

TINY = DatasetSpec.from_profile("tiny")
D1, D2, D3 = date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)


def test_the_days_run_crashes_after_bronze_and_may_be_retried_once() -> None:
    schedule = RetryAfterPartialCommit(partition=D2).apply(clean_schedule(TINY))

    assert schedule[:6] == (
        Deliver(D1),
        Run(D1),
        Deliver(D2),
        Run(D2, failpoint="after_bronze_write", retries=1),
        Deliver(D3),
        Run(D3),  # every other run: no failpoint, no retry
    )


def test_it_needs_failpoints_and_retries() -> None:
    fault = RetryAfterPartialCommit(partition=D2)

    assert fault.injection_point is InjectionPoint.EXECUTION
    assert fault.requires == {Capability.FAILPOINTS, Capability.RETRIES}
    assert fault.heal() == []
    assert fault.affected_partitions() == {D2}
    assert fault.describe() == (
        "the run of 2026-11-02 dies right after writing bronze, before the "
        "orchestrator records it, and the orchestrator retries it"
    )


def test_the_day_must_be_in_the_schedule() -> None:
    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        RetryAfterPartialCommit(partition=date(2026, 11, 9)).apply(clean_schedule(TINY))


def test_the_built_in_experiment_crashes_day_two() -> None:
    (fault,) = BUILTIN["retry-after-partial-commit"](TINY).faults

    assert isinstance(fault, RetryAfterPartialCommit)
    assert fault.partition == D2
    with pytest.raises(ValueError, match="at least 3 days"):
        retry_after_partial_commit(DatasetSpec.from_profile("tiny", days=2))
