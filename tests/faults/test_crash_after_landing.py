from datetime import date
from pathlib import Path

import pytest

from faultline.adapters.base import Capability
from faultline.datagen import DatasetSpec
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.execution import CrashAfterLanding
from faultline.schedule import Deliver, Run, Snapshot, clean_schedule
from faultline.spec import SpecError, load_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENTS = Path(__file__).resolve().parents[2] / "experiments"
D1, D2, D3 = date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 3)


def test_only_the_days_run_is_asked_to_crash_with_a_snapshot_on_either_side() -> None:
    schedule = CrashAfterLanding(partition=D2).apply(clean_schedule(TINY))

    assert schedule[:8] == (
        Deliver(D1),
        Run(D1),
        Deliver(D2),
        Snapshot("before_crash"),
        Run(D2, failpoint="after_landing"),
        Snapshot("after_crash"),
        Deliver(D3),
        Run(D3),
    )


def test_it_is_an_execution_fault_that_needs_failpoints() -> None:
    fault = CrashAfterLanding(partition=D2)

    assert fault.injection_point is InjectionPoint.EXECUTION
    assert fault.requires == {Capability.FAILPOINTS}
    assert fault.heal() == []
    assert fault.affected_partitions() == {D2}
    assert fault.describe() == (
        "the run of 2026-11-02 dies right after landing its files, before anything is loaded"
    )


def test_the_day_must_be_in_the_schedule() -> None:
    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        CrashAfterLanding(partition=date(2026, 11, 9)).apply(clean_schedule(TINY))


def test_the_built_in_experiment_crashes_day_two() -> None:
    (fault,) = load_experiment(EXPERIMENTS / "crash-after-landing.yaml").faults

    assert isinstance(fault, CrashAfterLanding)
    assert fault.partition == D2
    with pytest.raises(SpecError, match="day 2 is outside a 1-day dataset"):
        load_experiment(EXPERIMENTS / "crash-after-landing.yaml", dataset={"days": 1})
