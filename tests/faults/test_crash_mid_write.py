from datetime import date

import pytest

from faultline.adapters.base import Capability
from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN, crash_mid_write
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.execution import CrashMidWrite
from faultline.properties.core import NoVanishingRows
from faultline.schedule import Deliver, Run, Snapshot, clean_schedule

TINY = DatasetSpec.from_profile("tiny")
D2, D3, D4 = date(2026, 11, 2), date(2026, 11, 3), date(2026, 11, 4)


def test_the_days_run_crashes_mid_write_between_two_snapshots() -> None:
    schedule = CrashMidWrite(partition=D3).apply(clean_schedule(TINY))

    assert schedule[3:10] == (
        Run(D2),
        Deliver(D3),
        Snapshot("before_crash"),
        Run(D3, failpoint="gold_mid_write"),
        Snapshot("after_crash"),
        Deliver(D4),
        Run(D4),
    )


def test_it_is_an_execution_fault_that_needs_failpoints() -> None:
    fault = CrashMidWrite(partition=D3)

    assert fault.injection_point is InjectionPoint.EXECUTION
    assert fault.requires == {Capability.FAILPOINTS}
    assert fault.heal() == []
    assert fault.affected_partitions() == {D3}
    assert fault.describe() == (
        "the run of 2026-11-03 dies while refreshing gold, "
        "between taking the old rows out and putting the new ones in"
    )


def test_the_day_must_be_in_the_schedule() -> None:
    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        CrashMidWrite(partition=date(2026, 11, 9)).apply(clean_schedule(TINY))


def test_the_built_in_experiment_crashes_day_three_and_watches_for_vanishing_rows() -> None:
    experiment = BUILTIN["crash-mid-write"](TINY)
    (fault,) = experiment.faults

    assert isinstance(fault, CrashMidWrite)
    assert fault.partition == D3  # the first extract that touches an older day
    assert any(isinstance(p, NoVanishingRows) for p in experiment.properties)
    with pytest.raises(ValueError, match="at least 3 days"):
        crash_mid_write(DatasetSpec.from_profile("tiny", days=2))
