from datetime import date

import pytest

from faultline.adapters.base import Capability, Stage
from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN, late_file
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.landing import LateFile
from faultline.schedule import Deliver, HoldLandingFiles, ReleaseLandingFiles, Run, clean_schedule

TINY = DatasetSpec.from_profile("tiny")
D1, D2, D3, D4 = (date(2026, 11, d) for d in (1, 2, 3, 4))
LINES = "pos/order_items"


def test_the_day_splits_around_the_held_file_which_lands_before_the_later_run() -> None:
    schedule = LateFile(partition=D2, lands_before=D4).apply(clean_schedule(TINY))

    assert schedule == (
        Deliver(D1),
        Run(D1),
        Deliver(D2),
        Run(D2, Stage.INGEST),
        HoldLandingFiles(LINES, D2),
        Run(D2, Stage.LOAD),
        Deliver(D3),
        Run(D3),
        Deliver(D4),
        ReleaseLandingFiles(LINES, D2),
        Run(D4),
    )


def test_it_is_a_landing_fault_that_needs_stages_and_landing_files() -> None:
    fault = LateFile(partition=D2, lands_before=D4)

    assert fault.injection_point is InjectionPoint.LANDING
    assert fault.requires == {Capability.LANDING_FILES, Capability.RUN_STAGES}
    assert fault.affected_partitions() == {D2}  # the operator reruns the late file's day
    assert fault.describe() == (
        "the pos/order_items file of 2026-11-02 lands late, just before the run of 2026-11-04"
    )


def test_several_files_can_be_late_together() -> None:
    fault = LateFile(partition=D1, lands_before=D3, datasets=("pos/orders", LINES))

    schedule = fault.apply(clean_schedule(TINY))

    assert [a for a in schedule if isinstance(a, HoldLandingFiles)] == [
        HoldLandingFiles("pos/orders", D1),
        HoldLandingFiles(LINES, D1),
    ]
    assert schedule.index(ReleaseLandingFiles("pos/orders", D1)) < schedule.index(Run(D3))


@pytest.mark.parametrize(("partition", "lands_before"), [(D3, D3), (D3, D2)])
def test_the_file_lands_after_its_own_day(partition: date, lands_before: date) -> None:
    with pytest.raises(FaultSetupError, match="must come after"):
        LateFile(partition=partition, lands_before=lands_before)


def test_both_days_must_be_in_the_schedule() -> None:
    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        LateFile(partition=D2, lands_before=date(2026, 11, 9)).apply(clean_schedule(TINY))


def test_an_empty_dataset_list_is_refused() -> None:
    with pytest.raises(FaultSetupError, match="at least one dataset"):
        LateFile(partition=D2, lands_before=D4, datasets=())


def test_the_built_in_experiment_holds_day_two_until_day_four() -> None:
    (fault,) = BUILTIN["late-file"](TINY).faults

    assert isinstance(fault, LateFile)
    assert (fault.partition, fault.lands_before, fault.datasets) == (D2, D4, (LINES,))
    with pytest.raises(ValueError, match="at least 4 days"):
        late_file(DatasetSpec.from_profile("tiny", days=3))
