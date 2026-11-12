from datetime import date

import pytest

from faultline.adapters.base import Capability
from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN, duplicate_file
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.landing import POS_DATASETS, DuplicateFile
from faultline.schedule import CopyLandingFiles, Deliver, Run, clean_schedule

TINY = DatasetSpec.from_profile("tiny")
D1, D2, D3, D4 = (date(2026, 11, d) for d in (1, 2, 3, 4))


def test_the_copies_land_right_before_the_later_run() -> None:
    schedule = DuplicateFile(original_day=D2, redelivered_on=D3).apply(clean_schedule(TINY))

    copies = tuple(CopyLandingFiles(d, D2, D3) for d in POS_DATASETS)
    before = (Deliver(D1), Run(D1), Deliver(D2), Run(D2), Deliver(D3))
    assert schedule == (*before, *copies, Run(D3), Deliver(D4), Run(D4))


def test_it_is_a_landing_fault_that_needs_landing_files() -> None:
    fault = DuplicateFile(original_day=D2, redelivered_on=D3)

    assert fault.injection_point is InjectionPoint.LANDING
    assert fault.requires == {Capability.LANDING_FILES}
    assert fault.affected_partitions() == {D3}  # the day an operator would rerun
    assert (
        fault.describe()
        == "the extract of 2026-11-02 lands again under 2026-11-03, as a retried upload"
    )


def test_one_dataset_can_be_singled_out() -> None:
    fault = DuplicateFile(original_day=D1, redelivered_on=D4, datasets=("pos/order_items",))

    schedule = fault.apply(clean_schedule(TINY))

    assert [a for a in schedule if isinstance(a, CopyLandingFiles)] == [
        CopyLandingFiles("pos/order_items", D1, D4)
    ]
    assert fault.describe().startswith("pos/order_items of 2026-11-01")


@pytest.mark.parametrize(
    ("original", "redelivered", "message"),
    [
        (D3, D3, "must come after"),
        (D3, D2, "must come after"),
    ],
)
def test_the_resend_comes_after_the_original(
    original: date, redelivered: date, message: str
) -> None:
    with pytest.raises(FaultSetupError, match=message):
        DuplicateFile(original_day=original, redelivered_on=redelivered)


def test_both_days_must_be_in_the_schedule() -> None:
    fault = DuplicateFile(original_day=D2, redelivered_on=date(2026, 11, 9))

    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        fault.apply(clean_schedule(TINY))


def test_an_empty_dataset_list_is_refused() -> None:
    with pytest.raises(FaultSetupError, match="at least one dataset"):
        DuplicateFile(original_day=D2, redelivered_on=D3, datasets=())


def test_the_built_in_experiment_resends_day_two_under_day_three() -> None:
    experiment = BUILTIN["duplicate-file"](TINY)

    (fault,) = experiment.faults
    assert isinstance(fault, DuplicateFile)
    assert (fault.original_day, fault.redelivered_on) == (D2, D3)
    with pytest.raises(ValueError, match="at least 3 days"):
        duplicate_file(DatasetSpec.from_profile("tiny", days=2))
