from datetime import date

import pytest

from faultline.adapters.base import Capability, Stage
from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.landing import TruncatedFile
from faultline.schedule import (
    Deliver,
    ReleaseLandingFiles,
    Run,
    TruncateLandingFiles,
    clean_schedule,
)

TINY = DatasetSpec.from_profile("tiny")
D3, D4 = date(2026, 11, 3), date(2026, 11, 4)
LINES = "pos/order_items"


def test_the_day_is_cut_between_its_ingest_and_its_load() -> None:
    schedule = TruncatedFile(partition=D4).apply(clean_schedule(TINY))

    assert schedule[-5:] == (
        Run(D3),
        Deliver(D4),
        Run(D4, Stage.INGEST),
        TruncateLandingFiles(LINES, D4, 0.8),
        Run(D4, Stage.LOAD),
    )


def test_recovery_redelivers_the_full_file_and_reruns_the_day() -> None:
    fault = TruncatedFile(partition=D4)

    assert fault.heal() == [ReleaseLandingFiles(LINES, D4)]
    assert fault.affected_partitions() == {D4}


def test_it_is_a_landing_fault_that_needs_stages_and_landing_files() -> None:
    fault = TruncatedFile(partition=D4, keep=0.5)

    assert fault.injection_point is InjectionPoint.LANDING
    assert fault.requires == {Capability.LANDING_FILES, Capability.RUN_STAGES}
    assert fault.describe() == (
        "the pos/order_items file of 2026-11-04 is cut to 50% of its rows, on a row boundary"
    )


@pytest.mark.parametrize("keep", [0, 1, 1.2])
def test_keep_is_a_share_strictly_between_none_and_all(keep: float) -> None:
    with pytest.raises(FaultSetupError, match="between 0 and 1"):
        TruncatedFile(partition=D4, keep=keep)


def test_the_day_must_be_in_the_schedule() -> None:
    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        TruncatedFile(partition=date(2026, 11, 9)).apply(clean_schedule(TINY))


def test_an_empty_dataset_list_is_refused() -> None:
    with pytest.raises(FaultSetupError, match="at least one dataset"):
        TruncatedFile(partition=D4, datasets=())


def test_the_built_in_experiment_cuts_the_last_day() -> None:
    (fault,) = BUILTIN["truncated-file"](TINY).faults

    assert isinstance(fault, TruncatedFile)
    assert (fault.partition, fault.keep, fault.datasets) == (TINY.end_date, 0.8, (LINES,))
