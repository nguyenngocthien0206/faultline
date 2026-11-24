from datetime import date

import pytest

from faultline.adapters.base import Capability, Stage
from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN
from faultline.faults.base import FaultSetupError, InjectionPoint
from faultline.faults.landing import ColumnDrift
from faultline.schedule import (
    Deliver,
    ReleaseLandingFiles,
    RenameLandingColumn,
    Run,
    clean_schedule,
)

TINY = DatasetSpec.from_profile("tiny")
D3, D4 = date(2026, 11, 3), date(2026, 11, 4)
PAYMENTS = "pos/payments"


def test_the_column_is_renamed_between_the_days_ingest_and_its_load() -> None:
    schedule = ColumnDrift(partition=D4).apply(clean_schedule(TINY))

    assert schedule[-5:] == (
        Run(D3),
        Deliver(D4),
        Run(D4, Stage.INGEST),
        RenameLandingColumn(PAYMENTS, D4, "amount_vnd", "total_amount_vnd"),
        Run(D4, Stage.LOAD),
    )


def test_it_can_also_move_the_column_to_the_end() -> None:
    schedule = ColumnDrift(partition=D4, move_to_end=True).apply(clean_schedule(TINY))

    assert (
        RenameLandingColumn(PAYMENTS, D4, "amount_vnd", "total_amount_vnd", move_to_end=True)
        in schedule
    )


def test_recovery_redelivers_the_agreed_schema_and_reruns_the_day() -> None:
    fault = ColumnDrift(partition=D4)

    assert fault.heal() == [ReleaseLandingFiles(PAYMENTS, D4)]
    assert fault.affected_partitions() == {D4}


def test_it_is_a_landing_fault_that_needs_stages_and_landing_files() -> None:
    fault = ColumnDrift(partition=D4, move_to_end=True)

    assert fault.injection_point is InjectionPoint.LANDING
    assert fault.requires == {Capability.LANDING_FILES, Capability.RUN_STAGES}
    assert fault.describe() == (
        "the pos/payments file of 2026-11-04 arrives with amount_vnd renamed "
        "total_amount_vnd, moved to the end"
    )


@pytest.mark.parametrize(("old", "new"), [("amount_vnd", "amount_vnd"), ("", "x"), ("x", "")])
def test_a_rename_needs_two_different_names(old: str, new: str) -> None:
    with pytest.raises(FaultSetupError, match="two different, non-empty"):
        ColumnDrift(partition=D4, old=old, new=new)


def test_the_day_must_be_in_the_schedule() -> None:
    with pytest.raises(FaultSetupError, match="2026-11-09 is never run"):
        ColumnDrift(partition=date(2026, 11, 9)).apply(clean_schedule(TINY))


def test_the_built_in_experiment_renames_the_last_days_payment_amounts() -> None:
    (fault,) = BUILTIN["column-rename"](TINY).faults

    assert isinstance(fault, ColumnDrift)
    assert (fault.partition, fault.dataset, fault.old, fault.new, fault.move_to_end) == (
        TINY.end_date,
        PAYMENTS,
        "amount_vnd",
        "total_amount_vnd",
        False,
    )
