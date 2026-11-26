"""The column drift fault end to end, on the toy file-based platform (no Docker)."""

from pathlib import Path

import pytest
from fake_files import files_target

from faultline.adapters.base import RunStatus
from faultline.datagen import DatasetSpec
from faultline.faults.landing import ColumnDrift
from faultline.outcome import Outcome
from faultline.properties.core import NoDataLoss, NoSilentCorruption
from faultline.runner import Experiment, run_experiment

TINY = DatasetSpec.from_profile("tiny")


def _experiment(move_to_end: bool) -> Experiment:
    return Experiment(
        name="column-rename",
        description="The last day's order lines arrive with qty renamed quantity.",
        spec=TINY,
        faults=(
            ColumnDrift(
                partition=TINY.end_date,
                dataset="pos/order_items",  # what the toy platform lands
                old="qty",
                new="quantity",
                move_to_end=move_to_end,
            ),
        ),
        properties=(NoDataLoss(), NoSilentCorruption()),
    )


def _run(mode: str, move_to_end: bool, work_dir: Path):
    return run_experiment(_experiment(move_to_end), files_target(mode), work_dir)


@pytest.mark.parametrize("move_to_end", [False, True])
@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.DETECTED)]
)
def test_one_renamed_column_two_platforms(
    mode: str, expected: Outcome, move_to_end: bool, tmp_path: Path
) -> None:
    result = _run(mode, move_to_end, tmp_path)

    assert result.outcome is expected, result.render()


def test_reading_by_name_turns_the_missing_column_into_zeros_and_nothing_fails(
    tmp_path: Path,
) -> None:
    result = _run("append", False, tmp_path)

    loss, corruption = result.properties
    assert loss.at_f.holds  # every line is there...
    assert not corruption.at_f.holds  # ...with qty 0
    assert any("changed" in e and "qty" in e for e in corruption.at_f.evidence)
    assert all(r.succeeded for runs in result.runs.values() for _, r in runs)


def test_a_contract_refuses_the_file_and_the_agreed_schema_heals_it(tmp_path: Path) -> None:
    result = _run("keyed", False, tmp_path)

    (run,) = [r for _, r in result.runs["fault phase"] if r.status is RunStatus.FAILED]
    assert any("missing qty" in e for e in run.errors), run.errors
    assert any("unexpected quantity" in e for e in run.errors), run.errors
    for prop in result.properties:
        assert prop.at_r.holds, prop.at_r.evidence
