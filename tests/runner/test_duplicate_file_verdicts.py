"""The duplicate file fault end to end, on a toy file-based platform (no Docker)."""

from datetime import date
from pathlib import Path

import pytest
from fake_files import FakeFilePlatform

from faultline.datagen import DatasetSpec
from faultline.faults.landing import DuplicateFile
from faultline.outcome import Outcome
from faultline.properties.core import NoDataLoss, NoSilentCorruption
from faultline.runner import Experiment, run_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENT = Experiment(
    name="duplicate-file",
    description="Day 2's extract lands again under day 3.",
    spec=TINY,
    faults=(
        DuplicateFile(
            original_day=date(2026, 11, 2),
            redelivered_on=date(2026, 11, 3),
            datasets=("pos/orders", "pos/order_items"),  # all the toy platform lands
        ),
    ),
    properties=(NoDataLoss(), NoSilentCorruption()),
)


def _run(mode: str, work_dir: Path):
    return run_experiment(
        EXPERIMENT, lambda root: FakeFilePlatform(root, mode), work_dir, target=f"files-{mode}"
    )


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_fault_two_loaders(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path)

    assert result.outcome is expected, result.render()


def test_appending_loses_nothing_but_counts_lines_twice(tmp_path: Path) -> None:
    result = _run("append", tmp_path)

    loss, corruption = result.properties
    assert loss.at_f.holds  # every key is there...
    assert loss.at_r.holds
    assert not corruption.at_f.holds  # ...some of them twice
    assert any(
        e.startswith("silver_order_items:") and "duplicated" in e for e in corruption.at_f.evidence
    ), corruption.at_f.evidence
    assert not any(e.startswith("silver_orders:") for e in corruption.at_f.evidence)
