"""The late file fault end to end, on the toy file-based platform (no Docker)."""

from datetime import date
from pathlib import Path

import pytest
from fake_files import FakeFilePlatform

from faultline.datagen import DatasetSpec
from faultline.experiments import late_file
from faultline.outcome import Outcome
from faultline.runner import run_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENT = late_file(TINY)  # order lines of day 2 land just before day 4 runs


def _run(mode: str, work_dir: Path):
    return run_experiment(
        EXPERIMENT, lambda root: FakeFilePlatform(root, mode), work_dir, target=f"files-{mode}"
    )


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_late_file_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path)

    assert result.outcome is expected, result.render()


def test_without_a_look_back_the_lines_are_gone_and_nothing_failed(tmp_path: Path) -> None:
    result = _run("append", tmp_path)

    loss = next(p for p in result.properties if p.name == "no_data_loss")
    assert not loss.at_f.holds
    assert any(
        e.startswith("silver_order_items: complete_keys violated") for e in loss.at_f.evidence
    )
    assert not any(e.startswith("silver_orders:") for e in loss.at_f.evidence)  # orders are fine
    assert all(r.succeeded for runs in result.runs.values() for _, r in runs)


def test_the_day_runs_in_two_halves_with_the_file_held_between(tmp_path: Path) -> None:
    result = _run("append", tmp_path)

    fault_days = [d for d, _ in result.runs["fault phase"]]
    assert fault_days.count(date(2026, 11, 2)) == 2  # ingest, then load
    assert result.faults == (
        "the pos/order_items file of 2026-11-02 lands late, just before the run of 2026-11-04",
    )
