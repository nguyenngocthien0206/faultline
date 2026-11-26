"""The crash mid write end to end, on the toy file-based platform (no Docker)."""

from pathlib import Path

import pytest
from fake_files import files_target

from faultline.adapters.base import RunStatus
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import run_experiment
from faultline.spec import load_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENTS = Path(__file__).resolve().parents[2] / "experiments"
EXPERIMENT = load_experiment(
    EXPERIMENTS / "crash-mid-write.yaml"
)  # day 3 dies inside the gold refresh


def _run(mode: str, work_dir: Path):
    return run_experiment(EXPERIMENT, files_target(mode), work_dir)


def _vanishing(result):
    return next(p for p in result.properties if p.name == "no_vanishing_rows")


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_crash_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path)

    assert result.outcome is expected, result.render()


def test_a_delete_then_insert_refresh_leaves_rows_gone(tmp_path: Path) -> None:
    result = _run("append", tmp_path)

    statuses = [r.status for _, r in result.runs["fault phase"]]
    assert statuses.count(RunStatus.INTERRUPTED) == 1
    assert RunStatus.FAILED not in statuses
    vanishing = _vanishing(result)
    assert not vanishing.at_f.holds
    assert vanishing.at_f.evidence[0].startswith("gold_daily_sales: ")
    assert "keys vanished" in vanishing.at_f.evidence[0]


def test_a_refresh_swapped_in_whole_loses_nothing(tmp_path: Path) -> None:
    vanishing = _vanishing(_run("keyed", tmp_path))

    assert vanishing.at_f.holds
    assert vanishing.at_r.holds
