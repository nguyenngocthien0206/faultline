"""The crash after landing end to end, on the toy file-based platform (no Docker)."""

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
    EXPERIMENTS / "crash-after-landing.yaml"
)  # day 2 dies between landing and loading


def _run(mode: str, work_dir: Path):
    return run_experiment(EXPERIMENT, files_target(mode), work_dir)


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_crash_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path)

    assert result.outcome is expected, result.render()


def test_the_crash_is_faultlines_and_does_not_count_as_a_signal(tmp_path: Path) -> None:
    result = _run("append", tmp_path)

    statuses = [r.status for _, r in result.runs["fault phase"]]
    assert statuses.count(RunStatus.INTERRUPTED) == 1
    assert RunStatus.FAILED not in statuses  # nothing the platform itself raised
    loss = next(p for p in result.properties if p.name == "no_data_loss")
    assert not loss.at_f.holds  # day 2 never loaded...
    assert loss.at_r.holds  # ...until the operator reran it
