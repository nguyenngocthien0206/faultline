"""The retry after a partial commit end to end, on the toy file-based platform (no Docker)."""

from pathlib import Path

import pytest
from fake_files import files_target

from faultline.adapters.base import RunStatus
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment
from faultline.spec import load_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENTS = Path(__file__).resolve().parents[2] / "experiments"
EXPERIMENT = load_experiment(
    EXPERIMENTS / "retry-after-partial-commit.yaml"
)  # day 2 dies after loading, then is retried


def _run(mode: str, work_dir: Path, lines: list[str]) -> ExperimentResult:
    return run_experiment(
        EXPERIMENT,
        files_target(mode),
        work_dir,
        log=lines.append,
    )


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_retry_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path, [])

    assert result.outcome is expected, result.render()


def test_the_retry_goes_green_and_writes_the_lines_twice(tmp_path: Path) -> None:
    lines: list[str] = []
    result = _run("append", tmp_path, lines)

    statuses = [r.status for _, r in result.runs["fault phase"]]
    assert statuses == [RunStatus.SUCCEEDED] * 4  # the crash was retried away
    assert "  [fault] run 2026-11-02 (crash after_bronze_write, 2 attempts): SUCCEEDED" in lines
    corruption = next(p for p in result.properties if p.name == "no_silent_corruption")
    assert any(
        e.startswith("silver_order_items: ") and "duplicated keys" in e
        for e in corruption.at_f.evidence
    ), corruption.at_f.evidence
