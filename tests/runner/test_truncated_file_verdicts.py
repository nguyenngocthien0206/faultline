"""The truncated file fault end to end, on the toy file-based platform (no Docker)."""

from pathlib import Path

import pytest
from fake_files import FakeFilePlatform

from faultline.adapters.base import RunStatus
from faultline.datagen import DatasetSpec
from faultline.experiments import truncated_file
from faultline.outcome import Outcome
from faultline.runner import run_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENT = truncated_file(TINY)  # the last day's order lines cut to 80%


def _run(mode: str, work_dir: Path):
    return run_experiment(
        EXPERIMENT, lambda root: FakeFilePlatform(root, mode), work_dir, target=f"files-{mode}"
    )


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.DETECTED)]
)
def test_one_cut_file_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path)

    assert result.outcome is expected, result.render()


def test_without_a_manifest_the_cut_goes_unnoticed(tmp_path: Path) -> None:
    result = _run("append", tmp_path)

    loss = next(p for p in result.properties if p.name == "no_data_loss")
    assert not loss.at_f.holds
    assert all(r.succeeded for runs in result.runs.values() for _, r in runs)


def test_with_a_manifest_the_load_fails_and_the_full_file_heals_it(tmp_path: Path) -> None:
    result = _run("keyed", tmp_path)

    failed = [r for _, r in result.runs["fault phase"] if r.status is RunStatus.FAILED]
    (run,) = failed
    assert any("promised" in e for e in run.errors), run.errors
    assert all(r.succeeded for _, r in result.runs["recovery"])  # redelivered in full
    for prop in result.properties:
        assert prop.at_r.holds, prop.at_r.evidence
