"""Rerunning a processed day end to end, on the toy file-based platform (no Docker)."""

from datetime import date
from pathlib import Path

import pytest
from fake_files import FakeFilePlatform

from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN, idempotency
from faultline.outcome import Outcome
from faultline.properties.core import Idempotency
from faultline.runner import run_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENT = idempotency(TINY)  # four clean days, then day 2 again


def test_the_built_in_experiment_has_no_fault_and_reruns_day_two() -> None:
    experiment = BUILTIN["idempotency"](TINY)

    assert experiment.faults == ()
    (probe, *_) = experiment.properties
    assert isinstance(probe, Idempotency)
    assert probe.rerun == date(2026, 11, 2)
    with pytest.raises(ValueError, match="at least 3 days"):
        idempotency(DatasetSpec.from_profile("tiny", days=2))


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_rerun_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = run_experiment(
        EXPERIMENT, lambda root: FakeFilePlatform(root, mode), tmp_path, target=f"files-{mode}"
    )

    assert result.outcome is expected, result.render()


def test_an_appending_platform_counts_the_rerun_day_twice(tmp_path: Path) -> None:
    result = run_experiment(EXPERIMENT, lambda root: FakeFilePlatform(root, "append"), tmp_path)

    rerun = next(p for p in result.properties if p.name == "idempotency")
    assert not rerun.at_f.holds
    assert not rerun.at_r.holds  # nothing reruns it back: the state readers keep
    assert any(
        e.startswith("rerun of 2026-11-02: silver_order_items: ") and "duplicated keys" in e
        for e in rerun.at_f.evidence
    ), rerun.at_f.evidence
