"""Rerunning a processed day end to end, on the toy file-based platform (no Docker)."""

from datetime import date
from pathlib import Path

import pytest
from fake_files import files_target

from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.properties.core import Idempotency
from faultline.runner import run_experiment
from faultline.spec import SpecError, load_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENTS = Path(__file__).resolve().parents[2] / "experiments"
# four clean days, then day 2 again
EXPERIMENT = load_experiment(EXPERIMENTS / "idempotency.yaml")


def test_the_built_in_experiment_has_no_fault_and_reruns_day_two() -> None:
    experiment = load_experiment(EXPERIMENTS / "idempotency.yaml")

    assert experiment.faults == ()
    (probe, *_) = experiment.properties
    assert isinstance(probe, Idempotency)
    assert probe.rerun == date(2026, 11, 2)
    with pytest.raises(SpecError, match="day 2 is outside a 1-day dataset"):
        load_experiment(EXPERIMENTS / "idempotency.yaml", dataset={"days": 1})


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_rerun_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = run_experiment(EXPERIMENT, files_target(mode), tmp_path)

    assert result.outcome is expected, result.render()


def test_an_appending_platform_counts_the_rerun_day_twice(tmp_path: Path) -> None:
    result = run_experiment(EXPERIMENT, files_target("append"), tmp_path)

    rerun = next(p for p in result.properties if p.name == "idempotency")
    assert not rerun.at_f.holds
    assert not rerun.at_r.holds  # nothing reruns it back: the state readers keep
    assert any(
        e.startswith("rerun of 2026-11-02: silver_order_items: ") and "duplicated keys" in e
        for e in rerun.at_f.evidence
    ), rerun.at_f.evidence
