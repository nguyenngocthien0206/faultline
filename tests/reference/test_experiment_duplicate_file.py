"""The first landing fault, on both editions: day 2's extract lands again under day 3.

faultline copies the landing files byte for byte, under new names, between the
run of day 2 and the run of day 3, as a retried upload would leave them.

Naive: bronze appends every file under dt=day 3, the copies included. Orders
look clean (silver keeps the latest state per order), but order lines and
payments are there twice and gold counts day 2 twice. Rerunning day 3 appends
everything once more. Every run is green: SILENT_CORRUPTION.

Hardened: the copies are in no load manifest, so bronze refuses day 3 and the
run fails, loudly; day 4's lag window reads day 3's rows again from the source,
and silver keeps one row per key, so consumers see exactly the clean numbers:
PASS, with the red run as the notice. No ledger of loaded files is needed.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, experiment, reference_target

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    result = run_experiment(experiment("duplicate-file"), reference_target(edition), work_dir)
    print(result.render())
    return result


def _failed(result: ExperimentResult, phase: str) -> list:
    return [(d, r) for d, r in result.runs[phase] if r.status is RunStatus.FAILED]


def _all_green(result: ExperimentResult) -> bool:
    return all(r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs)


class TestNaive:
    def test_counts_the_resent_files_again_without_a_word(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        corruption = next(p for p in result.properties if p.name == "no_silent_corruption")
        assert loss.at_f.holds  # nothing is missing...
        assert not corruption.at_f.holds  # ...some of it is there twice
        assert not corruption.at_r.holds
        evidence = corruption.at_f.evidence
        assert any(e.startswith("silver_order_items:") and "duplicated" in e for e in evidence)
        assert not any(e.startswith("silver_orders:") for e in evidence)  # orders look clean
        assert _all_green(result)


@pytest.mark.hardened
class TestHardened:
    def test_refuses_the_resent_files_and_keeps_the_numbers(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.PASS, result.render()
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
        failed = _failed(result, "fault phase")
        assert [day for day, _ in failed] == [date(2026, 11, 3)]  # the day the copies sit in
        assert all(any("stray file" in e for e in r.errors) for _, r in failed)
