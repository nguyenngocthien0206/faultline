"""The first landing fault, on both editions: day 2's extract lands again under day 3.

faultline copies the landing files byte for byte, under new names, between the
run of day 2 and the run of day 3, as a retried upload would leave them.

Naive: bronze appends every file under dt=day 3, the copies included. Orders
look clean (silver keeps the latest state per order), but order lines and
payments are there twice and gold counts day 2 twice. Rerunning day 3 appends
everything once more. Every run is green: SILENT_CORRUPTION.

Hardened: bronze still holds the copies, but silver keeps one row per key and
gold reads final states, so consumers see exactly the clean numbers: PASS,
without a ledger of loaded files (every POS table has a key).

Each edition is its own class, the unit pytest-xdist schedules.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition, ReferenceAdapter, target_name
from faultline.datagen import DatasetSpec
from faultline.experiments import duplicate_file
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    config = reference_config()
    result = run_experiment(
        duplicate_file(TINY),
        lambda root: ReferenceAdapter(config, root, edition=edition),
        work_dir,
        target=target_name(edition),
    )
    print(result.render())
    return result


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
    def test_absorbs_the_resent_files(self, stack: Stack, tmp_path: Path) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.PASS, result.render()
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
        assert _all_green(result)
