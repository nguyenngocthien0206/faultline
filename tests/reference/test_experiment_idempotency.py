"""Rerunning a processed day, on both editions: nothing breaks, day 2 is run again.

Every day runs as usual, then day 2 is run once more, as an operator does after
an incident or a fix, with a snapshot on either side (the idempotency probe).

Naive: bronze appends every landing file of day 2 again, so the day's order
lines and payments are there twice, and the rerun is green: SILENT_CORRUPTION.

Hardened: the lag window extracts again, bronze rebuilds the day's partition,
silver keeps one row per key and gold is built from final state: PASS.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.reference import Edition, ReferenceAdapter, target_name
from faultline.datagen import DatasetSpec
from faultline.experiments import idempotency
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    config = reference_config()
    result = run_experiment(
        idempotency(TINY),
        lambda root: ReferenceAdapter(config, root, edition=edition),
        work_dir,
        target=target_name(edition),
    )
    print(result.render())
    return result


class TestNaive:
    def test_a_rerun_counts_the_day_twice_and_nothing_says_so(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert all(r.succeeded for _, r in result.runs["fault phase"])
        rerun = next(p for p in result.properties if p.name == "idempotency")
        assert any(
            e.startswith("rerun of 2026-11-02: silver_order_items: ") and "duplicated keys" in e
            for e in rerun.at_f.evidence
        ), rerun.at_f.evidence


@pytest.mark.hardened
class TestHardened:
    def test_a_rerun_changes_nothing(self, stack: Stack, tmp_path: Path) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.PASS, result.render()
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
