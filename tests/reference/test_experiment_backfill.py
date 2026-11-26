"""A history load, on both editions: four days delivered, then one Dagster backfill.

Nothing breaks. Every day reaches the source before anything runs, then Dagster
backfills the four days in one request: one run per day, side by side (the run
queue allows it). The tables must equal those of running the days one by one.

Naive: wrong every time, but not the same way twice. Side by side, the runs'
extracts share dlt's cursor and working files: runs fail on files another run
moved, and the ones that started before any cursor was saved each load the
whole history again (measured: everything once, twice or three times). Even
when every run is green, gold is off: it nets change events, and a history
load only sees each order's final state. So the verdict is SILENT_CORRUPTION
when no run happened to fail, BROKEN when one did; both are measured.

Hardened: its extracts take a lock on the source, one at a time; loads and dbt
still run side by side, and keyed silver plus gold from final state give the
daily answer: PASS, every run green.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, experiment, reference_target
from shared_baseline import Baselines

from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path, baselines: Baselines | None = None) -> ExperimentResult:
    result = run_experiment(
        experiment("backfill"),
        reference_target(edition),
        work_dir,
        baseline=baselines(edition) if baselines else None,
    )
    print(result.render())
    return result


class TestNaive:
    def test_a_backfill_never_gives_the_daily_answer(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path, baselines)

        # Which of the two depends on whether a run lost the race for dlt's files.
        assert result.outcome in (Outcome.SILENT_CORRUPTION, Outcome.BROKEN), result.render()
        days = sorted(day for day, _ in result.runs["fault phase"])
        assert days == list(TINY.dates())  # one run per day, all of them the backfill's
        equivalence = next(p for p in result.properties if p.name == "backfill_equivalence")
        assert not equivalence.at_f.holds
        assert any(
            e.startswith("backfill 2026-11-01..2026-11-04: gold_daily_store_sales: ")
            for e in equivalence.at_f.evidence
        ), equivalence.at_f.evidence


@pytest.mark.hardened
class TestHardened:
    def test_a_backfill_gives_the_daily_answer(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path, baselines)

        assert result.outcome is Outcome.PASS, result.render()
        assert all(r.succeeded for _, r in result.runs["fault phase"])  # the lock, not luck
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
