"""A retry after a partial commit, on both editions: day 2 dies after bronze, is retried.

Day 2's run is killed, hard, right after bronze wrote the day's order lines,
before Dagster recorded the step as done. Dagster's daemon retries the run
from the failed step (asked for by this run alone, `dagster/max_retries`),
and the retry goes green. The crash is faultline's own (INTERRUPTED), so the
day's run counts as green.

Naive: bronze appends, so the retry writes day 2's order lines a second time,
and nothing ever says so: SILENT_CORRUPTION.

Hardened: bronze replaces the day's partition, so writing it again changes
nothing: PASS.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunResult
from faultline.adapters.reference import Edition, ReferenceAdapter, target_name
from faultline.datagen import DatasetSpec
from faultline.experiments import retry_after_partial_commit
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")
CRASHED = date(2026, 11, 2)


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    config = reference_config()
    result = run_experiment(
        retry_after_partial_commit(TINY),
        lambda root: ReferenceAdapter(config, root, edition=edition),
        work_dir,
        target=target_name(edition),
    )
    print(result.render())
    return result


def _crashed_run(result: ExperimentResult) -> RunResult:
    (run,) = [r for day, r in result.runs["fault phase"] if day == CRASHED]
    return run


class TestNaive:
    def test_the_retry_writes_the_day_twice_and_nothing_says_so(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        run = _crashed_run(result)
        assert run.succeeded
        assert run.metadata["statuses"] == "INTERRUPTED,SUCCEEDED"
        corruption = next(p for p in result.properties if p.name == "no_silent_corruption")
        assert any(
            e.startswith("silver_order_items: ") and "duplicated keys" in e
            for e in corruption.at_f.evidence
        ), corruption.at_f.evidence


@pytest.mark.hardened
class TestHardened:
    def test_writing_the_day_again_changes_nothing(self, stack: Stack, tmp_path: Path) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.PASS, result.render()
        run = _crashed_run(result)
        assert run.metadata["statuses"] == "INTERRUPTED,SUCCEEDED"
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
