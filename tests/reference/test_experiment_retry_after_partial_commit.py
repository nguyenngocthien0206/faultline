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
from reference_stack import Stack, experiment, keys_off, reference_target
from shared_baseline import Baselines

from faultline.adapters.base import RunResult
from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")
CRASHED = date(2026, 11, 2)


def _run(edition: Edition, work_dir: Path, baselines: Baselines | None = None) -> ExperimentResult:
    result = run_experiment(
        experiment("retry-after-partial-commit"),
        reference_target(edition),
        work_dir,
        baseline=baselines(edition) if baselines else None,
    )
    print(result.render())
    return result


def _crashed_run(result: ExperimentResult) -> RunResult:
    (run,) = [r for day, r in result.runs["fault phase"] if day == CRASHED]
    return run


class TestNaive:
    def test_the_retry_writes_the_day_twice_and_nothing_says_so(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path, baselines)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert keys_off(result)[-1] > 0  # three rounds of reruns never bring it back
        assert len(keys_off(result)) == 3
        assert result.recovery is not None
        assert result.recovery.summary().endswith("growing")  # every rerun appends again
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
    def test_writing_the_day_again_changes_nothing(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path, baselines)

        assert result.outcome is Outcome.PASS, result.render()
        assert keys_off(result) == [0]  # healed by the first round of recovery
        run = _crashed_run(result)
        assert run.metadata["statuses"] == "INTERRUPTED,SUCCEEDED"
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
