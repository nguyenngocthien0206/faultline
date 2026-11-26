"""A crash inside the gold refresh, on both editions: day 3 dies mid-write.

Day 3's run is killed, hard, while dbt refreshes gold_daily_store_sales,
between the strategy's statements (a failpoint in the adapter's macros,
reference/dbt/macros/failpoint_mid_write.sql). Day 3 is the first extract that
touches an older day: refunds of day 1 orders.

Naive (delete_insert): the day 1 rows of the stores the refunds touched are
deleted, the insert never runs. Readers lose rows they could see a moment ago,
and days 4 and on never touch those keys again: SILENT_CORRUPTION, with
no_vanishing_rows violated at F. Rerunning day 3 puts them back.

Hardened (insert_overwrite): the new rows are built aside and never swapped in,
the old ones stay where they were, and day 4 rebuilds the days it touches from
final state: PASS.

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
CRASHED = date(2026, 11, 3)


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    result = run_experiment(experiment("crash-mid-write"), reference_target(edition), work_dir)
    print(result.render())
    return result


def _statuses(result: ExperimentResult, phase: str) -> dict[date, list[RunStatus]]:
    statuses: dict[date, list[RunStatus]] = {}
    for day, run in result.runs[phase]:
        statuses.setdefault(day, []).append(run.status)
    return statuses


class TestNaive:
    def test_rows_vanish_from_gold_and_nothing_says_so(self, stack: Stack, tmp_path: Path) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert _statuses(result, "fault phase")[CRASHED] == [RunStatus.INTERRUPTED]
        vanishing = next(p for p in result.properties if p.name == "no_vanishing_rows")
        assert not vanishing.at_f.holds
        assert any(
            e.startswith("gold_daily_store_sales: ") and "keys vanished" in e
            for e in vanishing.at_f.evidence
        ), vanishing.at_f.evidence
        assert vanishing.at_r.holds  # the rerun only adds back


@pytest.mark.hardened
class TestHardened:
    def test_the_old_rows_stay_until_the_new_ones_are_swapped_in(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.PASS, result.render()
        assert _statuses(result, "fault phase")[CRASHED] == [RunStatus.INTERRUPTED]
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
