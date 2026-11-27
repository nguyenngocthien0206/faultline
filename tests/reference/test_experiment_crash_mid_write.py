"""A crash inside the gold refresh, on both editions: day 3 dies mid-write.

Day 3's run is killed, hard, while dbt refreshes gold_daily_store_sales,
between the strategy's statements (a failpoint in the adapter's macros,
reference/dbt/macros/failpoint_mid_write.sql). Day 3 is the first extract that
touches an older day: refunds of day 1 orders.

Naive (delete_insert): the day 1 rows of the stores the refunds touched are
deleted, the insert never runs. Readers lose rows they could see a moment ago,
and days 4 and on never touch those keys again: SILENT_CORRUPTION, with
no_vanishing_rows violated at F and gold half written right after the crash
(atomicity). Rerunning day 3 puts the rows back.

Hardened (insert_overwrite): the new rows are built aside and never swapped in,
the old ones stay where they were, and day 4 rebuilds the days it touches from
final state: no row is lost. But silver (views over bronze) and the payments
report already show day 3, and the sales report does not: until the next run
the two gold tables disagree, and nothing says so (atomicity): SILENT_CORRUPTION.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, experiment, keys_off, reference_target
from shared_baseline import Baselines

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")
CRASHED = date(2026, 11, 3)


def _run(edition: Edition, work_dir: Path, baselines: Baselines | None = None) -> ExperimentResult:
    result = run_experiment(
        experiment("crash-mid-write"),
        reference_target(edition),
        work_dir,
        baseline=baselines(edition) if baselines else None,
    )
    print(result.render())
    return result


def _statuses(result: ExperimentResult, phase: str) -> dict[date, list[RunStatus]]:
    statuses: dict[date, list[RunStatus]] = {}
    for day, run in result.runs[phase]:
        statuses.setdefault(day, []).append(run.status)
    return statuses


class TestNaive:
    def test_rows_vanish_from_gold_and_nothing_says_so(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path, baselines)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert keys_off(result)[-1] > 0  # three rounds of reruns never bring it back
        assert len(keys_off(result)) == 3
        assert _statuses(result, "fault phase")[CRASHED] == [RunStatus.INTERRUPTED]
        vanishing = next(p for p in result.properties if p.name == "no_vanishing_rows")
        assert not vanishing.at_f.holds
        assert any(
            e.startswith("gold_daily_store_sales: ") and "keys vanished" in e
            for e in vanishing.at_f.evidence
        ), vanishing.at_f.evidence
        assert vanishing.at_r.holds  # the rerun only adds back
        atomicity = next(p for p in result.properties if p.name == "atomicity")
        assert any(
            e.startswith("crash on 2026-11-03: half written, gold_daily_store_sales: ")
            for e in atomicity.at_f.evidence
        ), atomicity.at_f.evidence


@pytest.mark.hardened
class TestHardened:
    def test_no_row_is_lost_but_the_gold_tables_disagree_until_the_next_run(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path, baselines)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert keys_off(result) == [0]  # healed by the first round of recovery
        assert _statuses(result, "fault phase")[CRASHED] == [RunStatus.INTERRUPTED]
        atomicity = next(p for p in result.properties if p.name == "atomicity")
        assert atomicity.at_f.evidence == (
            "crash on 2026-11-03: tables disagree: silver_orders, silver_order_items, "
            "silver_payments, gold_daily_payments show the day, "
            "gold_daily_store_sales does not",
        )
        for prop in result.properties:  # only the moment of the crash was wrong
            if prop.name != "atomicity":
                assert prop.at_f.holds, prop.at_f.evidence
                assert prop.at_r.holds, prop.at_r.evidence
