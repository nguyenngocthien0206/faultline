"""The naive POS pipeline, end to end, against the generator's truth.

Each generated day is replayed into the source Postgres, then the `pos_daily`
Dagster job runs that partition: dlt -> landing -> bronze -> dbt silver/gold.
On the happy path naive must match the truth exactly. The last test previews
the flaw faultline exists to catch: a rerun is green and silver stays clean,
yet the revenue is wrong.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from reference_stack import Stack

from faultline.connectors.dagster import RunOutcome
from faultline.connectors.workspace import new_workspace_name
from faultline.datagen import DatasetSpec, generate
from faultline.datagen.truth import read_control_totals
from faultline.sources.pos import apply_day, create_tables

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")
GOLD_MEASURES = ("orders", "items_sold", "gross_vnd", "discount_vnd", "net_vnd")
TRUTH_FOR = {
    "orders": "orders_completed",
    "items_sold": "items_sold",
    "gross_vnd": "gross_vnd",
    "discount_vnd": "discount_vnd",
    "net_vnd": "net_vnd",
}


@dataclass
class NaiveRun:
    stack: Stack
    workspace: str
    generated: Path
    outcomes: list[RunOutcome]

    def run_day(self, dt: str) -> RunOutcome:
        run_config = {"resources": {"workspace": {"config": {"name": self.workspace}}}}
        run_id = self.stack.dagster.launch_run("pos_daily", partition=dt, run_config=run_config)
        return self.stack.dagster.wait_for_run(run_id, timeout=600)

    def gold_sales(self) -> dict[tuple[str, str], dict[str, int]]:
        rows = self.stack.clickhouse.query(
            f"SELECT toString(order_date) AS order_date, store_id, {', '.join(GOLD_MEASURES)} "
            f"FROM {self.workspace}.gold_daily_store_sales"
        )
        return {(r["order_date"], r["store_id"]): {m: r[m] for m in GOLD_MEASURES} for r in rows}

    def truth_sales(self) -> dict[tuple[str, str], dict[str, int]]:
        return {
            (t["order_date"], t["store_id"]): {m: int(t[TRUTH_FOR[m]]) for m in GOLD_MEASURES}
            for t in read_control_totals(self.generated)
        }

    def duplicate_keys(self, table: str, key: str) -> int:
        rows = self.stack.clickhouse.query(
            f"SELECT count() - uniqExact({key}) AS dupes FROM {self.workspace}.{table}"
        )
        return int(rows[0]["dupes"])


@pytest.fixture(scope="module")
def naive_run(stack: Stack, tmp_path_factory: pytest.TempPathFactory) -> Iterator[NaiveRun]:
    generated = tmp_path_factory.mktemp("tiny")
    generate(TINY, generated, workers=1)
    workspace = new_workspace_name()
    stack.source.create_workspace(workspace)
    create_tables(stack.source, workspace)

    run = NaiveRun(stack=stack, workspace=workspace, generated=generated, outcomes=[])
    for d in TINY.dates():
        apply_day(stack.source, workspace, generated, d)
        run.outcomes.append(run.run_day(d.isoformat()))

    yield run
    stack.source.drop_workspace(workspace)
    stack.clickhouse.drop_workspace(workspace)


def test_every_daily_run_succeeds(naive_run: NaiveRun) -> None:
    for outcome in naive_run.outcomes:
        assert outcome.succeeded, outcome.errors


def test_gold_sales_match_the_truth_to_the_dong(naive_run: NaiveRun) -> None:
    gold, truth = naive_run.gold_sales(), naive_run.truth_sales()

    assert set(gold) == set(truth)
    for key, expected in truth.items():
        assert gold[key] == expected, key


def test_silver_keys_are_unique(naive_run: NaiveRun) -> None:
    assert naive_run.duplicate_keys("silver_orders", "order_id") == 0
    assert naive_run.duplicate_keys("silver_order_items", "(order_id, line_no)") == 0
    assert naive_run.duplicate_keys("silver_payments", "payment_id") == 0


def test_payments_net_to_the_same_revenue(naive_run: NaiveRun) -> None:
    paid = naive_run.stack.clickhouse.query(
        f"SELECT sum(net_vnd) AS net FROM {naive_run.workspace}.gold_daily_payments"
    )[0]["net"]
    truth = sum(v["net_vnd"] for v in naive_run.truth_sales().values())

    assert paid == truth


def test_rerunning_a_day_is_green_and_silver_stays_clean_but_revenue_is_wrong(
    naive_run: NaiveRun,
) -> None:
    # Runs last: it changes the workspace. This is the naive flaw, previewed;
    # faultline's faults and properties will report it as SILENT_CORRUPTION.
    last_day = TINY.end_date.isoformat()

    outcome = naive_run.run_day(last_day)

    assert outcome.succeeded, outcome.errors
    assert naive_run.duplicate_keys("silver_orders", "order_id") == 0
    gold, truth = naive_run.gold_sales(), naive_run.truth_sales()
    wrong = sorted(k for k in truth if gold[k] != truth[k])
    assert wrong, "a rerun should have double counted the last day"
    assert all(day <= last_day for day, _ in wrong)
