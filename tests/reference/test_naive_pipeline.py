"""The naive POS pipeline, end to end, against the generator's truth.

Reads the shared clean baseline (`clean_baseline` in conftest): each generated
day replayed into the source Postgres, then `pos_daily` run for that partition
(dlt -> landing -> bronze -> dbt silver/gold). On the happy path naive must
match the truth exactly. What a rerun does to it is checked in
test_adapter_oracle.py.
"""

from pathlib import Path

import duckdb
import pytest

from faultline.datagen.truth import read_control_totals
from faultline.oracle.invariants import unique_keys
from faultline.runner import Baseline

pytestmark = pytest.mark.integration

GOLD_MEASURES = ("orders", "items_sold", "gross_vnd", "discount_vnd", "net_vnd")
TRUTH_FOR = {
    "orders": "orders_completed",
    "items_sold": "items_sold",
    "gross_vnd": "gross_vnd",
    "discount_vnd": "discount_vnd",
    "net_vnd": "net_vnd",
}


def _rows(path: Path, sql: str) -> list[tuple[object, ...]]:
    return duckdb.connect().execute(sql.format(t=f"read_parquet('{path.as_posix()}')")).fetchall()


def test_every_daily_run_succeeds(clean_baseline: Baseline) -> None:
    assert len(clean_baseline.runs) == clean_baseline.spec.days
    for day, result in clean_baseline.runs:
        assert result.succeeded, (day, result.errors)


def test_gold_sales_match_the_truth_to_the_dong(clean_baseline: Baseline) -> None:
    rows = _rows(
        clean_baseline.snapshots["gold_daily_store_sales"],
        f"SELECT CAST(order_date AS VARCHAR), store_id, {', '.join(GOLD_MEASURES)} FROM {{t}}",
    )
    gold = {(r[0], r[1]): dict(zip(GOLD_MEASURES, r[2:], strict=True)) for r in rows}
    truth = {
        (t["order_date"], t["store_id"]): {m: int(t[TRUTH_FOR[m]]) for m in GOLD_MEASURES}
        for t in read_control_totals(clean_baseline.truth_root)
    }

    assert set(gold) == set(truth)
    for key, expected in truth.items():
        assert gold[key] == expected, key


def test_every_consumer_table_has_unique_keys(clean_baseline: Baseline) -> None:
    for contract in clean_baseline.contracts:
        result = unique_keys(clean_baseline.snapshots[contract.name], contract)
        assert result.holds, result.summary()


def test_payments_net_to_the_same_revenue(clean_baseline: Baseline) -> None:
    (paid,) = _rows(
        clean_baseline.snapshots["gold_daily_payments"], "SELECT sum(net_vnd)::BIGINT FROM {t}"
    )[0]
    truth = sum(int(t["net_vnd"]) for t in read_control_totals(clean_baseline.truth_root))

    assert paid == truth
