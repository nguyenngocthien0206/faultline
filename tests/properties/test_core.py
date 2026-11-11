from datetime import date
from pathlib import Path

import duckdb
import pytest

from faultline.adapters.base import RunResult, RunStatus, TableContract
from faultline.datagen.writer import write_landing_file
from faultline.properties.base import Checkpoint, PropertyContext
from faultline.properties.core import NoDataLoss, NoSilentCorruption

ORDERS = TableContract(
    "silver_orders", key=("order_id",), measures=("total_vnd",), truth_dataset="pos/orders"
)
SALES = TableContract("gold_sales", key=("order_date",), measures=("net_vnd",))


def _parquet(path: Path, sql: str) -> Path:
    duckdb.connect().execute(f"COPY ({sql}) TO '{path.as_posix()}' (FORMAT parquet)")
    return path


def _orders(path: Path, rows: list[tuple[str, int]]) -> Path:
    values = ", ".join(f"('{k}', {v})" for k, v in rows)
    return _parquet(path, f"SELECT * FROM (VALUES {values}) t(order_id, total_vnd)")


def _sales(path: Path, rows: list[tuple[str, int]]) -> Path:
    values = ", ".join(f"(DATE '{d}', {v})" for d, v in rows)
    return _parquet(path, f"SELECT * FROM (VALUES {values}) t(order_date, net_vnd)")


@pytest.fixture
def ctx(tmp_path: Path) -> PropertyContext:
    truth = tmp_path / "truth"
    write_landing_file(
        truth,
        source="pos",
        dataset="orders",
        partition={"dt": "2026-11-01", "store": "HN-001"},
        columns=("order_id", "total_vnd"),
        rows=[("O-1", 100), ("O-2", 200), ("O-3", 300)],
    )
    baseline = {
        "silver_orders": _orders(
            tmp_path / "b_orders.parquet", [("O-1", 100), ("O-2", 200), ("O-3", 300)]
        ),
        "gold_sales": _sales(
            tmp_path / "b_sales.parquet", [("2026-11-01", 400), ("2026-11-02", 200)]
        ),
    }
    return PropertyContext(contracts=(ORDERS, SALES), baseline=baseline, truth_root=truth)


def _checkpoint(
    tmp_path: Path, orders: list[tuple[str, int]], sales: list[tuple[str, int]]
) -> Checkpoint:
    return Checkpoint(
        name="F",
        snapshots={
            "silver_orders": _orders(tmp_path / "f_orders.parquet", orders),
            "gold_sales": _sales(tmp_path / "f_sales.parquet", sales),
        },
        runs=((date(2026, 11, 1), RunResult(RunStatus.SUCCEEDED)),),
    )


def test_identical_outputs_keep_both_promises(ctx: PropertyContext, tmp_path: Path) -> None:
    cp = _checkpoint(
        tmp_path,
        [("O-1", 100), ("O-2", 200), ("O-3", 300)],
        [("2026-11-01", 400), ("2026-11-02", 200)],
    )

    assert NoDataLoss().check(ctx, cp).holds
    assert NoSilentCorruption().check(ctx, cp).holds


def test_lost_rows_break_both_promises_with_evidence(ctx: PropertyContext, tmp_path: Path) -> None:
    cp = _checkpoint(tmp_path, [("O-1", 100), ("O-3", 300)], [("2026-11-01", 400)])

    loss = NoDataLoss().check(ctx, cp)
    corruption = NoSilentCorruption().check(ctx, cp)

    assert not loss.holds
    assert loss.checkpoint == "F"
    assert loss.evidence == (
        "silver_orders: complete_keys violated by 1 keys, e.g. O-2",
        "gold_sales: 1 keys of the baseline missing, e.g. 2026-11-02",
    )
    assert not corruption.holds
    assert len(corruption.evidence) == 2


def test_a_wrong_value_is_corruption_but_not_loss(ctx: PropertyContext, tmp_path: Path) -> None:
    cp = _checkpoint(
        tmp_path,
        [("O-1", 100), ("O-2", 200), ("O-3", 300)],
        [("2026-11-01", 999), ("2026-11-02", 200)],
    )

    assert NoDataLoss().check(ctx, cp).holds
    corruption = NoSilentCorruption().check(ctx, cp)
    assert not corruption.holds
    assert corruption.evidence == ("gold_sales: 1 changed keys, net_vnd 600 -> 1,199 (+99.8%)",)
