from datetime import date
from pathlib import Path

import duckdb
import pytest

from faultline.adapters.base import Capability, RunResult, RunStatus, TableContract
from faultline.datagen.writer import write_landing_file
from faultline.properties.base import Checkpoint, PropertyContext
from faultline.properties.core import (
    BackfillEquivalence,
    Idempotency,
    NoDataLoss,
    NoSilentCorruption,
    NoVanishingRows,
)
from faultline.schedule import Backfill, Run, Snapshot

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


def _marks(tmp_path: Path, before: list[tuple[str, int]], after: list[tuple[str, int]]):
    return {
        "before_crash": {"gold_sales": _sales(tmp_path / "before.parquet", before)},
        "after_crash": {"gold_sales": _sales(tmp_path / "after.parquet", after)},
    }


def _gold_only(tmp_path: Path) -> PropertyContext:
    return PropertyContext(contracts=(SALES,), baseline={}, truth_root=tmp_path)


def test_rows_that_vanish_around_the_crash_violate_it(tmp_path: Path) -> None:
    marks = _marks(tmp_path, [("2026-11-01", 100), ("2026-11-02", 200)], [("2026-11-02", 200)])
    at_f = Checkpoint("F", {}, (), marks)

    result = NoVanishingRows().check(_gold_only(tmp_path), at_f)

    assert not result.holds
    assert result.evidence == ("gold_sales: 1 keys vanished, e.g. 2026-11-01",)


def test_new_or_changed_rows_are_not_vanishing_ones(tmp_path: Path) -> None:
    marks = _marks(tmp_path, [("2026-11-01", 100)], [("2026-11-01", 90), ("2026-11-02", 200)])

    result = NoVanishingRows().check(_gold_only(tmp_path), Checkpoint("F", {}, (), marks))

    assert result.holds  # a refreshed value and a new day are what a refresh does


def test_at_r_it_compares_the_end_of_the_fault_phase_with_the_end_of_recovery(
    tmp_path: Path,
) -> None:
    at_f = {"gold_sales": _sales(tmp_path / "f.parquet", [("2026-11-01", 1), ("2026-11-02", 2)])}
    at_r = {"gold_sales": _sales(tmp_path / "r.parquet", [("2026-11-02", 2)])}

    result = NoVanishingRows().check(_gold_only(tmp_path), Checkpoint("R", at_r, (), {"F": at_f}))

    assert not result.holds


def test_it_needs_the_snapshots_it_names(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no snapshot named after_crash, before_crash at F"):
        NoVanishingRows().check(_gold_only(tmp_path), Checkpoint("F", {}, ()))


def _rerun(tmp_path: Path, before: list[tuple[str, int]], after: list[tuple[str, int]], at: str):
    marks = {
        "before_rerun": {"gold_sales": _sales(tmp_path / "before.parquet", before)},
        "after_rerun": {"gold_sales": _sales(tmp_path / "after.parquet", after)},
    }
    return Checkpoint(at, {}, (), marks)


def test_the_idempotency_probe_reruns_the_day_between_two_snapshots() -> None:
    assert Idempotency(rerun=date(2026, 11, 2)).probe() == (
        Snapshot("before_rerun"),
        Run(date(2026, 11, 2)),
        Snapshot("after_rerun"),
    )


def test_a_rerun_that_changes_nothing_is_idempotent(tmp_path: Path) -> None:
    rows = [("2026-11-01", 100), ("2026-11-02", 200)]

    result = Idempotency(date(2026, 11, 2)).check(
        _gold_only(tmp_path), _rerun(tmp_path, rows, rows, "F")
    )

    assert result.holds


@pytest.mark.parametrize(
    ("after", "bucket"),
    [
        ([("2026-11-01", 100), ("2026-11-02", 200), ("2026-11-02", 200)], "1 duplicated keys"),
        ([("2026-11-01", 100), ("2026-11-02", 400)], "1 changed keys"),
        ([("2026-11-01", 100)], "1 missing keys"),
        ([("2026-11-01", 100), ("2026-11-02", 200), ("2026-11-03", 5)], "1 extra keys"),
    ],
)
def test_any_difference_after_the_rerun_breaks_it(
    after: list[tuple[str, int]], bucket: str, tmp_path: Path
) -> None:
    before = [("2026-11-01", 100), ("2026-11-02", 200)]

    result = Idempotency(date(2026, 11, 2)).check(
        _gold_only(tmp_path), _rerun(tmp_path, before, after, "R")
    )

    assert not result.holds
    (line,) = result.evidence
    assert line.startswith("rerun of 2026-11-02: gold_sales: ")
    assert bucket in line


def test_idempotency_needs_the_snapshots_of_its_probe(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="no snapshot named after_rerun, before_rerun at F"):
        Idempotency(date(2026, 11, 2)).check(_gold_only(tmp_path), Checkpoint("F", {}, ()))


def _against(tmp_path: Path, baseline: list[tuple[str, int]], actual: list[tuple[str, int]]):
    ctx = PropertyContext(
        contracts=(SALES,),
        baseline={"gold_sales": _sales(tmp_path / "baseline.parquet", baseline)},
        truth_root=tmp_path,
    )
    return ctx, Checkpoint("F", {"gold_sales": _sales(tmp_path / "f.parquet", actual)}, ())


def test_backfill_equivalence_probes_with_one_backfill_and_needs_the_capability() -> None:
    prop = BackfillEquivalence(date(2026, 11, 1), date(2026, 11, 4))

    assert prop.probe() == (Backfill(date(2026, 11, 1), date(2026, 11, 4)),)
    assert prop.requires == {Capability.BACKFILL}


def test_a_backfill_that_gives_the_daily_answer_is_equivalent(tmp_path: Path) -> None:
    rows = [("2026-11-01", 100), ("2026-11-02", 200)]
    ctx, at_f = _against(tmp_path, rows, rows)

    assert BackfillEquivalence(date(2026, 11, 1), date(2026, 11, 2)).check(ctx, at_f).holds


def test_any_difference_from_the_daily_answer_breaks_equivalence(tmp_path: Path) -> None:
    ctx, at_f = _against(
        tmp_path,
        [("2026-11-01", 100), ("2026-11-02", 200)],
        [("2026-11-01", 100), ("2026-11-01", 100), ("2026-11-02", 200)],
    )

    result = BackfillEquivalence(date(2026, 11, 1), date(2026, 11, 2)).check(ctx, at_f)

    assert not result.holds
    (line,) = result.evidence
    assert line.startswith("backfill 2026-11-01..2026-11-02: gold_sales: 1 duplicated keys")
