from datetime import date
from pathlib import Path

import pytest
from oracle_helpers import SALES, write

from faultline.adapters.base import TableContract
from faultline.datagen.writer import write_landing_file
from faultline.oracle.invariants import complete_keys, unique_keys

ORDERS = TableContract("silver_orders", key=("order_id",), truth_dataset="pos/orders")
ORDER_COLUMNS = "order_id VARCHAR, total_vnd BIGINT"


def _generated(root: Path, ids_by_store_day: dict[tuple[date, str], list[str]]) -> Path:
    for (d, store), ids in ids_by_store_day.items():
        write_landing_file(
            root,
            source="pos",
            dataset="orders",
            partition={"dt": d.isoformat(), "store": store},
            columns=("order_id", "total_vnd"),
            rows=[(i, 1000) for i in ids],
        )
    return root


def test_unique_keys(tmp_path: Path) -> None:
    rows = [("2026-11-01", "HCM-001", 1, 1, "t"), ("2026-11-01", "HN-001", 1, 1, "t")]
    clean = unique_keys(write(tmp_path / "clean.parquet", rows), SALES)
    dup = unique_keys(write(tmp_path / "dup.parquet", [*rows, rows[1]]), SALES)

    assert clean.holds
    assert not dup.holds
    assert dup.violations == 1
    assert dup.samples == ("2026-11-01|HN-001",)
    assert "violated by 1 keys" in dup.summary()


def test_complete_keys_finds_what_the_platform_lost(tmp_path: Path) -> None:
    generated = _generated(
        tmp_path / "gen",
        {
            (date(2026, 11, 1), "HCM-001"): ["O-1", "O-2"],
            (date(2026, 11, 2), "HN-001"): ["O-3"],
        },
    )
    snapshot = write(tmp_path / "s.parquet", [("O-1", 1), ("O-3", 1)], ORDER_COLUMNS)

    result = complete_keys(snapshot, ORDERS, generated)

    assert result.violations == 1
    assert result.samples == ("O-2",)


def test_complete_keys_holds_when_nothing_is_missing(tmp_path: Path) -> None:
    generated = _generated(tmp_path / "gen", {(date(2026, 11, 1), "HCM-001"): ["O-1"]})
    snapshot = write(tmp_path / "s.parquet", [("O-1", 1), ("O-1", 1)], ORDER_COLUMNS)

    # Duplicates are unique_keys' business; completeness only asks "is anything missing?"
    assert complete_keys(snapshot, ORDERS, generated).holds


def test_complete_keys_needs_a_truth_dataset(tmp_path: Path) -> None:
    snapshot = write(tmp_path / "s.parquet", [])

    with pytest.raises(ValueError, match="no truth_dataset"):
        complete_keys(snapshot, SALES, tmp_path)
