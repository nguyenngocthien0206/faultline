from pathlib import Path

import pytest
from oracle_helpers import SALES, Row, write

from faultline.oracle.diff import diff

A: Row = ("2026-11-01", "HCM-001", 40, 3_000_000, "t1")
B: Row = ("2026-11-01", "HN-001", 46, 4_000_000, "t1")
C: Row = ("2026-11-02", "HCM-001", 30, 2_000_000, "t1")
D: Row = ("2026-11-02", "HN-001", 31, 1_000_000, "t1")
E: Row = ("2026-11-03", "DN-001", 10, 500_000, "t1")


def test_identical_snapshots(tmp_path: Path) -> None:
    result = diff(
        write(tmp_path / "e.parquet", [A, B]), write(tmp_path / "a.parquet", [B, A]), SALES
    )

    assert result.is_empty
    assert result.summary() == "gold_sales: identical"


def test_each_kind_of_damage_lands_in_its_own_bucket(tmp_path: Path) -> None:
    expected = write(tmp_path / "e.parquet", [A, B, C, D])
    changed_d = (*D[:3], 1_500_000, "t1")
    actual = write(tmp_path / "a.parquet", [B, C, C, changed_d, E])  # A lost, C twice, D off, E new

    result = diff(expected, actual, SALES)

    assert result.counts == {"missing": 1, "extra": 1, "duplicated": 1, "changed": 1}
    assert result.samples == {
        "missing": ("2026-11-01|HCM-001",),
        "extra": ("2026-11-03|DN-001",),
        "duplicated": ("2026-11-02|HCM-001",),
        "changed": ("2026-11-02|HN-001",),
    }
    # Four kinds of damage, and yet the revenue total is exactly the same: comparing
    # totals alone would call this snapshot correct. Keyed buckets are what catch it.
    net = {m.name: m for m in result.measures}["net_vnd"]
    assert (net.expected, net.actual) == (10_000_000, 10_000_000)
    assert net.delta == 0
    assert not result.is_empty


def test_summary_reads_like_a_scorecard_line(tmp_path: Path) -> None:
    expected = write(tmp_path / "e.parquet", [A, B])
    actual = write(tmp_path / "a.parquet", [A, B, B])

    assert diff(expected, actual, SALES).summary() == (
        "gold_sales: 1 duplicated keys, orders 86 -> 132 (+53.5%), "
        "net_vnd 7,000,000 -> 11,000,000 (+57.1%)"
    )


def test_ignored_columns_never_show_up_as_changes(tmp_path: Path) -> None:
    expected = write(tmp_path / "e.parquet", [A, B])
    actual = write(tmp_path / "a.parquet", [(*A[:4], "t9"), (*B[:4], "t9")])

    assert diff(expected, actual, SALES).is_empty


def test_snapshots_with_different_columns_cannot_be_compared(tmp_path: Path) -> None:
    expected = write(tmp_path / "e.parquet", [A])
    actual = write(
        tmp_path / "a.parquet",
        [(*A, "extra")],
        columns="order_date DATE, store_id VARCHAR, orders BIGINT, net_vnd BIGINT, "
        "loaded_at VARCHAR, channel VARCHAR",
    )

    with pytest.raises(ValueError, match="different columns"):
        diff(expected, actual, SALES)
