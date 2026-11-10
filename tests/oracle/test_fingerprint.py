from pathlib import Path

import pytest
from oracle_helpers import SALES, Row, write

from faultline.adapters.base import TableContract
from faultline.oracle.fingerprint import fingerprint

A: Row = ("2026-11-01", "HCM-001", 40, 3_202_000, "t1")
B: Row = ("2026-11-01", "HN-001", 46, 4_134_000, "t1")
C: Row = ("2026-11-02", "HN-001", 31, 2_812_000, "t1")


def test_row_order_does_not_matter(tmp_path: Path) -> None:
    one = fingerprint(write(tmp_path / "1.parquet", [A, B, C]), SALES)
    two = fingerprint(write(tmp_path / "2.parquet", [C, A, B]), SALES)

    assert one == two
    assert one.rows == 3
    assert one.duplicate_rows == 0
    assert one.measure("net_vnd") == 3_202_000 + 4_134_000 + 2_812_000


def test_a_duplicated_row_changes_the_fingerprint(tmp_path: Path) -> None:
    clean = fingerprint(write(tmp_path / "clean.parquet", [A, B]), SALES)
    dup = fingerprint(write(tmp_path / "dup.parquet", [A, B, B]), SALES)

    assert dup.rows == clean.rows + 1
    assert dup.duplicate_rows == 1
    assert dup.row_hash != clean.row_hash
    assert dup.measure("orders") == clean.measure("orders") + 46


def test_pairs_of_duplicates_do_not_cancel_out(tmp_path: Path) -> None:
    # With XOR, C ^ C == 0, so [A, B, C, C] would fingerprint exactly like [A, B].
    clean = fingerprint(write(tmp_path / "clean.parquet", [A, B]), SALES)
    doubled = fingerprint(write(tmp_path / "doubled.parquet", [A, B, C, C]), SALES)

    assert doubled.row_hash != clean.row_hash


def test_ignored_columns_do_not_count(tmp_path: Path) -> None:
    first = fingerprint(write(tmp_path / "1.parquet", [A, B]), SALES)
    reloaded = [(*r[:4], "t2") for r in (A, B)]
    second = fingerprint(write(tmp_path / "2.parquet", reloaded), SALES)

    assert first == second


def test_a_changed_value_changes_the_hash_but_not_the_counts(tmp_path: Path) -> None:
    before = fingerprint(write(tmp_path / "1.parquet", [A, B]), SALES)
    after = fingerprint(write(tmp_path / "2.parquet", [A, (*B[:3], 0, "t1")]), SALES)

    assert (after.rows, after.distinct_keys) == (before.rows, before.distinct_keys)
    assert after.row_hash != before.row_hash
    assert after.measure("net_vnd") == before.measure("net_vnd") - 4_134_000


def test_empty_snapshot(tmp_path: Path) -> None:
    empty = fingerprint(write(tmp_path / "empty.parquet", []), SALES)

    assert (empty.rows, empty.distinct_keys, empty.row_hash) == (0, 0, 0)
    assert empty.measure("net_vnd") == 0


def test_contract_columns_must_exist(tmp_path: Path) -> None:
    path = write(tmp_path / "1.parquet", [A])
    wrong = TableContract("gold_sales", key=("order_date", "shop_id"))

    with pytest.raises(ValueError, match="shop_id"):
        fingerprint(path, wrong)
