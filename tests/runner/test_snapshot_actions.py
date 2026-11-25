"""Snapshot actions: the consumer tables as a reader sees them, mid-schedule."""

from pathlib import Path

import duckdb
import pytest
from fake_files import FakeFilePlatform

from faultline.datagen import DatasetSpec
from faultline.runner import execute, generate_truth
from faultline.schedule import Deliver, Run, Snapshot

TINY = DatasetSpec.from_profile("tiny")


def _rows(path: Path) -> int:
    return duckdb.connect().execute(f"SELECT count(*) FROM '{path.as_posix()}'").fetchone()[0]


def test_a_snapshot_records_the_tables_at_that_point_of_the_schedule(tmp_path: Path) -> None:
    d1, d2 = list(TINY.dates())[:2]
    platform = FakeFilePlatform(generate_truth(TINY, tmp_path), "keyed")
    marks: dict[str, dict[str, Path]] = {}
    lines: list[str] = []

    schedule = (Deliver(d1), Run(d1), Snapshot("after_day_1"), Deliver(d2), Run(d2))
    execute(platform, schedule, lines.append, "fault", marks=marks, work_dir=tmp_path)

    mid = marks["after_day_1"]
    assert set(mid) == {t.name for t in platform.capabilities().tables}
    assert 0 < _rows(mid["silver_orders"]) < len(platform.orders)  # day 1 only
    assert "  [fault] snapshot 'after_day_1'" in lines


def test_a_snapshot_needs_somewhere_to_go(tmp_path: Path) -> None:
    platform = FakeFilePlatform(tmp_path, "keyed")

    with pytest.raises(ValueError, match="needs a work directory"):
        execute(platform, (Snapshot("x"),), print, "fault")
