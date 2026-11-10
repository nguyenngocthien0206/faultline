import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from faultline.datagen.truth import read_control_totals, write_control_totals
from faultline.datagen.writer import render_csv, write_landing_file


def test_render_csv_formats_cells_and_counts_rows() -> None:
    ts = datetime(2026, 11, 1, 7, 5, tzinfo=timezone(timedelta(hours=7)))
    content, count = render_csv(("a", "b", "c"), [(1, None, date(2026, 11, 1)), ("x,y", ts, 0)])

    assert count == 2
    expected = 'a,b,c\n1,,2026-11-01\n"x,y",2026-11-01T07:05:00+07:00,0\n'
    assert content.decode("utf-8") == expected


def test_render_csv_is_utf8_without_bom_and_lf_only() -> None:
    content, _ = render_csv(("name",), [("Cà phê sữa đá",)])

    assert not content.startswith(b"\xef\xbb\xbf")
    assert b"\r" not in content
    assert content.decode("utf-8") == "name\nCà phê sữa đá\n"


def test_landing_file_and_manifest(tmp_path: Path) -> None:
    stat = write_landing_file(
        tmp_path,
        source="pos",
        dataset="orders",
        partition={"dt": "2026-11-01", "store": "HCM-001"},
        columns=("order_id", "total_vnd"),
        rows=[("A", 1000), ("B", 2000)],
    )

    directory = tmp_path / "landing/pos/orders/dt=2026-11-01/store=HCM-001"
    content = (directory / "orders.csv").read_bytes()
    manifest = json.loads((directory / "orders.manifest.json").read_text(encoding="utf-8"))
    assert stat.path == "landing/pos/orders/dt=2026-11-01/store=HCM-001/orders.csv"
    assert manifest == {
        "source": "pos",
        "dataset": "orders",
        "partition": {"dt": "2026-11-01", "store": "HCM-001"},
        "file": "orders.csv",
        "row_count": 2,
        "sha256": hashlib.sha256(content).hexdigest(),
        "schema_version": 1,
        "columns": ["order_id", "total_vnd"],
    }
    assert stat.row_count == 2
    assert stat.sha256 == manifest["sha256"]


def test_empty_extract_still_writes_a_header_only_file(tmp_path: Path) -> None:
    # "The extract ran and found nothing" must look different from "no file arrived".
    stat = write_landing_file(
        tmp_path,
        source="backoffice",
        dataset="members",
        partition={"dt": "2026-11-02"},
        columns=("member_id",),
        rows=[],
    )

    assert stat.row_count == 0
    assert (tmp_path / stat.path).read_text(encoding="utf-8") == "member_id\n"


def test_control_totals_round_trip_sorted(tmp_path: Path) -> None:
    rows = [
        (date(2026, 11, 2), "HN-001", 1, 1, 0, 0, 2, 50_000, 0, 50_000),
        (date(2026, 11, 1), "HN-001", 3, 2, 1, 0, 4, 90_000, 9_000, 81_000),
    ]
    write_control_totals(tmp_path, rows)

    loaded = read_control_totals(tmp_path)
    assert [r["order_date"] for r in loaded] == ["2026-11-01", "2026-11-02"]
    assert loaded[0]["net_vnd"] == "81000"
