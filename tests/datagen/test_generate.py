import hashlib
import json
from collections import Counter, defaultdict
from collections.abc import Callable
from datetime import datetime
from pathlib import Path

import pytest
from datagen_helpers import Rows, snapshot_files

from faultline.datagen import DatasetSpec, OutputNotEmptyError, generate
from faultline.datagen.truth import read_control_totals

TINY = DatasetSpec.from_profile("tiny")


# ------------------------------------------------------------------- determinism


def test_output_is_identical_for_any_number_of_workers(tiny_root: Path, tmp_path: Path) -> None:
    generate(TINY, tmp_path, workers=2)

    assert snapshot_files(tmp_path) == snapshot_files(tiny_root)


def test_extending_the_window_leaves_existing_partitions_untouched(
    tiny_root: Path, tmp_path: Path
) -> None:
    generate(DatasetSpec.from_profile("tiny", days=TINY.days + 3), tmp_path, workers=1)

    longer = snapshot_files(tmp_path)
    original = {k: v for k, v in snapshot_files(tiny_root).items() if k.startswith("landing/")}
    assert original
    for path, content in original.items():
        assert longer[path] == content, path


def test_generation_metadata_has_no_volatile_fields(tiny_root: Path) -> None:
    meta = json.loads((tiny_root / "generation.json").read_text(encoding="utf-8"))

    assert set(meta) == {"generator", "faultline_version", "spec", "files", "row_counts"}
    assert meta["spec"] == TINY.to_dict()


def test_refuses_a_non_empty_output_directory(tmp_path: Path) -> None:
    (tmp_path / "keep.txt").write_text("existing work", encoding="utf-8")

    with pytest.raises(OutputNotEmptyError):
        generate(TINY, tmp_path, workers=1)
    assert (tmp_path / "keep.txt").read_text(encoding="utf-8") == "existing work"


# ---------------------------------------------------------------------- manifests


def test_every_file_has_a_matching_manifest(tiny_root: Path) -> None:
    csv_files = sorted((tiny_root / "landing").rglob("*.csv"))

    assert csv_files
    for path in csv_files:
        manifest_path = path.with_name(path.stem + ".manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        content = path.read_bytes()
        assert manifest["sha256"] == hashlib.sha256(content).hexdigest()
        assert manifest["row_count"] == content.count(b"\n") - 1


def test_row_counts_in_metadata_match_the_files(
    tiny_root: Path, landing: Callable[[str, str], Rows]
) -> None:
    meta = json.loads((tiny_root / "generation.json").read_text(encoding="utf-8"))

    for name, count in meta["row_counts"].items():
        source, dataset = name.split("/")
        assert len(landing(source, dataset)) == count, name


def test_pos_extracts_come_as_a_complete_set_per_store_day(tiny_root: Path) -> None:
    folders = Counter(
        (p.parent.parent.name, p.parent.name) for p in (tiny_root / "landing/pos").rglob("*.csv")
    )

    assert folders
    assert set(folders.values()) == {3}  # orders, order_items, payments


# ----------------------------------------------------------- cross-dataset checks


def test_referential_integrity_across_datasets(landing: Callable[[str, str], Rows]) -> None:
    stores = {r["store_id"] for r in landing("backoffice", "stores")}
    items = {r["item_id"] for r in landing("backoffice", "menu_items")}
    promos = {r["promo_code"] for r in landing("backoffice", "promotions")}
    members = {r["member_id"] for r in landing("backoffice", "members")}
    orders = landing("pos", "orders")
    order_ids = {o["order_id"] for o in orders}

    for o in orders:
        assert o["store_id"] in stores
        assert o["store_id"] == o["_store"]
        assert not o["member_id"] or o["member_id"] in members
        assert not o["promo_code"] or o["promo_code"] in promos
    for line in landing("pos", "order_items"):
        assert line["order_id"] in order_ids
        assert line["item_id"] in items
    for p in landing("pos", "payments"):
        assert p["order_id"] in order_ids


def test_keys_are_unique_per_dataset(landing: Callable[[str, str], Rows]) -> None:
    lines = [(r["order_id"], r["line_no"]) for r in landing("pos", "order_items")]
    payments = [r["payment_id"] for r in landing("pos", "payments")]
    # An order id repeats only when a later extract carries its refund.
    order_keys = [(r["order_id"], r["status"]) for r in landing("pos", "orders")]

    assert len(lines) == len(set(lines))
    assert len(payments) == len(set(payments))
    assert len(order_keys) == len(set(order_keys))


def test_truth_matches_what_a_correct_platform_would_compute(
    tiny_root: Path, landing: Callable[[str, str], Rows]
) -> None:
    # Latest state per order wins, revenue belongs to the day the order was placed.
    latest: dict[str, dict[str, str]] = {}
    for o in sorted(landing("pos", "orders"), key=lambda o: o["updated_at"]):
        latest[o["order_id"]] = o
    qty: dict[str, int] = defaultdict(int)
    for line in landing("pos", "order_items"):
        qty[line["order_id"]] += int(line["qty"])

    expected: dict[tuple[str, str], Counter[str]] = defaultdict(Counter)
    for o in latest.values():
        c = expected[(datetime.fromisoformat(o["ordered_at"]).date().isoformat(), o["store_id"])]
        c["orders_placed"] += 1
        c[f"orders_{o['status']}"] += 1
        if o["status"] == "completed":
            c["items_sold"] += qty[o["order_id"]]
            c["gross_vnd"] += int(o["subtotal_vnd"])
            c["discount_vnd"] += int(o["discount_vnd"])
            c["net_vnd"] += int(o["total_vnd"])

    truth = read_control_totals(tiny_root)
    assert len(truth) == len(expected)
    for row in truth:
        c = expected[(row["order_date"], row["store_id"])]
        for column, value in row.items():
            if column not in {"order_date", "store_id"}:
                assert int(value) == c[column], (row["order_date"], row["store_id"], column)


def test_truth_is_outside_the_landing_zone(tiny_root: Path) -> None:
    assert (tiny_root / "truth" / "control_totals.csv").is_file()
    assert not list((tiny_root / "landing").rglob("control_totals*"))
