from datetime import date
from pathlib import Path

import pytest

from faultline.connectors.postgres import SourceDatabase
from faultline.connectors.workspace import UnsafeWorkspaceError
from faultline.datagen import DatasetSpec, generate
from faultline.datagen.pos import ORDER_COLUMNS, ORDER_ITEM_COLUMNS, PAYMENT_COLUMNS
from faultline.sources.pos import TABLES, apply_day, create_tables, day_files, upsert_sql

TINY = DatasetSpec.from_profile("tiny")


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("tiny")
    generate(TINY, root, workers=1)
    return root


def _columns_in_ddl(ddl: str) -> list[str]:
    names = []
    for line in ddl.strip().splitlines():
        first = line.strip().split()[0]
        if first != "PRIMARY":
            names.append(first)
    return names


def test_source_columns_match_the_generator() -> None:
    expected = {
        "orders": list(ORDER_COLUMNS),
        "order_items": list(ORDER_ITEM_COLUMNS),
        "payments": list(PAYMENT_COLUMNS),
    }

    for table in TABLES:
        assert _columns_in_ddl(table.ddl) == expected[table.name]
        assert table.cursor in expected[table.name]


def test_day_files_cover_every_open_store(generated: Path) -> None:
    files = day_files(generated, "orders", TINY.start_date)

    assert files
    assert all(p.name == "orders.csv" for p in files)
    assert files == sorted(files)
    assert day_files(generated, "orders", date(2030, 1, 1)) == []


def test_orders_upsert_but_lines_and_payments_only_insert() -> None:
    orders, items, payments = TABLES

    assert upsert_sql("fl_abcd1234", orders, ["order_id", "status", "updated_at"]) == (
        'INSERT INTO "fl_abcd1234".orders (order_id, status, updated_at) '
        "SELECT order_id, status, updated_at FROM staging_orders "
        "ON CONFLICT (order_id) DO UPDATE SET status = EXCLUDED.status, "
        "updated_at = EXCLUDED.updated_at"
    )
    assert "ON CONFLICT" not in upsert_sql("fl_abcd1234", items, ["order_id", "line_no"])
    assert "ON CONFLICT" not in upsert_sql("fl_abcd1234", payments, ["payment_id"])


def test_replay_refuses_non_workspace_schemas(generated: Path) -> None:
    db = SourceDatabase("postgresql://nobody:nothing@127.0.0.1:9/none")

    with pytest.raises(UnsafeWorkspaceError):
        create_tables(db, "public")
    with pytest.raises(UnsafeWorkspaceError):
        apply_day(db, "public", generated, TINY.start_date)
    assert db._conn is None
