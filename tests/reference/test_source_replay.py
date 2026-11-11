"""Replaying generated days into the source Postgres (needs the reference stack)."""

from collections import Counter
from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack

from faultline.datagen import DatasetSpec, generate
from faultline.datagen.pos import ORDER_COLUMNS
from faultline.datagen.writer import write_landing_file
from faultline.sources.pos import apply_day, create_tables

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("tiny")
    generate(TINY, root, workers=1)
    return root


def test_replaying_every_day_rebuilds_the_final_source_state(
    stack: Stack, workspace: str, generated: Path
) -> None:
    create_tables(stack.source, workspace)
    totals: Counter[str] = Counter()
    for d in TINY.dates():
        totals.update(apply_day(stack.source, workspace, generated, d))

    meta = (generated / "generation.json").read_text(encoding="utf-8")
    statuses = dict(
        stack.source.fetch_all(f'SELECT status, count(*) FROM "{workspace}".orders GROUP BY 1')
    )
    rows = {
        t: stack.source.fetch_all(f'SELECT count(*) FROM "{workspace}".{t}')[0][0]
        for t in ("orders", "order_items", "payments")
    }

    # Every extracted line and payment lands once; a refunded order is one row, updated.
    assert f'"pos/order_items": {rows["order_items"]}' in meta
    assert f'"pos/payments": {rows["payments"]}' in meta
    assert totals["orders"] == rows["orders"] + statuses["refunded"]
    assert statuses["refunded"] > 0
    assert set(statuses) == {"completed", "voided", "refunded"}


def test_a_refund_updates_the_row_and_moves_its_cursor(
    stack: Stack, workspace: str, generated: Path
) -> None:
    create_tables(stack.source, workspace)
    for d in TINY.dates():
        apply_day(stack.source, workspace, generated, d)

    refunded = stack.source.fetch_all(
        f"SELECT ordered_at, updated_at FROM \"{workspace}\".orders WHERE status = 'refunded'"
    )

    assert refunded
    for ordered_at, updated_at in refunded:
        assert updated_at.date() > ordered_at.date()


def test_a_failed_day_leaves_nothing_behind(stack: Stack, workspace: str, generated: Path) -> None:
    create_tables(stack.source, workspace)
    apply_day(stack.source, workspace, generated, TINY.start_date)

    def snapshot() -> list[tuple[object, ...]]:
        return stack.source.fetch_all(
            f'SELECT (SELECT count(*) FROM "{workspace}".orders), '
            f'(SELECT max(updated_at) FROM "{workspace}".orders), '
            f'(SELECT count(*) FROM "{workspace}".order_items), '
            f'(SELECT count(*) FROM "{workspace}".payments)'
        )

    before = snapshot()
    # Orders upsert fine, but lines collide on their key: the whole day must roll back.
    with pytest.raises(Exception, match="duplicate key"):
        apply_day(stack.source, workspace, generated, TINY.start_date)

    assert snapshot() == before


def _order_row(status: str, updated_at: str) -> tuple[object, ...]:
    return (
        "HCM-001-20261102-00001",
        "HCM-001",
        None,
        "takeaway",
        status,
        "2026-11-02T08:00:00+07:00",
        updated_at,
        29000,
        0,
        29000,
        None,
        1,
    )


def _deliver(root: Path, dt: str, folder: str, rows: list[tuple[object, ...]]) -> None:
    write_landing_file(
        root,
        source="pos",
        dataset="orders",
        partition={"dt": dt, "store": folder},
        columns=ORDER_COLUMNS,
        rows=rows,
    )


def test_a_batch_holding_an_order_and_its_refund_keeps_the_latest(
    stack: Stack, workspace: str, tmp_path: Path
) -> None:
    # An offline till syncs the order and its refund in the same batch.
    _deliver(
        tmp_path,
        "2026-11-04",
        "HCM-001-offline",
        [
            _order_row("completed", "2026-11-02T08:01:00+07:00"),
            _order_row("refunded", "2026-11-03T10:00:00+07:00"),
        ],
    )
    create_tables(stack.source, workspace)

    apply_day(stack.source, workspace, tmp_path, date(2026, 11, 4))

    assert stack.source.fetch_all(f'SELECT status FROM "{workspace}".orders') == [("refunded",)]


def test_an_older_row_never_overwrites_a_newer_one(
    stack: Stack, workspace: str, tmp_path: Path
) -> None:
    _deliver(
        tmp_path, "2026-11-03", "HCM-001", [_order_row("refunded", "2026-11-03T10:00:00+07:00")]
    )
    _deliver(
        tmp_path, "2026-11-04", "HCM-001", [_order_row("completed", "2026-11-02T08:01:00+07:00")]
    )
    create_tables(stack.source, workspace)

    apply_day(stack.source, workspace, tmp_path, date(2026, 11, 3))
    counts = apply_day(stack.source, workspace, tmp_path, date(2026, 11, 4))

    assert counts["orders"] == 0  # the stale version was skipped, not applied
    assert stack.source.fetch_all(f'SELECT status FROM "{workspace}".orders') == [("refunded",)]
