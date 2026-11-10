"""Replaying generated days into the source Postgres (needs the reference stack)."""

from collections import Counter
from pathlib import Path

import pytest
from reference_stack import Stack

from faultline.datagen import DatasetSpec, generate
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
