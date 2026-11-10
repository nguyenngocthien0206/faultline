from collections import Counter, defaultdict
from datetime import date, datetime, time, timedelta

import pytest

from faultline.datagen.dimensions import (
    LOCAL_TZ,
    Dimensions,
    MemberIndex,
    build_dimensions,
)
from faultline.datagen.pos import (
    ORDER_COLUMNS,
    ORDER_ITEM_COLUMNS,
    PAYMENT_COLUMNS,
    TRUTH_COLUMNS,
    StoreDay,
    StoreSimulator,
)
from faultline.datagen.spec import DatasetSpec

SPEC = DatasetSpec.from_profile("tiny")


class Simulated:
    """All POS rows of the tiny profile, as dicts, with the extract day attached."""

    def __init__(self, spec: DatasetSpec) -> None:
        self.dims: Dimensions = build_dimensions(spec)
        index = MemberIndex.from_members(self.dims.members)
        self.days: dict[str, list[StoreDay]] = {}
        self.truth: list[dict[str, object]] = []
        self.orders: list[dict[str, object]] = []
        self.items: list[dict[str, object]] = []
        self.payments: list[dict[str, object]] = []
        for store in self.dims.stores:
            sim = StoreSimulator(spec, store, self.dims.menu, self.dims.promotions, index)
            self.days[store.store_id] = list(sim.days())
            self.truth += [dict(zip(TRUTH_COLUMNS, r, strict=True)) for r in sim.truth_rows()]
        for store_days in self.days.values():
            for d in store_days:
                self.orders += [_row(ORDER_COLUMNS, r, d.dt) for r in d.orders]
                self.items += [_row(ORDER_ITEM_COLUMNS, r, d.dt) for r in d.order_items]
                self.payments += [_row(PAYMENT_COLUMNS, r, d.dt) for r in d.payments]


def _row(columns: tuple[str, ...], values: tuple[object, ...], dt: date) -> dict[str, object]:
    return {**dict(zip(columns, values, strict=True)), "_dt": dt}


@pytest.fixture(scope="module")
def sim() -> Simulated:
    return Simulated(SPEC)


def _created(sim: Simulated) -> list[dict[str, object]]:
    """First appearance of every order (the day it was placed)."""
    return [o for o in sim.orders if o["status"] != "refunded"]


def _ts(value: object) -> datetime:
    return datetime.fromisoformat(str(value))


# ------------------------------------------------------------------------ volumes


def test_tiny_has_every_lifecycle_status(sim: Simulated) -> None:
    statuses = Counter(o["status"] for o in sim.orders)

    assert statuses["completed"] > 300
    assert statuses["voided"] >= 1
    assert statuses["refunded"] >= 1


def test_order_ids_are_unique_per_creation(sim: Simulated) -> None:
    ids = [o["order_id"] for o in _created(sim)]

    assert len(ids) == len(set(ids))


def test_closed_and_planned_days_produce_no_extract(sim: Simulated) -> None:
    for store in sim.dims.stores:
        extracted = {d.dt for d in sim.days[store.store_id]}
        assert extracted == {d for d in SPEC.dates() if store.is_open(d)}


# ------------------------------------------------------------------------- money


def test_order_totals_add_up(sim: Simulated) -> None:
    lines: dict[object, list[dict[str, object]]] = defaultdict(list)
    for item in sim.items:
        lines[item["order_id"]].append(item)

    for o in sim.orders:
        order_lines = lines[o["order_id"]]
        subtotal = sum(int(i["qty"]) * int(i["unit_price_vnd"]) for i in order_lines)
        assert o["subtotal_vnd"] == subtotal
        assert o["total_vnd"] == subtotal - int(o["discount_vnd"])
        assert o["item_count"] == len(order_lines)
        assert int(o["discount_vnd"]) % 1000 == 0
        assert int(o["total_vnd"]) >= 0


def test_payments_match_the_final_status(sim: Simulated) -> None:
    paid: dict[object, int] = defaultdict(int)
    for p in sim.payments:
        paid[p["order_id"]] += int(p["amount_vnd"])
    final = {o["order_id"]: o for o in sim.orders}  # later rows overwrite earlier ones

    for order_id, o in final.items():
        expected = o["total_vnd"] if o["status"] == "completed" else 0
        assert paid.get(order_id, 0) == expected, order_id


def test_split_payments_exist(sim: Simulated) -> None:
    charges = Counter(p["order_id"] for p in sim.payments if p["kind"] == "charge")

    assert any(n == 2 for n in charges.values())


def test_unit_price_is_the_menu_price_on_the_order_day(sim: Simulated) -> None:
    for item in sim.items:
        d = item["_dt"]
        assert isinstance(d, date)
        version = sim.dims.menu.version_on(str(item["item_id"]), d)
        assert version is not None
        assert sim.dims.menu.is_available(str(item["item_id"]), d)
        assert item["unit_price_vnd"] == version.price_vnd


def test_the_price_change_shows_up_in_order_lines(sim: Simulated) -> None:
    prices = {i["unit_price_vnd"] for i in sim.items if i["item_id"] == "CF-SUA-M"}

    assert prices == {29_000, 32_000}


def test_promotions_are_valid_and_correctly_applied(sim: Simulated) -> None:
    promos = {p.promo_code: p for p in sim.dims.promotions}
    lines: dict[object, list[dict[str, object]]] = defaultdict(list)
    for item in sim.items:
        lines[item["order_id"]].append(item)
    used = [o for o in _created(sim) if o["promo_code"]]

    assert used
    for o in used:
        promo = promos[str(o["promo_code"])]
        d = _ts(o["ordered_at"]).date()
        by_category: dict[str, int] = defaultdict(int)
        for i in lines[o["order_id"]]:
            version = sim.dims.menu.version_on(str(i["item_id"]), d)
            assert version is not None
            by_category[version.category] += int(i["qty"]) * int(i["unit_price_vnd"])
        assert promo.is_active(d)
        assert promo.discount_vnd(int(o["subtotal_vnd"]), by_category) == o["discount_vnd"]


def test_members_joined_before_the_order_day(sim: Simulated) -> None:
    joined = {m.member_id: m.joined_at.date() for m in sim.dims.members}
    with_member = [o for o in _created(sim) if o["member_id"]]

    assert with_member
    for o in with_member:
        assert joined[str(o["member_id"])] < _ts(o["ordered_at"]).date()


# -------------------------------------------------------------------------- time


def test_orders_are_local_time_inside_opening_hours(sim: Simulated) -> None:
    for o in _created(sim):
        ordered_at = _ts(o["ordered_at"])
        assert ordered_at.utcoffset() == timedelta(hours=7)
        assert ordered_at.date() == o["_dt"]
        assert time(6, 30) <= ordered_at.timetz().replace(tzinfo=None) < time(22, 0)


def test_lines_are_extracted_on_the_creation_day_only(sim: Simulated) -> None:
    created_on = {o["order_id"]: o["_dt"] for o in _created(sim)}

    for item in sim.items:
        assert item["_dt"] == created_on[item["order_id"]]


def test_some_orders_are_placed_at_dawn_in_local_time(sim: Simulated) -> None:
    # Before 07:00 local is still the previous day in UTC: the event-time trap.
    dawn = [o for o in _created(sim) if _ts(o["ordered_at"]).hour < 7]

    assert dawn
    assert all(_ts(o["ordered_at"]).astimezone(LOCAL_TZ).date() == o["_dt"] for o in dawn)


# --------------------------------------------------------------------- lifecycle


def test_voided_orders_appear_once_on_the_same_day(sim: Simulated) -> None:
    appearances = Counter(o["order_id"] for o in sim.orders)

    for o in (o for o in sim.orders if o["status"] == "voided"):
        assert appearances[o["order_id"]] == 1
        assert _ts(o["updated_at"]).date() == _ts(o["ordered_at"]).date()


def test_refunds_arrive_one_to_three_days_later_in_the_refund_day_extract(
    sim: Simulated,
) -> None:
    for o in (o for o in sim.orders if o["status"] == "refunded"):
        placed = _ts(o["ordered_at"]).date()
        refunded = _ts(o["updated_at"]).date()
        assert 1 <= (refunded - placed).days <= 3
        assert o["_dt"] == refunded  # ingestion day, not the order day


def test_refund_payments_reverse_each_charge(sim: Simulated) -> None:
    refunded = {o["order_id"] for o in sim.orders if o["status"] == "refunded"}
    by_order: dict[object, list[dict[str, object]]] = defaultdict(list)
    for p in sim.payments:
        if p["order_id"] in refunded:
            by_order[p["order_id"]].append(p)

    for payments in by_order.values():
        charges = sorted((p["method"], p["amount_vnd"]) for p in payments if p["kind"] == "charge")
        refunds = sorted(
            (p["method"], -int(p["amount_vnd"])) for p in payments if p["kind"] == "refund"
        )
        assert charges == refunds


# ------------------------------------------------------------------------- truth


def test_truth_matches_the_rows(sim: Simulated) -> None:
    final = {o["order_id"]: o for o in sim.orders}
    expected: dict[tuple[object, object], Counter[str]] = defaultdict(Counter)
    qty: dict[object, int] = defaultdict(int)
    for item in sim.items:
        qty[item["order_id"]] += int(item["qty"])
    for o in final.values():
        key = (_ts(o["ordered_at"]).date(), o["store_id"])
        c = expected[key]
        c["orders_placed"] += 1
        c[f"orders_{o['status']}"] += 1
        if o["status"] == "completed":
            c["items_sold"] += qty[o["order_id"]]
            c["gross_vnd"] += int(o["subtotal_vnd"])
            c["discount_vnd"] += int(o["discount_vnd"])
            c["net_vnd"] += int(o["total_vnd"])

    assert len(sim.truth) == len(expected)
    for t in sim.truth:
        c = expected[(t["order_date"], t["store_id"])]
        for column in TRUTH_COLUMNS[2:]:
            assert t[column] == c[column], (t["order_date"], t["store_id"], column)
