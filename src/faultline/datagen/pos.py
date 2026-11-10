"""POS simulation: orders, order lines and payments for one store, day by day.

A store is simulated sequentially because a refund decided on day D is
delivered in the extract of day D+k. Stores are independent of each other, so
the caller can run them in parallel. Every random draw for day D comes from a
generator seeded with (seed, store, D), so the bytes of a day never depend on
which worker ran it or on how many days are generated in total.
"""

from __future__ import annotations

import math
import random
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta

from faultline.datagen.dimensions import (
    MemberIndex,
    Menu,
    MenuItem,
    Promotion,
    Store,
    local_dt,
)
from faultline.datagen.spec import DatasetSpec

ORDER_COLUMNS = (
    "order_id",
    "store_id",
    "member_id",
    "channel",
    "status",
    "ordered_at",
    "updated_at",
    "subtotal_vnd",
    "discount_vnd",
    "total_vnd",
    "promo_code",
    "item_count",
)
# created_at is the order's ordered_at: lines are written when the order is rung
# up, and incremental extractors (dlt) need a timestamp to use as their cursor.
ORDER_ITEM_COLUMNS = ("order_id", "line_no", "item_id", "qty", "unit_price_vnd", "created_at")
PAYMENT_COLUMNS = ("payment_id", "order_id", "kind", "method", "amount_vnd", "paid_at")
TRUTH_COLUMNS = (
    "order_date",
    "store_id",
    "orders_placed",
    "orders_completed",
    "orders_voided",
    "orders_refunded",
    "items_sold",
    "gross_vnd",
    "discount_vnd",
    "net_vnd",
)

OPEN_SECOND = 6 * 3600 + 30 * 60
CLOSE_SECOND = 22 * 3600
# Demand peaks: (centre second, spread in seconds, weight).
PEAKS = (
    (7 * 3600 + 30 * 60, 50 * 60, 0.35),
    (12 * 3600 + 30 * 60, 60 * 60, 0.30),
    (16 * 3600, 90 * 60, 0.25),
    (19 * 3600 + 30 * 60, 60 * 60, 0.10),
)
WEEKDAY_FACTOR = (0.95, 0.95, 0.95, 0.95, 1.05, 1.25, 1.20)  # Monday first

CHANNELS, CHANNEL_WEIGHTS = ("dine_in", "takeaway"), (0.45, 0.55)
METHODS, METHOD_WEIGHTS = ("cash", "card", "ewallet"), (0.30, 0.20, 0.50)
LINE_COUNTS, LINE_COUNT_WEIGHTS = (1, 2, 3, 4), (0.50, 0.30, 0.15, 0.05)
QTYS, QTY_WEIGHTS = (1, 2, 3), (0.75, 0.20, 0.05)

MEMBER_SHARE = 0.35
PROMO_ATTEMPT_SHARE = 0.25
VOID_SHARE = 0.02
REFUND_SHARE = 0.02
SPLIT_PAYMENT_SHARE = 0.10
REFUND_DELAY_DAYS = (1, 3)


def _iso(ts: datetime) -> str:
    return ts.isoformat()


@dataclass
class _Order:
    order_id: str
    member_id: str | None
    channel: str
    ordered_at: datetime
    subtotal: int
    discount: int
    total: int
    promo_code: str | None
    item_count: int
    items_sold: int
    charges: list[tuple[str, str, int]]  # (payment_id, method, amount)

    def row(self, store_id: str, status: str, updated_at: datetime) -> tuple[object, ...]:
        return (
            self.order_id,
            store_id,
            self.member_id,
            self.channel,
            status,
            _iso(self.ordered_at),
            _iso(updated_at),
            self.subtotal,
            self.discount,
            self.total,
            self.promo_code,
            self.item_count,
        )


@dataclass
class StoreDay:
    """Everything the store's POS extracts for one day."""

    dt: date
    orders: list[tuple[object, ...]] = field(default_factory=list)
    order_items: list[tuple[object, ...]] = field(default_factory=list)
    payments: list[tuple[object, ...]] = field(default_factory=list)


@dataclass
class _Truth:
    placed: int = 0
    completed: int = 0
    voided: int = 0
    refunded: int = 0
    items_sold: int = 0
    gross: int = 0
    discount: int = 0
    net: int = 0


class StoreSimulator:
    def __init__(
        self,
        spec: DatasetSpec,
        store: Store,
        menu: Menu,
        promotions: tuple[Promotion, ...],
        members: MemberIndex,
    ) -> None:
        self.spec = spec
        self.store = store
        self.menu = menu
        self.promotions = promotions
        self.members = members
        self._pending_refunds: dict[date, list[tuple[_Order, int]]] = defaultdict(list)
        self._truth: dict[date, _Truth] = {}

    # ------------------------------------------------------------------ public

    def days(self) -> Iterator[StoreDay]:
        """Yield one StoreDay per day the store is open, in date order."""
        for d in self.spec.dates():
            if self.store.is_open(d):
                yield self._simulate_day(d)

    def truth_rows(self) -> list[tuple[object, ...]]:
        """Final per-business-day totals. Only valid after `days()` is exhausted."""
        return [
            (
                d,
                self.store.store_id,
                t.placed,
                t.completed,
                t.voided,
                t.refunded,
                t.items_sold,
                t.gross,
                t.discount,
                t.net,
            )
            for d, t in sorted(self._truth.items())
        ]

    # ----------------------------------------------------------------- per day

    def _simulate_day(self, d: date) -> StoreDay:
        rng = random.Random(f"{self.spec.seed}:pos:{self.store.store_id}:{d.isoformat()}")
        day = StoreDay(dt=d)
        truth = self._truth.setdefault(d, _Truth())

        available = self.menu.available_on(d)
        weights = [v.popularity for v in available]
        active_promos = [p for p in self.promotions if p.is_active(d)]
        eligible_members = self.members.eligible_count(d)

        expected = self.spec.orders_per_store_day * self.store.traffic_weight
        expected *= WEEKDAY_FACTOR[d.weekday()]
        n_orders = max(0, round(rng.gauss(expected, math.sqrt(expected))))
        seconds = sorted(_order_second(rng) for _ in range(n_orders))

        for seq, second in enumerate(seconds, start=1):
            ordered_at = local_dt(d, second)
            order_id = f"{self.store.store_id}-{d:%Y%m%d}-{seq:05d}"
            order = self._build_order(
                rng, order_id, ordered_at, available, weights, active_promos, eligible_members, day
            )
            truth.placed += 1

            if rng.random() < VOID_SHARE:
                voided_at = ordered_at + timedelta(seconds=rng.randint(60, 600))
                day.orders.append(order.row(self.store.store_id, "voided", voided_at))
                truth.voided += 1
                continue

            paid_at = ordered_at + timedelta(seconds=rng.randint(5, 120))
            self._charge(rng, order, paid_at, day)
            day.orders.append(order.row(self.store.store_id, "completed", paid_at))

            refunded = rng.random() < REFUND_SHARE
            refund_day = d + timedelta(days=rng.randint(*REFUND_DELAY_DAYS))
            refund_second = rng.randint(8 * 3600, 21 * 3600)
            if refunded and refund_day <= self.spec.end_date and self.store.is_open(refund_day):
                self._pending_refunds[refund_day].append((order, refund_second))
                truth.refunded += 1
            else:
                truth.completed += 1
                truth.items_sold += order.items_sold
                truth.gross += order.subtotal
                truth.discount += order.discount
                truth.net += order.total

        for order, second in self._pending_refunds.pop(d, []):
            refunded_at = local_dt(d, second)
            day.orders.append(order.row(self.store.store_id, "refunded", refunded_at))
            for n, (_, method, amount) in enumerate(order.charges, start=1):
                refund_id = f"{order.order_id}-R{n}"
                day.payments.append(
                    (refund_id, order.order_id, "refund", method, -amount, _iso(refunded_at))
                )

        day.orders.sort(key=lambda r: (r[6], r[0]))  # updated_at, order_id
        day.payments.sort(key=lambda r: (r[5], r[0]))  # paid_at, payment_id
        return day

    def _build_order(
        self,
        rng: random.Random,
        order_id: str,
        ordered_at: datetime,
        available: list[MenuItem],
        weights: list[float],
        active_promos: list[Promotion],
        eligible_members: int,
        day: StoreDay,
    ) -> _Order:
        channel = rng.choices(CHANNELS, CHANNEL_WEIGHTS)[0]
        member_id = None
        if rng.random() < MEMBER_SHARE and eligible_members:
            member_id = self.members.member_ids[rng.randrange(eligible_members)]

        n_lines = min(rng.choices(LINE_COUNTS, LINE_COUNT_WEIGHTS)[0], len(available))
        pool, pool_weights = list(available), list(weights)
        subtotal, items_sold = 0, 0
        by_category: dict[str, int] = defaultdict(int)
        for line_no in range(1, n_lines + 1):
            i = rng.choices(range(len(pool)), pool_weights)[0]
            item = pool.pop(i)
            pool_weights.pop(i)
            qty = rng.choices(QTYS, QTY_WEIGHTS)[0]
            day.order_items.append(
                (order_id, line_no, item.item_id, qty, item.price_vnd, _iso(ordered_at))
            )
            subtotal += qty * item.price_vnd
            items_sold += qty
            by_category[item.category] += qty * item.price_vnd

        promo_code, discount = None, 0
        if rng.random() < PROMO_ATTEMPT_SHARE:
            offers = [(p, p.discount_vnd(subtotal, by_category)) for p in active_promos]
            offers = [(p, amount) for p, amount in offers if amount > 0]
            if offers:
                promo, discount = offers[rng.randrange(len(offers))]
                promo_code = promo.promo_code

        return _Order(
            order_id=order_id,
            member_id=member_id,
            channel=channel,
            ordered_at=ordered_at,
            subtotal=subtotal,
            discount=discount,
            total=subtotal - discount,
            promo_code=promo_code,
            item_count=n_lines,
            items_sold=items_sold,
            charges=[],
        )

    def _charge(self, rng: random.Random, order: _Order, paid_at: datetime, day: StoreDay) -> None:
        if order.total == 0:
            return  # fully discounted: nothing to pay
        methods = [rng.choices(METHODS, METHOD_WEIGHTS)[0]]
        amounts = [order.total]
        if order.total >= 2000 and rng.random() < SPLIT_PAYMENT_SHARE:
            first = int(order.total * rng.uniform(0.3, 0.7)) // 1000 * 1000 or 1000
            second_method = rng.choice([m for m in METHODS if m != methods[0]])
            methods.append(second_method)
            amounts = [first, order.total - first]
        for n, (method, amount) in enumerate(zip(methods, amounts, strict=True), start=1):
            payment_id = f"{order.order_id}-P{n}"
            order.charges.append((payment_id, method, amount))
            day.payments.append(
                (payment_id, order.order_id, "charge", method, amount, _iso(paid_at))
            )


def _order_second(rng: random.Random) -> int:
    """Second of the day an order is placed, drawn from the demand peaks."""
    while True:
        centre, spread, _ = rng.choices(PEAKS, [p[2] for p in PEAKS])[0]
        s = round(rng.gauss(centre, spread))
        if OPEN_SECOND <= s < CLOSE_SECOND:
            return s
