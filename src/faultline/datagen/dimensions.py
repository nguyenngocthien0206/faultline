"""Backoffice dimensions: stores, menu price history, promotions and members.

Every change event (price increase, store opening, member upgrade) is placed on
a calendar anchored at `spec.start_date`, never derived from `spec.days`.
That keeps the history of day N identical whether 5 or 50 days are generated,
so extending a dataset never rewrites partitions that already exist.
"""

from __future__ import annotations

import random
from bisect import bisect_left
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone

from faker import Faker

from faultline.datagen import catalog
from faultline.datagen.spec import DatasetSpec

LOCAL_TZ = timezone(timedelta(hours=7), "Asia/Ho_Chi_Minh")

# Calendar of change events, as day offsets from start_date.
PRICE_CHANGE_FIRST_OFFSET = 2
PRICE_CHANGE_EVERY_DAYS = 30
SEASONAL_LAUNCH_OFFSETS = (1, 45)  # one offset per seasonal product, in catalog order
RETIRE_OFFSET = 2
STORE_OPENING_OFFSETS_START, STORE_OPENING_EVERY_DAYS = 1, 30
STORE_CLOSING_OFFSETS_START, STORE_CLOSING_EVERY_DAYS = 2, 45
CAMPAIGN_EVERY_DAYS = 7
TIER_REVIEW_FIRST_OFFSET, TIER_REVIEW_EVERY_DAYS = 1, 30

# Members: `spec.members` is the member base over a one-year horizon.
MEMBER_HORIZON_DAYS = 365
MEMBER_HISTORY_DAYS = 1500
NEW_MEMBER_SHARE = 0.15
UPGRADE_PROBABILITY_PER_REVIEW = 0.03

FAR_PAST_OFFSET = -400  # launch date of regular menu items


def local_dt(d: date, seconds: int) -> datetime:
    """A timezone-aware local timestamp, `seconds` after midnight of day `d`."""
    return datetime.combine(d, time(), LOCAL_TZ) + timedelta(seconds=seconds)


def _rng(spec: DatasetSpec, *parts: object) -> random.Random:
    return random.Random(":".join(str(p) for p in (spec.seed, *parts)))


# --------------------------------------------------------------------------- stores


@dataclass(frozen=True)
class Store:
    store_id: str
    name: str
    city: str
    district: str
    address: str
    opened_on: date
    closed_on: date | None
    traffic_weight: float  # generator-only, never exported

    def is_open(self, d: date) -> bool:
        return self.opened_on <= d and (self.closed_on is None or d < self.closed_on)

    def status_on(self, d: date) -> str:
        if d < self.opened_on:
            return "planned"
        if self.closed_on is not None and d >= self.closed_on:
            return "closed"
        return "open"


def _allocate(total: int, weights: list[float]) -> list[int]:
    """Split `total` across weights with the largest remainder method (deterministic)."""
    raw = [total * w / sum(weights) for w in weights]
    counts = [int(x) for x in raw]
    by_remainder = sorted(range(len(raw)), key=lambda i: (raw[i] - counts[i], -i), reverse=True)
    for i in by_remainder[: total - sum(counts)]:
        counts[i] += 1
    return counts


def build_stores(spec: DatasetSpec) -> tuple[Store, ...]:
    rng = _rng(spec, "stores")
    fake = Faker("vi_VN")
    fake.seed_instance(f"{spec.seed}:stores")

    drafts: list[dict[str, object]] = []
    counts = _allocate(spec.stores, [c.weight for c in catalog.CITIES])
    for city, count in zip(catalog.CITIES, counts, strict=True):
        for n in range(count):
            district = city.districts[n % len(city.districts)]
            branch = n // len(city.districts)
            # Draw order matters for determinism: house number, then street.
            house_number = rng.randint(1, 350)
            street = fake.street_name()
            drafts.append(
                {
                    "store_id": f"{city.code}-{n + 1:03d}",
                    "name": f"Chi nhánh {district}" + (f" {branch + 1}" if branch else ""),
                    "city": city.name,
                    "district": district,
                    "address": f"{house_number} {street}, {district}, {city.name}",
                    "opened_on": spec.start_date - timedelta(days=rng.randint(60, 2500)),
                    "closed_on": None,
                    "traffic_weight": round(rng.uniform(0.6, 1.4), 2),
                }
            )

    # A few stores open or close inside the period, on a fixed calendar.
    indices = list(range(len(drafts)))
    rng.shuffle(indices)
    n_open = max(1, round(spec.stores * 0.04))
    n_close = max(1, round(spec.stores * 0.02)) if spec.stores >= 3 else 0
    for k, i in enumerate(indices[:n_open]):
        offset = STORE_OPENING_OFFSETS_START + k * STORE_OPENING_EVERY_DAYS
        drafts[i]["opened_on"] = spec.start_date + timedelta(days=offset)
    for k, i in enumerate(indices[n_open : n_open + n_close]):
        offset = STORE_CLOSING_OFFSETS_START + k * STORE_CLOSING_EVERY_DAYS
        drafts[i]["closed_on"] = spec.start_date + timedelta(days=offset)

    return tuple(Store(**d) for d in drafts)  # type: ignore[arg-type]


# ----------------------------------------------------------------------------- menu


@dataclass(frozen=True)
class MenuItem:
    """One price version of one menu item. `valid_to` is exclusive."""

    item_id: str
    product_code: str
    name: str
    category: str
    size: str
    price_vnd: int
    popularity: float
    valid_from: date
    valid_to: date | None


@dataclass(frozen=True)
class Menu:
    versions: tuple[MenuItem, ...]
    launched_on: dict[str, date]
    retired_on: dict[str, date]

    def version_on(self, item_id: str, d: date) -> MenuItem | None:
        for v in self.versions:
            if (
                v.item_id == item_id
                and v.valid_from <= d
                and (v.valid_to is None or d < v.valid_to)
            ):
                return v
        return None

    def is_available(self, item_id: str, d: date) -> bool:
        retired = self.retired_on.get(item_id)
        return self.launched_on[item_id] <= d and (retired is None or d < retired)

    def known_on(self, d: date) -> list[MenuItem]:
        """Current version of every item launched by `d`, retired ones included."""
        out = []
        for item_id in self.launched_on:
            v = self.version_on(item_id, d)
            if v is not None:
                out.append(v)
        return out

    def available_on(self, d: date) -> list[MenuItem]:
        return [v for v in self.known_on(d) if self.is_available(v.item_id, d)]


def price_change_dates(spec: DatasetSpec, until: date) -> list[tuple[date, str]]:
    """(date, category) of every price increase up to `until`, rotating categories."""
    out = []
    k = 0
    while True:
        d = spec.start_date + timedelta(
            days=PRICE_CHANGE_FIRST_OFFSET + k * PRICE_CHANGE_EVERY_DAYS
        )
        if d > until:
            return out
        out.append((d, catalog.PRICE_CHANGE_CATEGORIES[k % len(catalog.PRICE_CHANGE_CATEGORIES)]))
        k += 1


def build_menu(spec: DatasetSpec) -> Menu:
    far_past = spec.start_date + timedelta(days=FAR_PAST_OFFSET)
    events = price_change_dates(spec, spec.end_date)
    seasonal = iter(SEASONAL_LAUNCH_OFFSETS)

    versions: list[MenuItem] = []
    launched_on: dict[str, date] = {}
    for product in catalog.PRODUCTS:
        launch = spec.start_date + timedelta(days=next(seasonal)) if product.seasonal else far_past
        for size, base_price in product.prices.items():
            item_id = f"{product.code}-{size}"
            launched_on[item_id] = launch
            # Price boundaries: launch, then every category price change after launch.
            changes = [d for d, cat in events if cat == product.category]
            boundaries = [launch] + [d for d in changes if d > launch]
            for i, start in enumerate(boundaries):
                bumps = sum(1 for d in changes if d <= start)
                versions.append(
                    MenuItem(
                        item_id=item_id,
                        product_code=product.code,
                        name=product.name,
                        category=product.category,
                        size=size,
                        price_vnd=base_price + bumps * catalog.PRICE_INCREASE_VND[product.category],
                        popularity=product.popularity,
                        valid_from=start,
                        valid_to=boundaries[i + 1] if i + 1 < len(boundaries) else None,
                    )
                )

    retired = {catalog.RETIRED_ITEM_ID: spec.start_date + timedelta(days=RETIRE_OFFSET)}
    return Menu(versions=tuple(versions), launched_on=launched_on, retired_on=retired)


# ----------------------------------------------------------------------- promotions


@dataclass(frozen=True)
class Promotion:
    promo_code: str
    name: str
    kind: str
    value: int
    min_spend_vnd: int
    applies_to: str
    valid_from: date
    valid_to: date | None  # inclusive; None means no end date
    created_on: date

    def is_active(self, d: date) -> bool:
        return self.valid_from <= d and (self.valid_to is None or d <= self.valid_to)

    def discount_vnd(self, subtotal_vnd: int, by_category: dict[str, int]) -> int:
        """Discount for an order, 0 when the promotion does not apply."""
        if subtotal_vnd < self.min_spend_vnd:
            return 0
        base = subtotal_vnd if self.applies_to == "all" else by_category.get(self.applies_to, 0)
        if base == 0:
            return 0
        if self.kind == "percent":
            # Vietnamese prices are quoted in thousands: round the discount down.
            return base * self.value // 100 // 1000 * 1000
        return min(self.value, base)


def build_promotions(spec: DatasetSpec) -> tuple[Promotion, ...]:
    evergreen = catalog.EVERGREEN_PROMOTION
    promos = [
        Promotion(
            promo_code=evergreen.prefix,
            name=evergreen.name,
            kind=evergreen.kind,
            value=evergreen.value,
            min_spend_vnd=evergreen.min_spend_vnd,
            applies_to=evergreen.applies_to,
            valid_from=spec.start_date - timedelta(days=30),
            valid_to=None,
            created_on=spec.start_date - timedelta(days=33),
        )
    ]
    k = 0
    while True:
        valid_from = spec.start_date + timedelta(days=k * CAMPAIGN_EVERY_DAYS)
        created_on = valid_from - timedelta(days=3)
        if created_on > spec.end_date:
            break
        template = catalog.CAMPAIGN_TEMPLATES[k % len(catalog.CAMPAIGN_TEMPLATES)]
        duration = _rng(spec, "promo", k).randint(3, 10)
        promos.append(
            Promotion(
                promo_code=f"{template.prefix}{valid_from:%y%m%d}",
                name=template.name,
                kind=template.kind,
                value=template.value,
                min_spend_vnd=template.min_spend_vnd,
                applies_to=template.applies_to,
                valid_from=valid_from,
                valid_to=valid_from + timedelta(days=duration - 1),
                created_on=created_on,
            )
        )
        k += 1
    return tuple(promos)


# -------------------------------------------------------------------------- members


@dataclass(frozen=True)
class Member:
    member_id: str
    full_name: str
    phone: str
    city: str
    joined_at: datetime
    initial_tier: str
    upgraded_at: datetime | None  # at most one upgrade inside the horizon

    def tier_at(self, moment: datetime) -> str:
        if self.upgraded_at is not None and self.upgraded_at <= moment:
            return catalog.MEMBER_TIERS[catalog.MEMBER_TIERS.index(self.initial_tier) + 1]
        return self.initial_tier

    def updated_at(self, moment: datetime) -> datetime:
        """Timestamp of the last change visible at `moment`."""
        if self.upgraded_at is not None and self.upgraded_at <= moment:
            return self.upgraded_at
        return self.joined_at


def _member_joins(spec: DatasetSpec) -> tuple[list[datetime], random.Random]:
    """Sorted join timestamps, plus the random stream positioned right after them.

    Split out so POS workers can rebuild who-joined-when without generating
    names; `build_members` continues drawing from the returned stream.
    """
    rng = _rng(spec, "members")
    n_new = round(spec.members * NEW_MEMBER_SHARE)
    n_old = spec.members - n_new

    # Existing members are spread over past years; new members are evenly
    # spaced over the next year so that every day sees a predictable trickle.
    joins: list[datetime] = []
    for _ in range(n_old):
        d = spec.start_date - timedelta(days=rng.randint(1, MEMBER_HISTORY_DAYS))
        joins.append(local_dt(d, rng.randint(7 * 3600, 22 * 3600 - 1)))
    for i in range(n_new):
        d = spec.start_date + timedelta(days=i * MEMBER_HORIZON_DAYS // max(n_new, 1))
        joins.append(local_dt(d, rng.randint(7 * 3600, 22 * 3600 - 1)))
    joins.sort()
    return joins, rng


def build_members(spec: DatasetSpec) -> tuple[Member, ...]:
    joins, rng = _member_joins(spec)

    reviews = [
        spec.start_date + timedelta(days=TIER_REVIEW_FIRST_OFFSET + k * TIER_REVIEW_EVERY_DAYS)
        for k in range(
            (MEMBER_HORIZON_DAYS - TIER_REVIEW_FIRST_OFFSET) // TIER_REVIEW_EVERY_DAYS + 1
        )
    ]

    fake = Faker("vi_VN")
    fake.seed_instance(f"{spec.seed}:members")
    city_names = [c.name for c in catalog.CITIES]
    city_weights = [c.weight for c in catalog.CITIES]
    top_tier = catalog.MEMBER_TIERS[-1]

    members = []
    for n, joined_at in enumerate(joins, start=1):
        tier = rng.choices(catalog.MEMBER_TIERS, catalog.MEMBER_TIER_WEIGHTS)[0]
        upgraded_at = None
        if tier != top_tier:
            for review in reviews:
                hit = rng.random() < UPGRADE_PROBABILITY_PER_REVIEW
                if hit and review > joined_at.date():
                    upgraded_at = local_dt(review, 2 * 3600)  # nightly tier job at 02:00
                    break
        gender = rng.choice(("male", "female"))
        given = fake.first_name_male() if gender == "male" else fake.first_name_female()
        full_name = f"{fake.last_name()} {rng.choice(catalog.MIDDLE_NAMES[gender])} {given}"
        members.append(
            Member(
                member_id=f"M{n:06d}",  # keep in sync with MemberIndex.for_spec
                full_name=full_name,
                phone=fake.phone_number(),
                city=rng.choices(city_names, city_weights)[0],
                joined_at=joined_at,
                initial_tier=tier,
                upgraded_at=upgraded_at,
            )
        )
    return tuple(members)


@dataclass(frozen=True)
class MemberIndex:
    """Compact, picklable view for POS workers: who can order on which day."""

    member_ids: tuple[str, ...]
    join_ordinals: tuple[int, ...]  # sorted, aligned with member_ids

    @classmethod
    def from_members(cls, members: tuple[Member, ...]) -> MemberIndex:
        return cls(
            member_ids=tuple(m.member_id for m in members),
            join_ordinals=tuple(m.joined_at.date().toordinal() for m in members),
        )

    @classmethod
    def for_spec(cls, spec: DatasetSpec) -> MemberIndex:
        """Same result as `from_members(build_members(spec))`, without generating names."""
        joins, _ = _member_joins(spec)
        return cls(
            member_ids=tuple(f"M{n:06d}" for n in range(1, len(joins) + 1)),
            join_ordinals=tuple(j.date().toordinal() for j in joins),
        )

    def eligible_count(self, d: date) -> int:
        """Members who joined strictly before `d` (ids are ordered by join time)."""
        return bisect_left(self.join_ordinals, d.toordinal())


# ----------------------------------------------------------------- snapshots / delta


@dataclass(frozen=True)
class Dimensions:
    stores: tuple[Store, ...]
    menu: Menu
    promotions: tuple[Promotion, ...]
    members: tuple[Member, ...]


def build_dimensions(spec: DatasetSpec) -> Dimensions:
    return Dimensions(
        stores=build_stores(spec),
        menu=build_menu(spec),
        promotions=build_promotions(spec),
        members=build_members(spec),
    )


STORE_COLUMNS = (
    "store_id",
    "name",
    "city",
    "district",
    "address",
    "opened_on",
    "closed_on",
    "status",
)
MENU_COLUMNS = ("item_id", "product_code", "name", "category", "size", "price_vnd", "is_active")
PROMOTION_COLUMNS = (
    "promo_code",
    "name",
    "kind",
    "value",
    "min_spend_vnd",
    "applies_to",
    "valid_from",
    "valid_to",
)
MEMBER_COLUMNS = ("member_id", "full_name", "phone", "city", "joined_on", "tier", "updated_at")

Row = tuple[object, ...]


def store_rows(dims: Dimensions, d: date) -> Iterator[Row]:
    """Full snapshot of the store register as the ERP shows it on day `d`."""
    for s in dims.stores:
        status = s.status_on(d)
        closed_on = s.closed_on if status == "closed" else None
        yield (s.store_id, s.name, s.city, s.district, s.address, s.opened_on, closed_on, status)


def menu_rows(dims: Dimensions, d: date) -> Iterator[Row]:
    for v in sorted(dims.menu.known_on(d), key=lambda v: v.item_id):
        active = "true" if dims.menu.is_available(v.item_id, d) else "false"
        yield (v.item_id, v.product_code, v.name, v.category, v.size, v.price_vnd, active)


def promotion_rows(dims: Dimensions, d: date) -> Iterator[Row]:
    for p in dims.promotions:
        if p.created_on <= d:
            yield (
                p.promo_code,
                p.name,
                p.kind,
                p.value,
                p.min_spend_vnd,
                p.applies_to,
                p.valid_from,
                p.valid_to,
            )


def _member_row(m: Member, d: date) -> Row:
    end_of_day = local_dt(d, 24 * 3600 - 1)
    return (
        m.member_id,
        m.full_name,
        m.phone,
        m.city,
        m.joined_at.date(),
        m.tier_at(end_of_day),
        m.updated_at(end_of_day),
    )


def member_full_rows(dims: Dimensions, d: date) -> Iterator[Row]:
    """Initial load: every member who has joined by the end of day `d`."""
    for m in dims.members:
        if m.joined_at.date() > d:
            break  # members are sorted by join time
        yield _member_row(m, d)


def member_changes_by_day(dims: Dimensions) -> dict[date, list[Member]]:
    """Index members by the days they changed (joined or upgraded), computed once."""
    changes: dict[date, list[Member]] = {}
    for m in dims.members:
        days = {m.joined_at.date()}
        if m.upgraded_at is not None:
            days.add(m.upgraded_at.date())
        for d in days:
            changes.setdefault(d, []).append(m)
    return changes


def member_delta_rows(changes: dict[date, list[Member]], d: date) -> Iterator[Row]:
    """Delta extract: only members who joined or changed tier on day `d`."""
    for m in sorted(changes.get(d, []), key=lambda m: m.member_id):
        yield _member_row(m, d)
