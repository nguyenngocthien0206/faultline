from datetime import date, timedelta
from itertools import pairwise

import pytest

from faultline.datagen import catalog
from faultline.datagen.dimensions import (
    TIER_REVIEW_EVERY_DAYS,
    TIER_REVIEW_FIRST_OFFSET,
    MemberIndex,
    Promotion,
    _allocate,
    build_dimensions,
    build_members,
    build_menu,
    build_stores,
    member_changes_by_day,
    member_delta_rows,
    member_full_rows,
    menu_rows,
    store_rows,
)
from faultline.datagen.spec import DatasetSpec

SPEC = DatasetSpec.from_profile("tiny")
START = SPEC.start_date


def day(offset: int) -> date:
    return START + timedelta(days=offset)


# ------------------------------------------------------------------------- stores


def test_allocate_is_exact_and_proportional() -> None:
    assert _allocate(10, [0.55, 0.35, 0.10]) == [6, 3, 1]
    assert sum(_allocate(7, [0.55, 0.35, 0.10])) == 7


def test_stores_have_unique_ids_and_the_requested_count() -> None:
    stores = build_stores(DatasetSpec.from_profile("demo"))

    assert len(stores) == 50
    assert len({s.store_id for s in stores}) == 50


def test_one_store_opens_and_one_closes_inside_the_tiny_window() -> None:
    stores = build_stores(SPEC)

    opening = [s for s in stores if START < s.opened_on <= SPEC.end_date]
    closing = [s for s in stores if s.closed_on is not None and s.closed_on <= SPEC.end_date]
    assert len(opening) == 1
    assert len(closing) == 1


def test_store_snapshot_hides_the_closing_date_until_the_store_closes() -> None:
    dims = build_dimensions(SPEC)
    closing = next(s for s in dims.stores if s.closed_on is not None)
    assert closing.closed_on is not None

    before = {r[0]: r for r in store_rows(dims, closing.closed_on - timedelta(days=1))}
    after = {r[0]: r for r in store_rows(dims, closing.closed_on)}
    assert before[closing.store_id][6:] == (None, "open")
    assert after[closing.store_id][6:] == (closing.closed_on, "closed")


def test_store_is_planned_before_it_opens() -> None:
    store = next(s for s in build_stores(SPEC) if s.opened_on > START)

    assert store.status_on(store.opened_on - timedelta(days=1)) == "planned"
    assert not store.is_open(store.opened_on - timedelta(days=1))
    assert store.is_open(store.opened_on)


# --------------------------------------------------------------------------- menu


def test_coffee_price_rises_on_the_first_price_change_day() -> None:
    menu = build_menu(SPEC)

    before = menu.version_on("CF-SUA-M", day(1))
    after = menu.version_on("CF-SUA-M", day(2))
    assert before is not None
    assert after is not None
    assert before.price_vnd == 29_000
    assert after.price_vnd == 29_000 + catalog.PRICE_INCREASE_VND["ca_phe"]


def test_other_categories_keep_their_price_in_the_tiny_window() -> None:
    menu = build_menu(SPEC)

    prices = {menu.version_on("TRA-DAO-M", d).price_vnd for d in SPEC.dates()}
    assert prices == {45_000}


def test_all_prices_are_whole_thousands() -> None:
    menu = build_menu(DatasetSpec.from_profile("tiny", days=200))

    assert all(v.price_vnd % 1000 == 0 for v in menu.versions)


def test_versions_of_an_item_do_not_overlap() -> None:
    menu = build_menu(DatasetSpec.from_profile("tiny", days=200))
    versions = sorted(
        (v for v in menu.versions if v.item_id == "CF-SUA-M"), key=lambda v: v.valid_from
    )

    for older, newer in pairwise(versions):
        assert older.valid_to == newer.valid_from
    assert versions[-1].valid_to is None


def test_seasonal_item_appears_on_its_launch_day() -> None:
    menu = build_menu(SPEC)

    assert not menu.is_available("TRA-OI-M", day(0))
    assert menu.is_available("TRA-OI-M", day(1))
    assert "TRA-OI-M" not in {v.item_id for v in menu.known_on(day(0))}


def test_retired_item_stays_in_the_snapshot_but_inactive() -> None:
    dims = build_dimensions(SPEC)
    rows = {r[0]: r for r in menu_rows(dims, day(2))}

    assert rows[catalog.RETIRED_ITEM_ID][-1] == "false"
    assert not dims.menu.is_available(catalog.RETIRED_ITEM_ID, day(2))
    assert dims.menu.is_available(catalog.RETIRED_ITEM_ID, day(1))


# --------------------------------------------------------------------- promotions


def _promo(kind: str, value: int, min_spend: int = 0, applies_to: str = "all") -> Promotion:
    return Promotion("P", "p", kind, value, min_spend, applies_to, START, None, START)


@pytest.mark.parametrize(
    ("promo", "subtotal", "by_category", "expected"),
    [
        (_promo("percent", 15), 64_000, {"ca_phe": 64_000}, 9_000),  # 9,600 rounded down
        (_promo("percent", 20, applies_to="tra"), 90_000, {"ca_phe": 45_000, "tra": 45_000}, 9_000),
        (_promo("percent", 20, applies_to="tra"), 45_000, {"ca_phe": 45_000}, 0),
        (_promo("fixed", 10_000, min_spend=60_000), 59_000, {"ca_phe": 59_000}, 0),
        (_promo("fixed", 10_000, min_spend=60_000), 60_000, {"ca_phe": 60_000}, 10_000),
        (_promo("fixed", 25_000, applies_to="da_xay"), 80_000, {"da_xay": 20_000}, 20_000),
    ],
)
def test_discount_rules(
    promo: Promotion, subtotal: int, by_category: dict[str, int], expected: int
) -> None:
    assert promo.discount_vnd(subtotal, by_category) == expected


def test_promotion_codes_are_unique_and_there_is_always_one_active() -> None:
    dims = build_dimensions(DatasetSpec.from_profile("tiny", days=60))
    codes = [p.promo_code for p in dims.promotions]

    assert len(codes) == len(set(codes))
    for d in DatasetSpec.from_profile("tiny", days=60).dates():
        assert any(p.is_active(d) for p in dims.promotions)


# ------------------------------------------------------------------------ members


def test_member_ids_follow_join_order() -> None:
    members = build_members(SPEC)

    assert len(members) == SPEC.members
    assert [m.joined_at for m in members] == sorted(m.joined_at for m in members)
    assert members[0].member_id == "M000001"


def test_member_index_for_spec_matches_the_full_build() -> None:
    assert MemberIndex.for_spec(SPEC) == MemberIndex.from_members(build_members(SPEC))


def test_upgrades_are_one_tier_on_a_review_day() -> None:
    members = build_members(DatasetSpec.from_profile("tiny", days=365))
    reviews = {
        START + timedelta(days=TIER_REVIEW_FIRST_OFFSET + k * TIER_REVIEW_EVERY_DAYS)
        for k in range(13)
    }
    upgraded = [m for m in members if m.upgraded_at is not None]

    assert upgraded
    for m in upgraded:
        assert m.upgraded_at is not None
        assert m.upgraded_at.date() in reviews
        assert m.upgraded_at.date() > m.joined_at.date()
        before = catalog.MEMBER_TIERS.index(m.initial_tier)
        assert catalog.MEMBER_TIERS.index(m.tier_at(m.upgraded_at)) == before + 1


def test_member_delta_contains_only_that_days_changes() -> None:
    dims = build_dimensions(SPEC)
    changes = member_changes_by_day(dims)
    by_id = {m.member_id: m for m in dims.members}

    seen_join, seen_upgrade = False, False
    for d in list(SPEC.dates())[1:]:
        for row in member_delta_rows(changes, d):
            m = by_id[str(row[0])]
            joined_today = m.joined_at.date() == d
            upgraded_today = m.upgraded_at is not None and m.upgraded_at.date() == d
            assert joined_today or upgraded_today
            seen_join |= joined_today
            seen_upgrade |= upgraded_today
    assert seen_join, "tiny must contain members who join inside the window"
    assert seen_upgrade, "tiny must contain tier upgrades inside the window"


def test_member_full_load_is_everyone_joined_by_that_day() -> None:
    dims = build_dimensions(SPEC)
    rows = list(member_full_rows(dims, START))

    assert len(rows) == sum(1 for m in dims.members if m.joined_at.date() <= START)


def test_names_are_vietnamese_order_without_titles() -> None:
    members = build_members(SPEC)

    for m in members[:200]:
        parts = m.full_name.split()
        assert len(parts) >= 3
        assert parts[0] not in {"Quý", "Bà", "Bác", "Ông", "Cô"}
