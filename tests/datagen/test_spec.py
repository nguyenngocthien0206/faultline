from datetime import date

import pytest

from faultline.datagen.spec import PROFILES, DatasetSpec


def test_from_profile_takes_the_profile_volumes() -> None:
    spec = DatasetSpec.from_profile("tiny")

    assert spec.profile == "tiny"
    assert spec.stores == PROFILES["tiny"].stores
    assert spec.days == PROFILES["tiny"].days
    assert spec.members == PROFILES["tiny"].members


def test_overrides_win_and_none_keeps_the_profile_value() -> None:
    spec = DatasetSpec.from_profile("tiny", days=9, stores=None, start_date=date(2026, 1, 1))

    assert spec.days == 9
    assert spec.stores == PROFILES["tiny"].stores
    assert spec.start_date == date(2026, 1, 1)


def test_unknown_profile_is_rejected() -> None:
    with pytest.raises(ValueError, match="unknown profile"):
        DatasetSpec.from_profile("huge")


@pytest.mark.parametrize("field", ["days", "stores", "orders_per_store_day", "members"])
def test_volumes_must_be_positive(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        DatasetSpec.from_profile("tiny", **{field: 0})


def test_dates_cover_the_window_inclusively() -> None:
    spec = DatasetSpec.from_profile("tiny", start_date=date(2026, 11, 30), days=3)

    assert list(spec.dates()) == [date(2026, 11, 30), date(2026, 12, 1), date(2026, 12, 2)]
    assert spec.end_date == date(2026, 12, 2)


def test_to_dict_is_json_friendly() -> None:
    data = DatasetSpec.from_profile("tiny").to_dict()

    assert data["start_date"] == "2026-11-01"
    assert data["profile"] == "tiny"
