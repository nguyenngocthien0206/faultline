"""Fixed reference data for the simulated coffee chain.

Everything that drives the numbers (cities, menu, prices, promotion templates)
is hard-coded here rather than produced by Faker. Faker output may change
between versions; revenue must not.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    code: str
    name: str
    weight: float  # share of stores in this city
    districts: tuple[str, ...]


CITIES: tuple[City, ...] = (
    City(
        "HCM",
        "TP. Hồ Chí Minh",
        0.55,
        (
            "Quận 1",
            "Quận 3",
            "Quận 4",
            "Quận 5",
            "Quận 7",
            "Quận 10",
            "Bình Thạnh",
            "Phú Nhuận",
            "Tân Bình",
            "Gò Vấp",
            "Thủ Đức",
        ),
    ),
    City(
        "HN",
        "Hà Nội",
        0.35,
        (
            "Hoàn Kiếm",
            "Ba Đình",
            "Đống Đa",
            "Cầu Giấy",
            "Tây Hồ",
            "Hai Bà Trưng",
            "Thanh Xuân",
            "Long Biên",
            "Nam Từ Liêm",
        ),
    ),
    City(
        "DN",
        "Đà Nẵng",
        0.10,
        ("Hải Châu", "Sơn Trà", "Thanh Khê", "Ngũ Hành Sơn", "Liên Chiểu"),
    ),
)


@dataclass(frozen=True)
class Product:
    """A menu product. Drinks come in sizes; each size becomes its own menu item."""

    code: str
    name: str
    category: str
    prices: dict[str, int]  # size -> price in VND
    popularity: float  # relative weight when a customer picks an item
    seasonal: bool = False  # launched during the generated period


# Prices are integer VND, multiples of 1,000, in line with mid-range Vietnamese chains.
PRODUCTS: tuple[Product, ...] = (
    Product("CF-DEN", "Cà phê đen đá", "ca_phe", {"M": 25_000, "L": 29_000}, 6.0),
    Product("CF-SUA", "Cà phê sữa đá", "ca_phe", {"M": 29_000, "L": 35_000}, 10.0),
    Product("BAC-XIU", "Bạc xỉu", "ca_phe", {"M": 32_000, "L": 38_000}, 7.0),
    Product("CF-MUOI", "Cà phê muối", "ca_phe", {"M": 35_000, "L": 42_000}, 5.0),
    Product("COLD-BREW", "Cold brew cam sả", "ca_phe", {"M": 45_000, "L": 52_000}, 2.5),
    Product("LATTE", "Latte", "ca_phe", {"M": 45_000, "L": 52_000}, 3.0),
    Product("CAPPU", "Cappuccino", "ca_phe", {"M": 45_000, "L": 52_000}, 2.0),
    Product("TRA-DAO", "Trà đào cam sả", "tra", {"M": 45_000, "L": 52_000}, 6.0),
    Product("TRA-VAI", "Trà vải", "tra", {"M": 42_000, "L": 49_000}, 3.5),
    Product("TRA-SEN", "Trà sen vàng", "tra", {"M": 45_000, "L": 52_000}, 3.0),
    Product("MATCHA", "Trà xanh matcha latte", "tra", {"M": 49_000, "L": 55_000}, 2.5),
    Product("DX-COOKIE", "Cookie đá xay", "da_xay", {"M": 55_000, "L": 62_000}, 1.5),
    Product("DX-MATCHA", "Matcha đá xay", "da_xay", {"M": 55_000, "L": 62_000}, 1.5),
    Product("BANH-MI-QUE", "Bánh mì que", "banh", {"STD": 19_000}, 3.0),
    Product("CROISSANT", "Bánh sừng bò", "banh", {"STD": 35_000}, 1.5),
    Product("BANH-MI-THIT", "Bánh mì thịt nướng", "banh", {"STD": 29_000}, 2.5),
    Product("TIRAMISU", "Tiramisu", "banh", {"STD": 39_000}, 1.0),
    Product("TRA-OI", "Trà ổi hồng", "tra", {"M": 45_000, "L": 52_000}, 3.0, seasonal=True),
    Product("CF-TRUNG", "Cà phê trứng", "ca_phe", {"M": 45_000}, 2.0, seasonal=True),
)

# The menu item retired during the generated period.
RETIRED_ITEM_ID = "CAPPU-L"

# Price increases rotate through categories, one category per event.
PRICE_CHANGE_CATEGORIES: tuple[str, ...] = ("ca_phe", "tra", "da_xay", "banh")
PRICE_INCREASE_VND: dict[str, int] = {"ca_phe": 3_000, "tra": 3_000, "da_xay": 5_000, "banh": 2_000}


@dataclass(frozen=True)
class PromotionTemplate:
    prefix: str
    name: str
    kind: str  # "percent" or "fixed"
    value: int  # percent (1-100) or VND
    min_spend_vnd: int
    applies_to: str  # "all" or a category


# Always available: a small fixed discount that keeps every day promotable.
EVERGREEN_PROMOTION = PromotionTemplate(
    "CHAOBAN", "Giảm 10.000đ cho đơn từ 60.000đ", "fixed", 10_000, 60_000, "all"
)

CAMPAIGN_TEMPLATES: tuple[PromotionTemplate, ...] = (
    PromotionTemplate("CAPHE", "Cà phê giảm 15%", "percent", 15, 0, "ca_phe"),
    PromotionTemplate("TRAXANH", "Trà giảm 20%", "percent", 20, 0, "tra"),
    PromotionTemplate("DAXAY", "Đá xay giảm 25.000đ", "fixed", 25_000, 100_000, "da_xay"),
    PromotionTemplate("BANHNGON", "Bánh giảm 30%", "percent", 30, 0, "banh"),
    PromotionTemplate("NHOM4", "Giảm 10% cho đơn từ 150.000đ", "percent", 10, 150_000, "all"),
)

# Faker's vi_VN `name()` uses Western order and adds titles ("Quý cô ..."), so
# names are assembled here as family name + middle name + given name.
MIDDLE_NAMES: dict[str, tuple[str, ...]] = {
    "male": ("Văn", "Hữu", "Đức", "Quang", "Minh", "Thành", "Công", "Gia"),
    "female": ("Thị", "Ngọc", "Thu", "Mai", "Thanh", "Phương", "Kim", "Bảo"),
}

MEMBER_TIERS: tuple[str, ...] = ("dong", "bac", "vang", "kim_cuong")
MEMBER_TIER_WEIGHTS: tuple[float, ...] = (0.60, 0.25, 0.12, 0.03)
