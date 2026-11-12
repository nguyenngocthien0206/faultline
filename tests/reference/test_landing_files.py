"""The reference adapter's landing files, on the real bucket (RustFS)."""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.reference import ReferenceAdapter

pytestmark = pytest.mark.integration

DAY = date(2026, 11, 2)


def test_landing_files_round_trip_and_teardown_empties_the_prefix(stack: Stack) -> None:
    adapter = ReferenceAdapter(reference_config(), Path("unused"))

    with adapter:
        adapter.write_landing_file("pos/orders", DAY, "probe.parquet", b"PAR1 not parquet")

        assert adapter.landing_files("pos/orders", DAY) == ["probe.parquet"]
        assert adapter.read_landing_file("pos/orders", DAY, "probe.parquet") == b"PAR1 not parquet"
        assert adapter.landing_files("pos/orders", date(2026, 11, 3)) == []
        assert adapter.landing_files("pos/payments", DAY) == []

    assert adapter.landing.list(f"{adapter.workspace}/") == []  # nothing left behind
