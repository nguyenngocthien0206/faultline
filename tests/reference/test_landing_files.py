"""The reference adapter's landing files and run stages, on the real stack."""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunRequest, Stage
from faultline.adapters.reference import ReferenceAdapter
from faultline.datagen import DatasetSpec
from faultline.runner import generate_truth

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


def test_ingest_lands_the_files_and_load_builds_the_tables(stack: Stack, tmp_path: Path) -> None:
    spec = DatasetSpec.from_profile("tiny")
    adapter = ReferenceAdapter(reference_config(), generate_truth(spec, tmp_path))
    day = spec.start_date

    def tables() -> int:
        sql = f"SELECT count() AS n FROM system.tables WHERE database = '{adapter.workspace}'"
        return int(stack.clickhouse.query(sql)[0]["n"])

    with adapter:
        adapter.deliver(day)
        ingest = adapter.run(RunRequest(day, Stage.INGEST))
        assert ingest.succeeded, ingest.errors
        (lines,) = adapter.landing_files("pos/order_items", day)
        assert tables() == 0  # landed, not loaded: the gap landing faults act in

        load = adapter.run(RunRequest(day, Stage.LOAD))
        assert load.succeeded, load.errors
        assert tables() > 0

        adapter.delete_landing_file("pos/order_items", day, lines)
        assert adapter.landing_files("pos/order_items", day) == []
