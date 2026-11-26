"""The reference adapter's landing files, run stages and cleanup, on the real stack."""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunRequest, RunStatus, Stage
from faultline.adapters.reference import ReferenceAdapter, clean_reference_stack
from faultline.connectors.workspace import UnmarkedWorkspaceError, new_workspace_name
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


def test_clean_removes_a_leftover_sandbox_everywhere_and_nothing_else(stack: Stack) -> None:
    config = reference_config()
    bucket = stack.landing
    leftover, alive, foreign = new_workspace_name(), new_workspace_name(), new_workspace_name()
    for name in (leftover, alive):
        stack.source.create_workspace(name)
        stack.clickhouse.create_workspace(name)
        bucket.mark_workspace(name)
        bucket.put(f"{name}/pos/orders/dt=2026-11-01/x.parquet", b"left behind")
    # fl_-named like a sandbox, never marked as one: someone else's, never faultline's to drop
    stack.source.execute(f"CREATE SCHEMA {foreign}")
    stack.clickhouse.command(f"CREATE DATABASE {foreign}")
    bucket.put(f"{foreign}/pos/orders/dt=2026-11-01/x.parquet", b"not ours")
    try:
        # force: the rest of the suite runs alongside, so the stack is never quiet;
        # only: never touch the other tests' live sandboxes.
        report = clean_reference_stack(config, force=True, only={leftover, foreign})

        assert (report.landing, report.source, report.warehouse) == ((leftover,),) * 3
        assert report.unmarked == (foreign,)
        assert leftover not in bucket.list_workspaces()
        assert leftover not in stack.source.list_workspaces()
        assert leftover not in stack.clickhouse.list_workspaces()
        for names in (
            bucket.list_workspaces(),
            stack.source.list_workspaces(),
            stack.clickhouse.list_workspaces(),
        ):
            assert alive in names
            assert foreign in names
        with pytest.raises(UnmarkedWorkspaceError):
            stack.source.drop_workspace(foreign)
    finally:
        clean_reference_stack(config, force=True, only={alive})
        stack.source.execute(f"DROP SCHEMA IF EXISTS {foreign} CASCADE")
        stack.clickhouse.command(f"DROP DATABASE IF EXISTS {foreign}")
        bucket.delete(f"{foreign}/pos/orders/dt=2026-11-01/x.parquet")


def test_a_run_crashed_at_a_failpoint_is_interrupted_with_its_files_landed(
    stack: Stack, tmp_path: Path
) -> None:
    spec = DatasetSpec.from_profile("tiny")
    adapter = ReferenceAdapter(reference_config(), generate_truth(spec, tmp_path))
    day = spec.start_date

    with adapter:
        adapter.deliver(day)
        result = adapter.run(RunRequest(day, failpoint="after_landing"))

        assert result.status is RunStatus.INTERRUPTED, result.errors  # faultline's crash
        assert adapter.landing_files("pos/order_items", day)  # landed...
        sql = f"SELECT count() AS n FROM system.tables WHERE database = '{adapter.workspace}'"
        assert int(stack.clickhouse.query(sql)[0]["n"]) == 0  # ...never loaded
