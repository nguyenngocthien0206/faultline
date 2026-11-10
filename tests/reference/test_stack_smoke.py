"""End-to-end plumbing check of the reference stack.

Postgres source -> dlt -> S3 (RustFS) Parquet -> ClickHouse, launched and
observed through Dagster's GraphQL API, exactly as faultline will drive it.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from reference_stack import Stack

pytestmark = pytest.mark.integration

PARTITION = "2026-11-01"


def _run_config(workspace: str) -> dict[str, object]:
    return {"resources": {"workspace": {"config": {"name": workspace}}}}


def _seed_ping(stack: Stack, workspace: str, rows: int) -> None:
    stack.source.execute(
        f'CREATE TABLE "{workspace}".ping (id integer PRIMARY KEY, updated_at timestamptz NOT NULL)'
    )
    stamp = datetime(2026, 11, 1, 8, 0, tzinfo=UTC)
    stack.source.copy_rows(
        workspace, "ping", ("id", "updated_at"), [(i, stamp) for i in range(rows)]
    )


def test_a_run_moves_rows_from_postgres_through_s3_into_clickhouse(
    stack: Stack, workspace: str
) -> None:
    _seed_ping(stack, workspace, rows=3)

    run_id = stack.dagster.launch_run(
        "smoke_job", partition=PARTITION, run_config=_run_config(workspace)
    )
    outcome = stack.dagster.wait_for_run(run_id, timeout=300)

    assert outcome.succeeded, outcome.errors
    loaded = stack.clickhouse.query(f"SELECT count() AS n FROM {workspace}.smoke_ping")
    landed = stack.clickhouse.query(
        "SELECT count() AS n FROM s3(landing_s3, "
        f"filename = '{workspace}/smoke/ping/dt={PARTITION}/*.parquet', format = 'Parquet')"
    )
    assert loaded == [{"n": 3}]
    assert landed == [{"n": 3}]


def test_a_failing_run_reports_failure_with_its_error(stack: Stack, workspace: str) -> None:
    # No `ping` table in this workspace: the extract step must fail loudly.
    run_id = stack.dagster.launch_run(
        "smoke_job", partition=PARTITION, run_config=_run_config(workspace)
    )
    outcome = stack.dagster.wait_for_run(run_id, timeout=300)

    assert outcome.status == "FAILURE"
    assert any("smoke_ping" in e for e in outcome.errors), outcome.errors
    assert any("ping" in e.lower() for e in outcome.errors)
