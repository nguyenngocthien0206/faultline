"""Fixtures for tests that need the reference stack (`make ref-up`).

Without the stack these tests are skipped, so `make test` works anywhere.
CI sets FAULTLINE_REQUIRE_STACK=1, which turns "stack unreachable" into a
failure: a broken stack must never pass silently.
"""

import os
from collections.abc import Iterator

import pytest
from reference_stack import Stack

from faultline.connectors.dagster import DagsterError
from faultline.connectors.workspace import new_workspace_name


def _unreachable(reason: str) -> None:
    if os.environ.get("FAULTLINE_REQUIRE_STACK") == "1":
        pytest.fail(f"reference stack required but unreachable: {reason}")
    pytest.skip(f"reference stack not running ({reason}); start it with `make ref-up`")


@pytest.fixture(scope="session")
def stack() -> Iterator[Stack]:
    stack = Stack.from_env()
    if not stack.clickhouse.ping():
        _unreachable(f"ClickHouse at {stack.clickhouse.url}")
    if not stack.source.ping():
        _unreachable("source Postgres on 127.0.0.1:5433")
    try:
        stack.dagster.version()
        load_error = stack.dagster.location_error()
    except DagsterError as exc:
        _unreachable(str(exc))
    if load_error:
        # The stack is up but the platform code is broken: always a failure.
        pytest.fail(f"Dagster code location failed to load: {load_error}")

    yield stack
    stack.source.close()


@pytest.fixture
def workspace(stack: Stack) -> Iterator[str]:
    """A fresh, isolated workspace: Postgres schema now, ClickHouse database on demand."""
    name = new_workspace_name()
    stack.source.create_workspace(name)
    yield name
    stack.source.drop_workspace(name)
    stack.clickhouse.drop_workspace(name)
