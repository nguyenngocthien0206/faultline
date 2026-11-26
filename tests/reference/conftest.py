"""Fixtures for tests that need the reference stack (`make ref-up`).

Without the stack these tests are skipped, so `make test` works anywhere.
CI sets FAULTLINE_REQUIRE_STACK=1, which turns "stack unreachable" into a
failure: a broken stack must never pass silently.

Tests that exercise the hardened edition carry the `hardened` marker; CI runs
them in a job of their own (`make test-integration EDITION=hardened`), next to
everything else (EDITION=naive).

The suite runs on several pytest-xdist workers with `--dist loadscope`: all
tests of one class (or, outside classes, of one module) go to the same worker.
Every worker is its own process with its own session fixtures, so the tests
that share a heavy fixture (a clean baseline, a live sandbox) live in one class
or module: each baseline is built once. Experiments share no baseline at all;
they run whole, baseline side by side with the fault phase.

Run with at least as many workers as units (`make test-integration` does):
xdist queues a second unit on a worker with two tests or fewer left, so an
experiment, a single long test, could otherwise wait behind another.

Not `--dist loadgroup` with `xdist_group` marks: with pytest 9, pytest-xdist
3.8 fails to attach the group to a test (it writes `item._nodeid`, which
pytest no longer reads), and every grouped test is scheduled on its own.
"""

import os
from collections.abc import Iterator

import pytest
from reference_stack import Stack
from shared_baseline import Baselines

from faultline.adapters.reference import Edition
from faultline.connectors.dagster import DagsterError
from faultline.connectors.workspace import new_workspace_name
from faultline.datagen import DatasetSpec
from faultline.runner import Baseline

TINY = DatasetSpec.from_profile("tiny")


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
    """A fresh, isolated workspace, marked in all three stores, and gone afterwards."""
    name = new_workspace_name()
    stack.source.create_workspace(name)
    stack.clickhouse.create_workspace(name)
    stack.landing.mark_workspace(name)
    yield name
    stack.source.drop_workspace(name)
    stack.clickhouse.drop_workspace(name)
    stack.landing.delete_workspace(name)


@pytest.fixture(scope="session")
def baselines(stack: Stack, tmp_path_factory: pytest.TempPathFactory, worker_id: str) -> Baselines:
    """Each edition's clean run of the tiny dataset, once for the whole session.

    Under xdist every worker's temporary folder sits in one session folder,
    which is where the shared baselines live (shared_baseline.py). Experiments
    pass it as `baseline=`, so each fault phase is compared with the same
    clean run instead of paying four runs for its own.
    """
    root = tmp_path_factory.getbasetemp()
    return Baselines(root if worker_id == "master" else root.parent)


def _established(baseline: Baseline) -> Baseline:
    assert baseline.established, baseline.problems
    return baseline


@pytest.fixture(scope="session")
def naive_baseline(baselines: Baselines) -> Baseline:
    """The naive pipeline's shared clean run, for the pipeline checks.

    Its snapshots live on disk; the sandbox itself is already torn down.
    """
    return _established(baselines(Edition.NAIVE))


@pytest.fixture(scope="session")
def hardened_baseline(baselines: Baselines) -> Baseline:
    """The same, on the hardened pipeline (with the platform's default lag window)."""
    return _established(baselines(Edition.HARDENED))
