from datetime import date
from pathlib import Path

import pytest

from faultline.adapters.base import (
    Adapter,
    Capabilities,
    Capability,
    RunRequest,
    RunResult,
    RunStatus,
    TableContract,
)

ORDERS = TableContract("silver_orders", key=("order_id",), measures=("total_vnd",))


class RecordingAdapter(Adapter):
    def __init__(self) -> None:
        self.calls: list[str] = []

    def capabilities(self) -> Capabilities:
        return Capabilities(frozenset({Capability.RUN_PARTITION}), (ORDERS,))

    def setup(self) -> None:
        self.calls.append("setup")

    def deliver(self, day: date) -> None:
        self.calls.append(f"deliver {day}")

    def run(self, request: RunRequest) -> RunResult:
        self.calls.append(f"run {request.partition}")
        return RunResult(RunStatus.SUCCEEDED)

    def snapshot(self, table: str, dest: Path) -> Path:
        return dest

    def teardown(self) -> None:
        self.calls.append("teardown")


def _experiment(adapter: Adapter) -> None:
    with adapter:
        adapter.deliver(date(2026, 11, 1))
        raise RuntimeError("experiment blew up")


def test_context_manager_sets_up_and_always_tears_down() -> None:
    adapter = RecordingAdapter()

    with pytest.raises(RuntimeError):
        _experiment(adapter)

    assert adapter.calls == ["setup", "deliver 2026-11-01", "teardown"]


def test_capabilities_answer_support_and_table_lookups() -> None:
    caps = RecordingAdapter().capabilities()

    assert caps.supports(Capability.RUN_PARTITION)
    assert not caps.supports(Capability.SNAPSHOT)
    assert caps.table("silver_orders") is ORDERS
    with pytest.raises(KeyError, match="gold"):
        caps.table("gold")


def test_only_succeeded_counts_as_success() -> None:
    assert RunResult(RunStatus.SUCCEEDED).succeeded
    assert not RunResult(RunStatus.FAILED, errors=("boom",)).succeeded
    assert not RunResult(RunStatus.INTERRUPTED).succeeded
