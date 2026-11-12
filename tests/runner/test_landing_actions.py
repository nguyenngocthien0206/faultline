"""Landing file actions in a schedule, executed against an in-memory landing zone."""

from datetime import date
from pathlib import Path

import pytest

from faultline.adapters.base import (
    Adapter,
    AdapterError,
    Capabilities,
    Capability,
    RunRequest,
    RunResult,
    RunStatus,
)
from faultline.runner import execute
from faultline.schedule import CopyLandingFiles, Run

D2, D3 = date(2026, 11, 2), date(2026, 11, 3)


class MemoryLanding(Adapter):
    """Just a landing zone: (dataset, partition, file) -> bytes."""

    def __init__(self) -> None:
        self.files: dict[tuple[str, date, str], bytes] = {}

    def capabilities(self) -> Capabilities:
        return Capabilities(frozenset({Capability.LANDING_FILES, Capability.RUN_PARTITION}))

    def setup(self) -> None: ...

    def teardown(self) -> None: ...

    def deliver(self, day: date) -> None: ...

    def run(self, request: RunRequest) -> RunResult:
        return RunResult(RunStatus.SUCCEEDED)

    def snapshot(self, table: str, dest: Path) -> Path:
        raise NotImplementedError

    def landing_files(self, dataset: str, partition: date) -> list[str]:
        return sorted(f for d, p, f in self.files if (d, p) == (dataset, partition))

    def read_landing_file(self, dataset: str, partition: date, file: str) -> bytes:
        return self.files[dataset, partition, file]

    def write_landing_file(self, dataset: str, partition: date, file: str, data: bytes) -> None:
        self.files[dataset, partition, file] = data


def test_files_land_again_under_the_new_partition_byte_for_byte() -> None:
    landing = MemoryLanding()
    landing.files["pos/orders", D2, "load-1.parquet"] = b"orders of day 2"
    landing.files["pos/orders", D3, "load-2.parquet"] = b"orders of day 3"
    lines: list[str] = []

    runs = execute(
        landing, (CopyLandingFiles("pos/orders", D2, D3), Run(D3)), lines.append, "fault"
    )

    assert landing.landing_files("pos/orders", D3) == [
        "load-2.parquet",
        "redelivered-load-1.parquet",
    ]
    assert landing.files["pos/orders", D3, "redelivered-load-1.parquet"] == b"orders of day 2"
    assert landing.landing_files("pos/orders", D2) == ["load-1.parquet"]  # the original stays
    assert [d for d, _ in runs] == [D3]  # a copy is not a run
    assert lines[0] == "  [fault] pos/orders: 1 file(s) of 2026-11-02 landed again under 2026-11-03"


def test_only_the_named_dataset_is_copied() -> None:
    landing = MemoryLanding()
    landing.files["pos/orders", D2, "a.parquet"] = b"orders"
    landing.files["pos/payments", D2, "a.parquet"] = b"payments"

    execute(landing, (CopyLandingFiles("pos/payments", D2, D3, prefix="retry-"),), print, "fault")

    assert landing.landing_files("pos/payments", D3) == ["retry-a.parquet"]
    assert landing.landing_files("pos/orders", D3) == []


def test_nothing_to_copy_is_an_error_not_a_quiet_no_op() -> None:
    # A fault that silently injected nothing would score a platform on no fault at all.
    with pytest.raises(AdapterError, match="no landing files of pos/orders for 2026-11-02"):
        execute(MemoryLanding(), (CopyLandingFiles("pos/orders", D2, D3),), print, "fault")
