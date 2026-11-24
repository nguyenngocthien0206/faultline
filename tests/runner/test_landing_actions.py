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
    Stage,
)
from faultline.runner import execute
from faultline.schedule import (
    CopyLandingFiles,
    HoldLandingFiles,
    ReleaseLandingFiles,
    Run,
    TruncateLandingFiles,
)

D2, D3 = date(2026, 11, 2), date(2026, 11, 3)


class MemoryLanding(Adapter):
    """Just a landing zone: (dataset, partition, file) -> bytes."""

    def __init__(self) -> None:
        self.files: dict[tuple[str, date, str], bytes] = {}
        self.requests: list[RunRequest] = []

    def capabilities(self) -> Capabilities:
        return Capabilities(frozenset({Capability.LANDING_FILES, Capability.RUN_PARTITION}))

    def setup(self) -> None: ...

    def teardown(self) -> None: ...

    def deliver(self, day: date) -> None: ...

    def run(self, request: RunRequest) -> RunResult:
        self.requests.append(request)
        return RunResult(RunStatus.SUCCEEDED)

    def snapshot(self, table: str, dest: Path) -> Path:
        raise NotImplementedError

    def landing_files(self, dataset: str, partition: date) -> list[str]:
        return sorted(f for d, p, f in self.files if (d, p) == (dataset, partition))

    def read_landing_file(self, dataset: str, partition: date, file: str) -> bytes:
        return self.files[dataset, partition, file]

    def write_landing_file(self, dataset: str, partition: date, file: str, data: bytes) -> None:
        self.files[dataset, partition, file] = data

    def delete_landing_file(self, dataset: str, partition: date, file: str) -> None:
        del self.files[dataset, partition, file]


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


def test_held_files_skip_the_load_and_land_late_unchanged() -> None:
    landing = MemoryLanding()
    landing.files["pos/order_items", D2, "load-1.parquet"] = b"lines of day 2"
    landing.files["pos/orders", D2, "load-1.parquet"] = b"orders of day 2"
    seen_by_load: list[list[str]] = []
    original_run = landing.run

    def run(request: RunRequest) -> RunResult:
        if request.stage is Stage.LOAD:
            seen_by_load.append(landing.landing_files("pos/order_items", request.partition))
        return original_run(request)

    landing.run = run  # type: ignore[method-assign]
    lines: list[str] = []

    execute(
        landing,
        (
            Run(D2, Stage.INGEST),
            HoldLandingFiles("pos/order_items", D2),
            Run(D2, Stage.LOAD),
            ReleaseLandingFiles("pos/order_items", D2),
        ),
        lines.append,
        "fault",
    )

    assert seen_by_load == [[]]  # the load found no order lines
    assert landing.files["pos/order_items", D2, "load-1.parquet"] == b"lines of day 2"  # back
    assert landing.landing_files("pos/orders", D2) == ["load-1.parquet"]  # never touched
    assert [r.stage for r in landing.requests] == [Stage.INGEST, Stage.LOAD]
    assert lines == [
        "  [fault] run 2026-11-02 (ingest): SUCCEEDED",
        "  [fault] pos/order_items: 1 file(s) of 2026-11-02 held back",
        "  [fault] run 2026-11-02 (load): SUCCEEDED",
        "  [fault] pos/order_items: 1 file(s) of 2026-11-02 put back in place",
    ]


def test_releasing_what_was_never_held_is_an_error() -> None:
    with pytest.raises(AdapterError, match="nothing of pos/orders for 2026-11-02 was held back"):
        execute(MemoryLanding(), (ReleaseLandingFiles("pos/orders", D2),), print, "fault")


def test_holding_nothing_is_an_error() -> None:
    with pytest.raises(AdapterError, match="no landing files of pos/orders for 2026-11-02 to hold"):
        execute(MemoryLanding(), (HoldLandingFiles("pos/orders", D2),), print, "fault")


def test_a_plain_run_covers_the_whole_partition() -> None:
    landing = MemoryLanding()
    lines: list[str] = []

    execute(landing, (Run(D3),), lines.append, "baseline")

    assert landing.requests == [RunRequest(D3)]
    assert lines == ["  [baseline] run 2026-11-03: SUCCEEDED"]


def test_a_cut_file_is_shorter_and_a_later_release_brings_the_full_one_back() -> None:
    landing = MemoryLanding()
    full = b"order_id,qty\no1,1\no2,2\no3,3\no4,4\no5,5\n"
    landing.files["pos/order_items", D3, "load-1.csv"] = full
    held: dict[tuple[str, date], list[tuple[str, bytes]]] = {}
    lines: list[str] = []

    execute(
        landing,
        (TruncateLandingFiles("pos/order_items", D3, keep=0.6),),
        lines.append,
        "fault",
        held,
    )
    cut = landing.files["pos/order_items", D3, "load-1.csv"]
    # The recovery is another phase: the same store of held files carries the original over.
    execute(landing, (ReleaseLandingFiles("pos/order_items", D3),), lines.append, "recovery", held)

    assert cut == b"order_id,qty\no1,1\no2,2\no3,3\n"
    assert landing.files["pos/order_items", D3, "load-1.csv"] == full
    assert lines == [
        "  [fault] pos/order_items: 1 file(s) of 2026-11-03 cut to 60% of their rows",
        "  [recovery] pos/order_items: 1 file(s) of 2026-11-03 put back in place",
    ]


def test_cutting_nothing_is_an_error() -> None:
    with pytest.raises(AdapterError, match="no landing files of pos/orders for 2026-11-02 to cut"):
        execute(MemoryLanding(), (TruncateLandingFiles("pos/orders", D2),), print, "fault")
