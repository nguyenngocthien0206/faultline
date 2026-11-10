from datetime import date
from pathlib import Path

from faultline.adapters.base import (
    Adapter,
    Capabilities,
    Capability,
    RunRequest,
    RunResult,
    RunStatus,
    TableContract,
)
from faultline.oracle.snapshot import snapshot_all


class TwoTables(Adapter):
    def capabilities(self) -> Capabilities:
        tables = (TableContract("silver_orders", ("order_id",)), TableContract("gold", ("d",)))
        return Capabilities(frozenset({Capability.SNAPSHOT}), tables)

    def setup(self) -> None: ...
    def deliver(self, day: date) -> None: ...
    def run(self, request: RunRequest) -> RunResult:
        return RunResult(RunStatus.SUCCEEDED)

    def snapshot(self, table: str, dest: Path) -> Path:
        dest.write_text(table, encoding="utf-8")
        return dest

    def teardown(self) -> None: ...


def test_snapshot_all_exports_every_declared_table(tmp_path: Path) -> None:
    paths = snapshot_all(TwoTables(), tmp_path)

    assert paths == {
        "silver_orders": tmp_path / "silver_orders.parquet",
        "gold": tmp_path / "gold.parquet",
    }
    assert (tmp_path / "gold.parquet").read_text(encoding="utf-8") == "gold"
