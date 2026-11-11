"""A toy platform in plain Python, to exercise the runner without Docker.

Source: the generated POS orders, delivered day by day. Extraction: an
incremental cursor on updated_at, like dlt. Output: one consumer table,
silver_orders, latest state per order. The mode decides what happens to rows
that reach the source with a timestamp older than the cursor:

    naive              never extracted, nobody notices        -> SILENT_CORRUPTION
    lag                re-extract a 3-day window, all good    -> PASS
    alert_then_repair  fail loudly, load them on the next run -> DETECTED
    alert_forever      fail loudly, never load them           -> BROKEN
    lossy              drops one order even on a clean run    -> steady state broken
"""

import csv
from datetime import date, datetime, timedelta
from pathlib import Path

import duckdb

from faultline.adapters.base import (
    Adapter,
    Capabilities,
    Capability,
    RunRequest,
    RunResult,
    RunStatus,
    TableContract,
)
from faultline.sources.pos import day_files

ORDERS = TableContract(
    "silver_orders", key=("order_id",), measures=("total_vnd",), truth_dataset="pos/orders"
)
LAG = timedelta(days=3)


class FakeCursorPlatform(Adapter):
    def __init__(self, input_root: Path, mode: str) -> None:
        self.input_root = input_root
        self.mode = mode
        self.source: dict[str, dict[str, str]] = {}
        self.warehouse: dict[str, dict[str, str]] = {}
        self.cursor: datetime | None = None
        self.repair_pending = False

    def capabilities(self) -> Capabilities:
        features = frozenset(
            {Capability.DELIVER_DAY, Capability.RUN_PARTITION, Capability.SNAPSHOT}
        )
        return Capabilities(features, (ORDERS,))

    def setup(self) -> None: ...

    def teardown(self) -> None: ...

    def deliver(self, day: date) -> None:
        for path in day_files(self.input_root, "orders", day):
            with path.open(encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    current = self.source.get(row["order_id"])
                    if current is None or _ts(row) >= _ts(current):
                        self.source[row["order_id"]] = row

    def run(self, request: RunRequest) -> RunResult:
        if self.mode == "alert_then_repair" and self.repair_pending:
            self._load(self.source.values())
            self.repair_pending = False
            return RunResult(RunStatus.SUCCEEDED)

        since = self.cursor - LAG if (self.mode == "lag" and self.cursor) else self.cursor
        fresh = [r for r in self.source.values() if since is None or _ts(r) > since]
        late = [
            r
            for r in self.source.values()
            if self.cursor is not None
            and _ts(r) <= self.cursor
            and r["order_id"] not in self.warehouse
        ]
        self._load(fresh)
        if self.mode == "lossy" and self.warehouse:
            self.warehouse.pop(min(self.warehouse))
        if late and self.mode in ("alert_then_repair", "alert_forever"):
            self.repair_pending = True
            return RunResult(RunStatus.FAILED, errors=(f"{len(late)} late rows behind the cursor",))
        return RunResult(RunStatus.SUCCEEDED)

    def snapshot(self, table: str, dest: Path) -> Path:
        # Via a CSV file rather than executemany(): binding Python values row by
        # row makes DuckDB probe for optional modules (pandas, numpy) on every
        # value, which turns a few hundred rows into seconds.
        dest.parent.mkdir(parents=True, exist_ok=True)
        staging = dest.with_suffix(".csv")
        with staging.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(("order_id", "status", "total_vnd"))
            writer.writerows(
                (r["order_id"], r["status"], r["total_vnd"]) for r in self.warehouse.values()
            )
        duckdb.connect().execute(
            f"COPY (SELECT order_id, status, total_vnd::BIGINT AS total_vnd "
            f"FROM read_csv('{staging.as_posix()}', header = true, all_varchar = true)) "
            f"TO '{dest.as_posix()}' (FORMAT parquet)"
        )
        staging.unlink()
        return dest

    def _load(self, rows: object) -> None:
        for row in rows:  # type: ignore[attr-defined]
            self.warehouse[row["order_id"]] = row
            if self.cursor is None or _ts(row) > self.cursor:
                self.cursor = _ts(row)


def _ts(row: dict[str, str]) -> datetime:
    return datetime.fromisoformat(row["updated_at"])
