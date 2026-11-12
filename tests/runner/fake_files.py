"""A toy file-based platform, to exercise landing faults without Docker.

Delivering a day writes that day's POS extract (orders, order lines) into an
in-memory landing zone, one CSV file per dataset under the day's partition.
Running a partition loads every landing file of that partition. The mode
decides what the loader makes of rows it has seen before:

    append   every file is loaded as it comes, lines appended  -> duplicates
    keyed    one row per key, latest version wins               -> idempotent

Orders are kept as latest state per order in both modes (as both reference
editions do), so a duplicated file shows up in the order lines, just as a
uniqueness check on orders alone would never notice it.
"""

import csv
import io
from datetime import date
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
LINES = TableContract(
    "silver_order_items",
    key=("order_id", "line_no"),
    measures=("qty",),
    truth_dataset="pos/order_items",
)
DATASETS = {"pos/orders": "orders", "pos/order_items": "order_items"}


class FakeFilePlatform(Adapter):
    def __init__(self, input_root: Path, mode: str) -> None:
        self.input_root = input_root
        self.mode = mode
        self.files: dict[tuple[str, date, str], bytes] = {}
        self.orders: dict[str, dict[str, str]] = {}
        self.lines: list[dict[str, str]] = []

    def capabilities(self) -> Capabilities:
        features = frozenset(
            {
                Capability.DELIVER_DAY,
                Capability.RUN_PARTITION,
                Capability.SNAPSHOT,
                Capability.CONCURRENT_SANDBOXES,
                Capability.LANDING_FILES,
            }
        )
        return Capabilities(features, (ORDERS, LINES))

    def setup(self) -> None: ...

    def teardown(self) -> None: ...

    def deliver(self, day: date) -> None:
        for dataset, table in DATASETS.items():
            rows = []
            for path in day_files(self.input_root, table, day):
                with path.open(encoding="utf-8", newline="") as f:
                    rows += list(csv.DictReader(f))
            if rows:
                out = io.StringIO()
                writer = csv.DictWriter(out, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
                self.files[dataset, day, f"extract-{day}.csv"] = out.getvalue().encode("utf-8")

    def run(self, request: RunRequest) -> RunResult:
        for dataset in DATASETS:
            for file in self.landing_files(dataset, request.partition):
                text = self.files[dataset, request.partition, file].decode("utf-8")
                rows = list(csv.DictReader(io.StringIO(text)))
                if dataset == "pos/orders":
                    for row in rows:
                        current = self.orders.get(row["order_id"])
                        if current is None or row["updated_at"] >= current["updated_at"]:
                            self.orders[row["order_id"]] = row
                else:
                    self.lines += rows
        if self.mode == "keyed":
            latest = {(r["order_id"], r["line_no"]): r for r in self.lines}
            self.lines = list(latest.values())
        return RunResult(RunStatus.SUCCEEDED)

    def snapshot(self, table: str, dest: Path) -> Path:
        if table == "silver_orders":
            header = ("order_id", "total_vnd")
            rows = [(r["order_id"], r["total_vnd"]) for r in self.orders.values()]
            select = "order_id, total_vnd::BIGINT AS total_vnd"
        else:
            header = ("order_id", "line_no", "qty")
            rows = [(r["order_id"], r["line_no"], r["qty"]) for r in self.lines]
            select = "order_id, line_no::INTEGER AS line_no, qty::BIGINT AS qty"
        dest.parent.mkdir(parents=True, exist_ok=True)
        staging = dest.with_suffix(".csv")  # see fake_platform.py: no executemany
        with staging.open("w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(header)
            writer.writerows(rows)
        duckdb.connect().execute(
            f"COPY (SELECT {select} FROM read_csv('{staging.as_posix()}', header = true, "
            f"all_varchar = true)) TO '{dest.as_posix()}' (FORMAT parquet)"
        )
        staging.unlink()
        return dest

    def landing_files(self, dataset: str, partition: date) -> list[str]:
        return sorted(f for d, p, f in self.files if (d, p) == (dataset, partition))

    def read_landing_file(self, dataset: str, partition: date, file: str) -> bytes:
        return self.files[dataset, partition, file]

    def write_landing_file(self, dataset: str, partition: date, file: str, data: bytes) -> None:
        self.files[dataset, partition, file] = data
