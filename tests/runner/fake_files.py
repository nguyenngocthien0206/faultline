"""A toy file-based platform, to exercise landing faults without Docker.

It has the reference stack's shape in miniature. Delivering a day adds that
day's POS rows (orders, order lines) to a source. A run has two stages:

    ingest  extract the source past a cursor into one CSV file per dataset,
            under the run's partition of an in-memory landing zone
    load    load every landing file of that partition (and only that one)

A plain run does both. The mode plays the two reference editions:

    append  no look back at the source, every file loaded as it comes, order
            lines appended                                    -> naive
    keyed   the extract re-reads the last three days, and one row per key
            survives, latest version wins                     -> hardened

Orders are kept as latest state per order in both modes (as both editions
do), so damage shows in the order lines, where a uniqueness check on orders
alone would never notice it.
"""

import csv
import io
from datetime import date, timedelta
from pathlib import Path

import duckdb

from faultline.adapters.base import (
    Adapter,
    Capabilities,
    Capability,
    RunRequest,
    RunResult,
    RunStatus,
    Stage,
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
LAG = timedelta(days=3)


class FakeFilePlatform(Adapter):
    def __init__(self, input_root: Path, mode: str) -> None:
        self.input_root = input_root
        self.mode = mode
        self.source: dict[str, dict[date, list[dict[str, str]]]] = {d: {} for d in DATASETS}
        self.cursor: date | None = None  # last delivered day already extracted
        self.files: dict[tuple[str, date, str], bytes] = {}
        self.extracts = 0
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
                Capability.RUN_STAGES,
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
            self.source[dataset][day] = rows

    def run(self, request: RunRequest) -> RunResult:
        if request.stage in (None, Stage.INGEST):
            self._ingest(request.partition)
        if request.stage in (None, Stage.LOAD):
            self._load(request.partition)
        return RunResult(RunStatus.SUCCEEDED)

    def _ingest(self, partition: date) -> None:
        since = self.cursor
        if since is not None and self.mode == "keyed":
            since -= LAG
        for dataset, by_day in self.source.items():
            rows = [
                r for day, rs in sorted(by_day.items()) if since is None or day > since for r in rs
            ]
            if rows:  # like dlt: nothing new, no file
                self.extracts += 1
                out = io.StringIO()
                writer = csv.DictWriter(out, fieldnames=list(rows[0]))
                writer.writeheader()
                writer.writerows(rows)
                name = f"extract-{self.extracts}.csv"
                self.files[dataset, partition, name] = out.getvalue().encode("utf-8")
        delivered = [day for by_day in self.source.values() for day in by_day]
        if delivered:
            self.cursor = max(delivered)

    def _load(self, partition: date) -> None:
        for dataset in DATASETS:
            for file in self.landing_files(dataset, partition):
                text = self.files[dataset, partition, file].decode("utf-8")
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

    def delete_landing_file(self, dataset: str, partition: date, file: str) -> None:
        del self.files[dataset, partition, file]
