"""Source-side faults (injection point 1): the business systems misbehave.

PosOfflineSync: a store's till loses its connection. Everything it records
while offline reaches the source database only when it reconnects, but with
the timestamps of when it really happened. An extractor that only asks for
rows newer than its cursor has already moved past those timestamps, so the
rows are never extracted. Nothing fails; the store's sales simply vanish.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import ClassVar

from faultline.datagen.writer import partition_dir
from faultline.faults.base import Fault, InjectionPoint

POS_DATASETS = ("orders", "order_items", "payments")


class FaultSetupError(ValueError):
    """The fault cannot be applied to this input (wrong store, empty days, bad dates)."""


@dataclass
class PosOfflineSync(Fault):
    offline_from: date
    sync_on: date
    store: str | None = None  # None: first store, by id, open on every day involved

    name: ClassVar[str] = "pos_offline_sync"
    injection_point: ClassVar[InjectionPoint] = InjectionPoint.SOURCE

    def __post_init__(self) -> None:
        if self.sync_on <= self.offline_from:
            raise FaultSetupError("sync_on must come after offline_from")

    @property
    def offline_days(self) -> list[date]:
        n = (self.sync_on - self.offline_from).days
        return [self.offline_from + timedelta(days=i) for i in range(n)]

    def prepare_input(self, input_root: Path) -> None:
        store = self.store or self._pick_store(input_root)
        self.store = store
        for day in self.offline_days:
            for dataset in POS_DATASETS:
                src = partition_dir(
                    input_root, "pos", dataset, {"dt": day.isoformat(), "store": store}
                )
                if not src.is_dir():
                    raise FaultSetupError(f"store {store} has no {dataset} extract on {day}")
                dst = partition_dir(
                    input_root,
                    "pos",
                    dataset,
                    {"dt": self.sync_on.isoformat(), "store": f"{store}-offline-{day.isoformat()}"},
                )
                dst.parent.mkdir(parents=True, exist_ok=True)
                shutil.move(str(src), str(dst))

    def affected_partitions(self) -> set[date]:
        return {*self.offline_days, self.sync_on}

    def describe(self) -> str:
        store = self.store or "<first eligible store>"
        days = ", ".join(d.isoformat() for d in self.offline_days)
        return f"POS of {store} offline on {days}; its rows reach the source on {self.sync_on}"

    def _pick_store(self, input_root: Path) -> str:
        """First store id (sorted) that has extracts on every offline day and the sync day."""
        candidates: set[str] | None = None
        for day in [*self.offline_days, self.sync_on]:
            folder = partition_dir(input_root, "pos", "orders", {"dt": day.isoformat()})
            stores = {p.name.removeprefix("store=") for p in folder.glob("store=*")}
            candidates = stores if candidates is None else candidates & stores
        if not candidates:
            raise FaultSetupError(
                f"no store is open on every day from {self.offline_from} to {self.sync_on}"
            )
        return sorted(candidates)[0]
