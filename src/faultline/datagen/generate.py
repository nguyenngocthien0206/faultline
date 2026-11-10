"""Generate a full dataset: backoffice extracts, POS extracts per store, truth.

Backoffice dimensions are built once in the main process. POS stores are
simulated in parallel worker processes, one store per task. Because every
store-day has its own seeded random stream, the output is byte-identical for
any number of workers.
"""

from __future__ import annotations

import multiprocessing
import os
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path

from faultline import __version__
from faultline.datagen.dimensions import (
    MEMBER_COLUMNS,
    MENU_COLUMNS,
    PROMOTION_COLUMNS,
    STORE_COLUMNS,
    Dimensions,
    MemberIndex,
    Menu,
    Promotion,
    Store,
    build_dimensions,
    build_menu,
    build_promotions,
    build_stores,
    member_changes_by_day,
    member_delta_rows,
    member_full_rows,
    menu_rows,
    promotion_rows,
    store_rows,
)
from faultline.datagen.pos import (
    ORDER_COLUMNS,
    ORDER_ITEM_COLUMNS,
    PAYMENT_COLUMNS,
    StoreSimulator,
)
from faultline.datagen.spec import DatasetSpec
from faultline.datagen.truth import write_control_totals
from faultline.datagen.writer import FileStat, write_json, write_landing_file

GENERATION_FILE = "generation.json"


class OutputNotEmptyError(Exception):
    """Raised when the output directory already contains files."""


@dataclass(frozen=True)
class GenerationReport:
    out_dir: Path
    spec: DatasetSpec
    row_counts: dict[str, int]  # "source/dataset" -> rows
    files: int
    elapsed_seconds: float

    @property
    def orders(self) -> int:
        return self.row_counts.get("pos/orders", 0)


def generate(spec: DatasetSpec, out_dir: Path, *, workers: int | None = None) -> GenerationReport:
    """Write the dataset described by `spec` into `out_dir`, which must be empty or absent."""
    started = time.perf_counter()
    out_dir = Path(out_dir)
    if out_dir.exists() and any(out_dir.iterdir()):
        raise OutputNotEmptyError(f"output directory is not empty: {out_dir}")
    out_dir.mkdir(parents=True, exist_ok=True)

    dims = build_dimensions(spec)
    stats = _write_backoffice(spec, dims, out_dir)

    context = _PosContext.from_dimensions(spec, dims)
    n_workers = max(1, min(workers or os.cpu_count() or 1, len(dims.stores)))
    results = _run_stores(context, out_dir, n_workers)

    truth_rows = []
    for store_stats, store_truth in results:
        stats.extend(store_stats)
        truth_rows.extend(store_truth)
    write_control_totals(out_dir, truth_rows)

    counts = Counter()
    for s in stats:
        counts[f"{s.source}/{s.dataset}"] += s.row_count
    row_counts = dict(sorted(counts.items()))
    write_json(
        out_dir / GENERATION_FILE,
        {
            "generator": "coffee_vn",
            "faultline_version": __version__,
            "spec": spec.to_dict(),
            "files": len(stats),
            "row_counts": row_counts,
        },
    )
    return GenerationReport(
        out_dir=out_dir,
        spec=spec,
        row_counts=row_counts,
        files=len(stats),
        elapsed_seconds=time.perf_counter() - started,
    )


# ----------------------------------------------------------------------- backoffice


def _write_backoffice(spec: DatasetSpec, dims: Dimensions, root: Path) -> list[FileStat]:
    stats = []
    changes = member_changes_by_day(dims)
    for d in spec.dates():
        part = {"dt": d.isoformat()}
        first_day = d == spec.start_date
        members = member_full_rows(dims, d) if first_day else member_delta_rows(changes, d)
        for dataset, columns, rows in (
            ("stores", STORE_COLUMNS, store_rows(dims, d)),
            ("menu_items", MENU_COLUMNS, menu_rows(dims, d)),
            ("promotions", PROMOTION_COLUMNS, promotion_rows(dims, d)),
            ("members", MEMBER_COLUMNS, members),
        ):
            stats.append(
                write_landing_file(
                    root,
                    source="backoffice",
                    dataset=dataset,
                    partition=part,
                    columns=columns,
                    rows=rows,
                )
            )
    return stats


# ------------------------------------------------------------------------------ pos


@dataclass(frozen=True)
class _PosContext:
    """What a POS simulation needs from the dimensions (no member names)."""

    spec: DatasetSpec
    stores: tuple[Store, ...]
    menu: Menu
    promotions: tuple[Promotion, ...]
    members: MemberIndex

    @classmethod
    def from_dimensions(cls, spec: DatasetSpec, dims: Dimensions) -> _PosContext:
        members = MemberIndex.from_members(dims.members)
        return cls(spec, dims.stores, dims.menu, dims.promotions, members)

    @classmethod
    def rebuild(cls, spec: DatasetSpec) -> _PosContext:
        """Rebuild from the seed alone. Deterministic, so identical to the main process."""
        return cls(
            spec,
            build_stores(spec),
            build_menu(spec),
            build_promotions(spec),
            MemberIndex.for_spec(spec),
        )


StoreResult = tuple[list[FileStat], list[tuple[object, ...]]]

# Set once per worker by the pool initializer. Only the small spec crosses the
# process boundary: on Windows the pool starts workers one after another and
# each must finish reading its init arguments first, so shipping megabytes of
# dimensions made startup dominate the run. Each worker rebuilds them instead.
_WORKER: tuple[_PosContext, Path] | None = None


def _init_worker(spec: DatasetSpec, root: Path) -> None:
    global _WORKER
    _WORKER = (_PosContext.rebuild(spec), root)


def _simulate_store_in_worker(store_index: int) -> StoreResult:
    assert _WORKER is not None, "worker not initialised"
    context, root = _WORKER
    return _simulate_store(context, root, store_index)


def _simulate_store(context: _PosContext, root: Path, store_index: int) -> StoreResult:
    store = context.stores[store_index]
    sim = StoreSimulator(context.spec, store, context.menu, context.promotions, context.members)
    stats = []
    for day in sim.days():
        part = {"dt": day.dt.isoformat(), "store": store.store_id}
        for dataset, columns, rows in (
            ("orders", ORDER_COLUMNS, day.orders),
            ("order_items", ORDER_ITEM_COLUMNS, day.order_items),
            ("payments", PAYMENT_COLUMNS, day.payments),
        ):
            stats.append(
                write_landing_file(
                    root, source="pos", dataset=dataset, partition=part, columns=columns, rows=rows
                )
            )
    return stats, sim.truth_rows()


def _run_stores(context: _PosContext, root: Path, workers: int) -> list[StoreResult]:
    indices = range(len(context.stores))
    if workers == 1:
        return [_simulate_store(context, root, i) for i in indices]
    # "spawn" everywhere, so Linux behaves like Windows and macOS.
    ctx = multiprocessing.get_context("spawn")
    with ctx.Pool(workers, initializer=_init_worker, initargs=(context.spec, root)) as pool:
        return list(pool.imap(_simulate_store_in_worker, indices))


# Measured on the demo profile: all landing files together take about 320
# bytes per order (orders, lines, payments, manifests, backoffice extracts).
BYTES_PER_ORDER = 330


def estimate_orders(spec: DatasetSpec) -> int:
    """Rough order count, for warning the user before a large run."""
    return spec.stores * spec.days * spec.orders_per_store_day


def estimate_bytes(spec: DatasetSpec) -> int:
    return estimate_orders(spec) * BYTES_PER_ORDER
