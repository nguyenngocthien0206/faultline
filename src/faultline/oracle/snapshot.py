"""Snapshots: Parquet copies of every consumer-visible table at one moment."""

from __future__ import annotations

from pathlib import Path

from faultline.adapters.base import Adapter


def snapshot_all(adapter: Adapter, dest_dir: Path) -> dict[str, Path]:
    """Export every table the adapter declares into `dest_dir/<table>.parquet`."""
    return {
        contract.name: adapter.snapshot(contract.name, Path(dest_dir) / f"{contract.name}.parquet")
        for contract in adapter.capabilities().tables
    }
