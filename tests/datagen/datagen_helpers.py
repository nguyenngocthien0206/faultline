"""Helpers for reading generated landing files in tests."""

import csv
from pathlib import Path

Rows = list[dict[str, str]]


def read_dataset(root: Path, source: str, dataset: str) -> Rows:
    """Every row of a landing dataset, with partition values added as `_dt` / `_store`."""
    rows: Rows = []
    for path in sorted((root / "landing" / source / dataset).rglob(f"{dataset}.csv")):
        partition = dict(part.split("=", 1) for part in path.parent.parts if "=" in part)
        with path.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                row["_dt"] = partition["dt"]
                if "store" in partition:
                    row["_store"] = partition["store"]
                rows.append(row)
    return rows


def snapshot_files(root: Path) -> dict[str, bytes]:
    return {p.relative_to(root).as_posix(): p.read_bytes() for p in root.rglob("*") if p.is_file()}
