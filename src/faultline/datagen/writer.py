"""Landing files: byte-deterministic CSV plus a manifest per file.

The manifest is the delivery contract between a source and the platform. It
says how many rows the file must contain and what its checksum is, so a
platform can tell a complete file from one that was cut short but still
parses. Bytes are deterministic (UTF-8 without BOM, `\\n` line endings, fixed
column order), otherwise the checksum would change on every run.
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

LANDING_DIR = "landing"
SCHEMA_VERSION = 1


@dataclass(frozen=True)
class FileStat:
    source: str
    dataset: str
    path: str  # posix path relative to the output root
    row_count: int
    sha256: str


def _cell(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime | date):
        return value.isoformat()
    return str(value)


def render_csv(columns: Sequence[str], rows: Iterable[Sequence[object]]) -> tuple[bytes, int]:
    """CSV bytes with a header line, and the number of data rows."""
    buffer = io.StringIO()
    writer = csv.writer(buffer, lineterminator="\n")
    writer.writerow(columns)
    count = 0
    for row in rows:
        writer.writerow([_cell(v) for v in row])
        count += 1
    return buffer.getvalue().encode("utf-8"), count


def write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, indent=2, ensure_ascii=False) + "\n"
    path.write_bytes(text.encode("utf-8"))


def partition_dir(root: Path, source: str, dataset: str, partition: dict[str, str]) -> Path:
    """Hive-style layout: landing/<source>/<dataset>/dt=YYYY-MM-DD[/store=...]."""
    path = root / LANDING_DIR / source / dataset
    for key, value in partition.items():
        path = path / f"{key}={value}"
    return path


def write_landing_file(
    root: Path,
    *,
    source: str,
    dataset: str,
    partition: dict[str, str],
    columns: Sequence[str],
    rows: Iterable[Sequence[object]],
) -> FileStat:
    """Write one CSV file and its manifest. Empty extracts still get a header-only file."""
    directory = partition_dir(root, source, dataset, partition)
    directory.mkdir(parents=True, exist_ok=True)
    content, row_count = render_csv(columns, rows)
    digest = hashlib.sha256(content).hexdigest()
    file_name = f"{dataset}.csv"
    (directory / file_name).write_bytes(content)
    write_json(
        directory / f"{dataset}.manifest.json",
        {
            "source": source,
            "dataset": dataset,
            "partition": partition,
            "file": file_name,
            "row_count": row_count,
            "sha256": digest,
            "schema_version": SCHEMA_VERSION,
            "columns": list(columns),
        },
    )
    return FileStat(
        source=source,
        dataset=dataset,
        path=(directory / file_name).relative_to(root).as_posix(),
        row_count=row_count,
        sha256=digest,
    )
