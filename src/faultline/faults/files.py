"""Editing landing files the way a fault needs: well formed, just wrong.

A file cut mid-row usually fails to parse, and somebody notices. The cut that
matters is the one on a row boundary: the file reads fine, with fewer rows.
Every other property of the file stays as its writer made it (for Parquet:
the schema, nullability and logical types, which is why this uses pyarrow,
the library dlt itself writes with), so the fault changes exactly one thing.
"""

from __future__ import annotations

import io
from typing import Any

from faultline.faults.base import FaultSetupError


def _pyarrow_parquet() -> Any:
    try:
        import pyarrow.parquet as pq
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "cutting Parquet files needs pyarrow: pip install 'faultline[reference]'"
        ) from exc
    return pq


def rows_to_keep(total: int, keep: float) -> int:
    """How many of `total` rows a cut keeps: always at least one row fewer."""
    if not 0 < keep < 1:
        raise FaultSetupError(f"keep must be between 0 and 1, got {keep}")
    if total == 0:
        raise FaultSetupError("cannot cut an empty file")
    return min(int(total * keep), total - 1)


def truncate(name: str, data: bytes, keep: float) -> bytes:
    """`data` with only its first `keep` share of rows, in the same format and schema."""
    if name.endswith(".parquet"):
        pq = _pyarrow_parquet()
        table = pq.read_table(io.BytesIO(data))
        out = io.BytesIO()
        pq.write_table(table.slice(0, rows_to_keep(table.num_rows, keep)), out)
        return out.getvalue()
    if name.endswith(".csv"):
        header, *rows = data.decode("utf-8").splitlines(keepends=True)
        return (header + "".join(rows[: rows_to_keep(len(rows), keep)])).encode("utf-8")
    raise FaultSetupError(f"cannot cut {name}: only Parquet and CSV files")
