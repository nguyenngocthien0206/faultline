import io

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from faultline.faults.base import FaultSetupError
from faultline.faults.files import rows_to_keep, truncate

# Like a dlt file: every column REQUIRED, a timezone-aware timestamp.
SCHEMA = pa.schema(
    [
        pa.field("order_id", pa.string(), nullable=False),
        pa.field("qty", pa.int64(), nullable=False),
        pa.field("created_at", pa.timestamp("us", tz="UTC"), nullable=False),
    ]
)


def _parquet(rows: int) -> bytes:
    table = pa.table(
        {
            "order_id": [f"o{i}" for i in range(rows)],
            "qty": list(range(rows)),
            "created_at": pa.array([1_795_000_000_000_000 + i for i in range(rows)], pa.int64()),
        }
    ).cast(SCHEMA)
    out = io.BytesIO()
    pq.write_table(table, out)
    return out.getvalue()


def test_a_cut_parquet_file_keeps_its_schema_and_its_first_rows() -> None:
    cut = pq.read_table(io.BytesIO(truncate("a.parquet", _parquet(10), keep=0.8)))

    assert cut.schema.equals(SCHEMA)  # nullability and logical types as the writer left them
    assert cut.num_rows == 8
    assert cut.column("order_id").to_pylist() == [f"o{i}" for i in range(8)]


def test_a_cut_csv_file_keeps_its_header_and_whole_lines() -> None:
    data = b"order_id,qty\no1,1\no2,2\no3,3\no4,4\no5,5\n"

    assert truncate("a.csv", data, keep=0.6) == b"order_id,qty\no1,1\no2,2\no3,3\n"


@pytest.mark.parametrize(("total", "keep", "kept"), [(10, 0.8, 8), (3, 0.9, 2), (1, 0.5, 0)])
def test_a_cut_always_drops_at_least_one_row(total: int, keep: float, kept: int) -> None:
    assert rows_to_keep(total, keep) == kept


@pytest.mark.parametrize("keep", [0, 1, 1.5, -0.1])
def test_keep_is_a_share_strictly_between_none_and_all(keep: float) -> None:
    with pytest.raises(FaultSetupError, match="between 0 and 1"):
        truncate("a.parquet", _parquet(5), keep)


def test_an_empty_file_cannot_be_cut() -> None:
    with pytest.raises(FaultSetupError, match="empty file"):
        truncate("a.parquet", _parquet(0), keep=0.8)


def test_only_parquet_and_csv_can_be_cut() -> None:
    with pytest.raises(FaultSetupError, match="only Parquet and CSV"):
        truncate("a.json", b"{}", keep=0.8)
