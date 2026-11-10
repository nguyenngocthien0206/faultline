"""Truth: what the generator knows the correct answer to be.

Truth lives next to `landing/`, never inside it. The platform under test only
ever sees landing files; faultline keeps the answer key to itself.

Control totals are per business date (the day an order was placed), after
every refund inside the generated window has been applied. That is the figure
a correct gold table must show, regardless of which day's extract carried the
refund.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable, Sequence
from pathlib import Path

from faultline.datagen.pos import TRUTH_COLUMNS
from faultline.datagen.writer import render_csv

TRUTH_DIR = "truth"
CONTROL_TOTALS_FILE = "control_totals.csv"


def write_control_totals(root: Path, rows: Iterable[Sequence[object]]) -> Path:
    path = root / TRUTH_DIR / CONTROL_TOTALS_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    ordered = sorted(rows, key=lambda r: (str(r[0]), str(r[1])))  # order_date, store_id
    content, _ = render_csv(TRUTH_COLUMNS, ordered)
    path.write_bytes(content)
    return path


def read_control_totals(root: Path) -> list[dict[str, str]]:
    with (root / TRUTH_DIR / CONTROL_TOTALS_FILE).open(encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))
