"""The failpoint macros copy dbt-clickhouse's strategies; they must track its version."""

import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_the_copied_strategy_macros_match_the_pinned_dbt_clickhouse() -> None:
    lock = (ROOT / "reference/dagster/requirements.lock").read_text(encoding="utf-8")
    macros = (ROOT / "reference/dbt/macros/failpoint_mid_write.sql").read_text(encoding="utf-8")

    pinned = re.search(r"^dbt-clickhouse==(\S+)$", lock, re.MULTILINE)
    copied = re.search(r"copies of dbt-clickhouse (\d+\.\d+\.\d+)", macros)
    assert pinned
    assert copied
    assert copied.group(1) == pinned.group(1), (
        "dbt-clickhouse moved: copy its delete_insert and insert_overwrite macros again "
        "and put the failpoints back"
    )
