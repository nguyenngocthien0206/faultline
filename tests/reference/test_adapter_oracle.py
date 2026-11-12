"""The adapter and the oracle together, on the real stack.

Two clean naive runs of the same input, in two workspaces running at the same
time, must fingerprint exactly alike, or the differential oracle would report
false alarms in every experiment (and running sandboxes side by side would not
be safe). Then rerunning one day on the second sandbox shows that the diff
points exactly at the damage, while the job stays green and silver stays clean.
"""

from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunRequest, TableContract
from faultline.adapters.reference import ReferenceAdapter
from faultline.datagen import DatasetSpec
from faultline.oracle.diff import diff
from faultline.oracle.fingerprint import fingerprint
from faultline.oracle.snapshot import snapshot_all
from faultline.runner import execute, generate_truth
from faultline.schedule import clean_schedule

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


@dataclass
class Pair:
    contracts: tuple[TableContract, ...]
    first: dict[str, Path]  # snapshots of a clean run whose sandbox is gone
    second: dict[str, Path]  # snapshots of a clean run whose sandbox is still up
    live: ReferenceAdapter  # that second sandbox, for the rerun test


@pytest.fixture(scope="module")
def pair(stack: Stack, tmp_path_factory: pytest.TempPathFactory) -> Iterator[Pair]:
    """Two clean runs of one input, side by side; the second sandbox stays alive."""
    work = tmp_path_factory.mktemp("pair")
    truth = generate_truth(TINY, work)
    config = reference_config()
    first, live = ReferenceAdapter(config, truth), ReferenceAdapter(config, truth)

    def clean_run(adapter: ReferenceAdapter, name: str) -> dict[str, Path]:
        runs = execute(adapter, clean_schedule(TINY), lambda _: None, name)
        assert all(r.succeeded for _, r in runs), runs
        return snapshot_all(adapter, work / name)

    def run_and_tear_down() -> dict[str, Path]:
        with first:
            return clean_run(first, "first")

    with live:
        with ThreadPoolExecutor(max_workers=2) as pool:
            a = pool.submit(run_and_tear_down)
            b = pool.submit(clean_run, live, "second")
        yield Pair(live.capabilities().tables, a.result(), b.result(), live)


def test_two_clean_runs_side_by_side_fingerprint_identically(pair: Pair) -> None:
    for contract in pair.contracts:
        fa = fingerprint(pair.first[contract.name], contract)
        fb = fingerprint(pair.second[contract.name], contract)
        assert fa == fb, contract.name
        assert fa.rows > 0, contract.name


def test_a_rerun_is_green_silver_stays_clean_and_the_diff_finds_the_damage(
    pair: Pair, tmp_path: Path
) -> None:
    # Changes the second sandbox, so it runs after the fingerprint comparison.
    result = pair.live.run(RunRequest(partition=TINY.end_date))
    assert result.succeeded, result.errors
    after = snapshot_all(pair.live, tmp_path)

    diffs = {t.name: diff(pair.second[t.name], after[t.name], t) for t in pair.contracts}
    for d in diffs.values():
        print(d.summary())

    assert diffs["silver_orders"].is_empty  # deduplicated by key: looks clean
    assert diffs["silver_order_items"].counts["duplicated"] > 0
    assert diffs["silver_payments"].counts["duplicated"] > 0
    sales = diffs["gold_daily_store_sales"]
    assert sales.counts["changed"] > 0
    assert sales.counts["missing"] == sales.counts["extra"] == 0
    net = {m.name: m for m in sales.measures}["net_vnd"]
    assert net.actual > net.expected  # counted twice, never less
