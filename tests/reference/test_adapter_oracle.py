"""The adapter and the oracle together, on the real stack.

A second clean naive baseline, in its own workspace, must fingerprint exactly like
the shared one, or the differential oracle would report false alarms in every
experiment. Then rerunning one day on that second sandbox shows that the diff
points exactly at the damage, while the job stays green and silver stays clean.
(The shared baseline's key invariants are asserted by its fixture.)
"""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from reference_stack import reference_config

from faultline.adapters.base import RunRequest
from faultline.adapters.reference import ReferenceAdapter
from faultline.oracle.diff import diff
from faultline.oracle.fingerprint import fingerprint
from faultline.oracle.snapshot import snapshot_all
from faultline.runner import Baseline, execute
from faultline.schedule import clean_schedule

pytestmark = pytest.mark.integration


@dataclass
class LiveBaseline:
    adapter: ReferenceAdapter
    snapshots: dict[str, Path]


@pytest.fixture(scope="module")
def second(
    naive_baseline: Baseline, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[LiveBaseline]:
    """Another clean run of the same input, kept alive for the rerun test."""
    adapter = ReferenceAdapter(reference_config(), naive_baseline.truth_root)
    with adapter:
        runs = execute(adapter, clean_schedule(naive_baseline.spec), lambda _: None, "second")
        assert all(r.succeeded for _, r in runs), runs
        yield LiveBaseline(adapter, snapshot_all(adapter, tmp_path_factory.mktemp("second")))


def test_two_clean_baselines_fingerprint_identically(
    naive_baseline: Baseline, second: LiveBaseline
) -> None:
    for contract in naive_baseline.contracts:
        fa = fingerprint(naive_baseline.snapshots[contract.name], contract)
        fb = fingerprint(second.snapshots[contract.name], contract)
        assert fa == fb, contract.name
        assert fa.rows > 0, contract.name


def test_a_rerun_is_green_silver_stays_clean_and_the_diff_finds_the_damage(
    naive_baseline: Baseline, second: LiveBaseline, tmp_path: Path
) -> None:
    # Changes the second sandbox, so it runs after the fingerprint comparison.
    result = second.adapter.run(RunRequest(partition=naive_baseline.spec.end_date))
    assert result.succeeded, result.errors
    after = snapshot_all(second.adapter, tmp_path)
    tables = second.adapter.capabilities().tables

    diffs = {t.name: diff(second.snapshots[t.name], after[t.name], t) for t in tables}
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
