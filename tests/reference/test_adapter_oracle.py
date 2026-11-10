"""The adapter and the oracle together, on the real stack.

Two clean baselines in two separate workspaces must fingerprint identically,
or the differential oracle would report false alarms in every experiment.
Then rerunning one day shows that the diff points exactly at the damage.
"""

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunRequest
from faultline.adapters.reference import ReferenceAdapter
from faultline.datagen import DatasetSpec, generate
from faultline.oracle.diff import diff
from faultline.oracle.fingerprint import fingerprint
from faultline.oracle.invariants import complete_keys, unique_keys
from faultline.oracle.snapshot import snapshot_all

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


@dataclass
class Baseline:
    adapter: ReferenceAdapter
    snapshots: dict[str, Path]


def run_baseline(adapter: ReferenceAdapter, dest: Path) -> Baseline:
    for day in TINY.dates():
        adapter.deliver(day)
        result = adapter.run(RunRequest(partition=day))
        assert result.succeeded, result.errors
    return Baseline(adapter, snapshot_all(adapter, dest))


@pytest.fixture(scope="module")
def generated(tmp_path_factory: pytest.TempPathFactory) -> Path:
    root = tmp_path_factory.mktemp("tiny")
    generate(TINY, root, workers=1)
    return root


@pytest.fixture(scope="module")
def baselines(
    stack: Stack, generated: Path, tmp_path_factory: pytest.TempPathFactory
) -> Iterator[tuple[Baseline, Baseline]]:
    first = ReferenceAdapter(reference_config(), generated)
    second = ReferenceAdapter(reference_config(), generated)
    try:
        first.setup()
        second.setup()
        yield (
            run_baseline(first, tmp_path_factory.mktemp("baseline_a")),
            run_baseline(second, tmp_path_factory.mktemp("baseline_b")),
        )
    finally:
        first.teardown()
        second.teardown()


def test_two_clean_baselines_fingerprint_identically(
    baselines: tuple[Baseline, Baseline],
) -> None:
    a, b = baselines
    contracts = a.adapter.capabilities().tables

    assert a.adapter.workspace != b.adapter.workspace
    for contract in contracts:
        fa = fingerprint(a.snapshots[contract.name], contract)
        fb = fingerprint(b.snapshots[contract.name], contract)
        assert fa == fb, contract.name
        assert fa.rows > 0, contract.name


def test_the_baseline_satisfies_the_key_invariants(
    baselines: tuple[Baseline, Baseline], generated: Path
) -> None:
    a, _ = baselines

    for contract in a.adapter.capabilities().tables:
        assert unique_keys(a.snapshots[contract.name], contract).holds, contract.name
        if contract.truth_dataset:
            result = complete_keys(a.snapshots[contract.name], contract, generated)
            assert result.holds, result.summary()


def test_a_rerun_shows_up_exactly_where_the_damage_is(
    baselines: tuple[Baseline, Baseline], tmp_path: Path
) -> None:
    # Runs last: it changes baseline A's workspace.
    a, _ = baselines
    result = a.adapter.run(RunRequest(partition=TINY.end_date))
    assert result.succeeded, result.errors
    after = snapshot_all(a.adapter, tmp_path)
    caps = a.adapter.capabilities()

    diffs = {t.name: diff(a.snapshots[t.name], after[t.name], t) for t in caps.tables}
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
