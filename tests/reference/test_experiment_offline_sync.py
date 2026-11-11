"""The first real experiment, on both editions: a POS goes offline and syncs late.

Naive: dlt's incremental cursor has moved past the offline days by the time
the rows arrive, so they are never extracted. Every run is green, recovery
reruns do not help, and the verdict must be SILENT_CORRUPTION.

Hardened: the lag window reads the late rows on the sync day, keyed silver and
final-state gold absorb the copies, and idempotent bronze makes the recovery
reruns harmless. Both checkpoints must equal the clean baseline: PASS.
"""

from pathlib import Path

import pytest
from reference_stack import reference_config

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition, ReferenceAdapter, target_name
from faultline.experiments import pos_offline_sync
from faultline.outcome import Outcome
from faultline.runner import Baseline, ExperimentResult, run_experiment

pytestmark = pytest.mark.integration


def _run(edition: Edition, baseline: Baseline, work_dir: Path) -> ExperimentResult:
    config = reference_config()
    result = run_experiment(
        pos_offline_sync(baseline.spec),
        lambda root: ReferenceAdapter(config, root, edition=edition),
        work_dir,
        target=target_name(edition),
        baseline=baseline,
    )
    print(result.render())
    return result


def _all_green(result: ExperimentResult) -> bool:
    return all(r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs)


def test_naive_loses_the_offline_store_without_a_word(
    naive_baseline: Baseline, tmp_path: Path
) -> None:
    result = _run(Edition.NAIVE, naive_baseline, tmp_path)

    assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
    loss = next(p for p in result.properties if p.name == "no_data_loss")
    assert not loss.at_f.holds
    assert not loss.at_r.holds  # rerunning the days does not bring the rows back
    assert any(e.startswith("silver_orders: complete_keys violated") for e in loss.at_f.evidence)
    assert _all_green(result)


@pytest.mark.hardened
def test_hardened_counts_the_offline_store_once_it_syncs(
    hardened_baseline: Baseline, tmp_path: Path
) -> None:
    result = _run(Edition.HARDENED, hardened_baseline, tmp_path)

    assert result.outcome is Outcome.PASS, result.render()
    for prop in result.properties:
        assert prop.at_f.holds, prop.at_f.evidence  # right as soon as the till synced
        assert prop.at_r.holds, prop.at_r.evidence  # and the reruns changed nothing
    assert _all_green(result)
