"""The first real experiment, on both editions: a POS goes offline and syncs late.

Naive: dlt's incremental cursor has moved past the offline days by the time
the rows arrive, so they are never extracted. Every run is green, recovery
reruns do not help, and the verdict must be SILENT_CORRUPTION.

Hardened: the lag window reads the late rows on the sync day, keyed silver and
final-state gold absorb the copies, and idempotent bronze makes the recovery
reruns harmless. Both checkpoints must equal the clean baseline: PASS.

Each experiment runs whole, as `faultline run` does: its own baseline, side by
side with the fault phase. Each edition is its own class, the unit pytest-xdist
schedules, so the two experiments run in parallel too.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, experiment, keys_off, reference_target
from shared_baseline import Baselines

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path, baselines: Baselines | None = None) -> ExperimentResult:
    result = run_experiment(
        experiment("pos-offline-sync"),
        reference_target(edition),
        work_dir,
        baseline=baselines(edition) if baselines else None,
    )
    print(result.render())
    return result


def _all_green(result: ExperimentResult) -> bool:
    return all(r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs)


class TestNaive:
    def test_loses_the_offline_store_without_a_word(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path, baselines)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert keys_off(result)[-1] > 0  # three rounds of reruns never bring it back
        assert len(keys_off(result)) == 3
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        assert not loss.at_f.holds
        assert not loss.at_r.holds  # rerunning the days does not bring the rows back
        assert any(
            e.startswith("silver_orders: complete_keys violated") for e in loss.at_f.evidence
        )
        assert _all_green(result)


@pytest.mark.hardened
class TestHardened:
    def test_counts_the_offline_store_once_it_syncs(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path, baselines)

        assert result.outcome is Outcome.PASS, result.render()
        assert keys_off(result) == [0]  # healed by the first round of recovery
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence  # right as soon as the till synced
            assert prop.at_r.holds, prop.at_r.evidence  # and the reruns changed nothing
        assert _all_green(result)
