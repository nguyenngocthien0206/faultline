"""Convergence: recovery reruns the affected days, round after round, until the outputs match.

The same late-sync fault on the toy platform: a platform that heals with one
round stops there, one that heals later is given the rounds it needs (up to
`recovery_rounds`), one that never heals uses them all and says how far off
it stayed.
"""

import dataclasses
import threading
from datetime import date
from pathlib import Path

from fake_platform import FakeCursorPlatform, cursor_target

from faultline.adapters.base import RunRequest, RunResult
from faultline.datagen import DatasetSpec
from faultline.faults.source import PosOfflineSync
from faultline.outcome import Outcome
from faultline.properties.core import Convergence, NoDataLoss
from faultline.runner import (
    Experiment,
    ExperimentResult,
    PropertyVerdict,
    run_baseline,
    run_experiment,
)
from faultline.safety import Target

TINY = DatasetSpec.from_profile("tiny")
AFFECTED = 3  # the outage's days, 2 to 4: one round of recovery is three reruns

EXPERIMENT = Experiment(
    name="pos-offline-sync",
    description="A till goes offline for two days and syncs late.",
    spec=TINY,
    faults=(PosOfflineSync(offline_from=date(2026, 11, 2), sync_on=date(2026, 11, 4)),),
    properties=(NoDataLoss(), Convergence()),
)


def _run(mode: str, work_dir: Path, *, rounds: int = 3, **options: object) -> ExperimentResult:
    experiment = dataclasses.replace(EXPERIMENT, recovery_rounds=rounds)
    result = run_experiment(experiment, cursor_target(mode, **options), work_dir)
    print(result.render())
    return result


def _convergence(result: ExperimentResult) -> PropertyVerdict:
    return next(p for p in result.properties if p.name == "convergence")


def test_a_platform_that_heals_at_once_stops_after_one_round(tmp_path: Path) -> None:
    result = _run("lag", tmp_path)

    assert result.recovery is not None
    assert result.recovery.converged
    assert len(result.recovery.rounds) == 1
    assert len(result.runs["recovery"]) == AFFECTED  # no round more than needed
    assert _convergence(result).outcome is Outcome.PASS
    assert "Recovery:   converged after 1 round (3 runs)" in result.render()


def test_a_platform_that_heals_later_gets_the_rounds_it_needs(tmp_path: Path) -> None:
    # The sandbox's eighth run is a full refresh: four in the fault phase, three
    # in the first round, so the second round is the one that heals.
    result = _run("reconcile", tmp_path, reconcile_every=8)

    assert result.recovery is not None
    assert [r.keys_off > 0 for r in result.recovery.rounds] == [True, False]
    assert _convergence(result).outcome is Outcome.PASS
    assert result.outcome is Outcome.SILENT_CORRUPTION  # rows were missing at F, silently
    assert "converged after 2 rounds (6 runs)" in result.render()


def test_the_same_platform_with_fewer_rounds_does_not_converge(tmp_path: Path) -> None:
    result = _run("reconcile", tmp_path, rounds=1, reconcile_every=8)

    convergence = _convergence(result)
    assert convergence.outcome is Outcome.SILENT_CORRUPTION
    assert convergence.at_f.holds  # judged at R only
    assert convergence.at_r.evidence[0].startswith("not converged within 1 round (3 runs): ")
    assert any(e.startswith("silver_orders: ") for e in convergence.at_r.evidence[1:])


def test_a_platform_that_never_heals_uses_every_round(tmp_path: Path) -> None:
    result = _run("naive", tmp_path)

    assert result.recovery is not None
    off = [r.keys_off for r in result.recovery.rounds]
    rows = [r.rows_off for r in result.recovery.rounds]
    assert len(off) == 3
    assert all(off)
    assert len(result.runs["recovery"]) == 3 * AFFECTED
    assert _convergence(result).outcome is Outcome.SILENT_CORRUPTION
    assert _convergence(result).at_r.evidence[0] == (
        f"not converged within 3 rounds (9 runs): {off[0]:,}, {off[1]:,}, {off[2]:,} "
        f"keys and {rows[0]:,}, {rows[1]:,}, {rows[2]:,} rows off the baseline"
    )


def test_not_converging_after_a_signal_is_broken(tmp_path: Path) -> None:
    result = _run("alert_forever", tmp_path)

    assert _convergence(result).outcome is Outcome.BROKEN


def test_a_detected_fault_that_heals_converges_in_one_round(tmp_path: Path) -> None:
    result = _run("alert_then_repair", tmp_path)

    assert result.outcome is Outcome.DETECTED
    assert _convergence(result).outcome is Outcome.PASS
    assert result.recovery is not None
    assert len(result.recovery.rounds) == 1


def test_one_after_the_other_converges_the_same(tmp_path: Path) -> None:
    result = _run("reconcile", tmp_path, reconcile_every=8, concurrent=False)

    assert result.recovery is not None
    assert len(result.recovery.rounds) == 2


def test_a_reused_baseline_is_what_every_round_is_compared_with(tmp_path: Path) -> None:
    target = cursor_target("reconcile", reconcile_every=8)
    baseline = run_baseline(TINY, target, tmp_path / "baseline")

    result = run_experiment(EXPERIMENT, target, tmp_path / "exp", baseline=baseline)

    assert "baseline (reused)" in result.runs
    assert result.recovery is not None
    assert len(result.recovery.rounds) == 2


def test_every_round_keeps_its_snapshots_and_r_is_the_last(tmp_path: Path) -> None:
    _run("reconcile", tmp_path, reconcile_every=8)

    for folder in ("recovery_round_1", "recovery_round_2", "checkpoint_R"):
        assert (tmp_path / folder / "silver_orders.parquet").is_file()
    assert not (tmp_path / "recovery_round_3").exists()
    last = (tmp_path / "recovery_round_2" / "silver_orders.parquet").read_bytes()
    assert (tmp_path / "checkpoint_R" / "silver_orders.parquet").read_bytes() == last


def test_without_a_fault_there_is_nothing_to_converge(tmp_path: Path) -> None:
    experiment = dataclasses.replace(EXPERIMENT, faults=())

    result = run_experiment(experiment, cursor_target("naive"), tmp_path)

    assert result.outcome is Outcome.ERROR
    assert result.problems == (
        "ValueError: convergence needs a recovery to count: the experiment has no fault",
    )


def test_side_by_side_a_baseline_that_fails_does_not_leave_recovery_waiting(
    tmp_path: Path,
) -> None:
    # The fault half reaches its first round, waits for the baseline, and the
    # baseline's sandbox dies: the experiment ends as ERROR, it does not hang.
    torn_down: list[str] = []
    fault_half_waiting = threading.Event()

    def factory(root: Path) -> FakeCursorPlatform:
        platform = FakeCursorPlatform(root, "naive")
        platform.teardown = lambda: torn_down.append(root.name)  # type: ignore[method-assign]
        original = platform.run
        if root.name == "generated":

            def broken(request: RunRequest) -> RunResult:
                fault_half_waiting.wait(timeout=10)
                raise RuntimeError("the clean sandbox lost its connection")

            platform.run = broken  # type: ignore[method-assign]
        else:

            def counted(request: RunRequest) -> RunResult:
                result = original(request)
                if request.partition == date(2026, 11, 4):
                    fault_half_waiting.set()
                return result

            platform.run = counted  # type: ignore[method-assign]
        return platform

    target = Target("platform", environment="test", allow_destructive=True, sandbox=factory)
    result = run_experiment(EXPERIMENT, target, tmp_path)

    assert result.outcome is Outcome.ERROR
    assert result.problems == ("RuntimeError: the clean sandbox lost its connection",)
    assert sorted(torn_down) == ["faulted_input", "generated"]
