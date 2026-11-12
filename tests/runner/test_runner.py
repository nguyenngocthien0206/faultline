"""The whole experiment machinery, end to end, on a toy platform (no Docker).

The same late-sync fault produces every verdict, depending only on how the
platform handles rows that arrive behind its cursor. That is the point of
faultline: the fault is fixed, the platform's behaviour decides the outcome.
"""

import threading
import time
from datetime import date, timedelta
from pathlib import Path

import pytest
from fake_platform import FakeCursorPlatform

from faultline.adapters.base import Capability, RunRequest, RunResult, RunStatus
from faultline.datagen import DatasetSpec
from faultline.faults.base import Fault, InjectionPoint
from faultline.faults.source import PosOfflineSync
from faultline.outcome import Outcome
from faultline.properties.core import NoDataLoss, NoSilentCorruption
from faultline.runner import Experiment, run_baseline, run_experiment

TINY = DatasetSpec.from_profile("tiny")
EXPERIMENT = Experiment(
    name="pos-offline-sync",
    description="A till goes offline for two days and syncs late.",
    spec=TINY,
    faults=(PosOfflineSync(offline_from=date(2026, 11, 2), sync_on=date(2026, 11, 4)),),
    properties=(NoDataLoss(), NoSilentCorruption()),
)


def _run(mode: str, work_dir: Path):
    return run_experiment(
        EXPERIMENT, lambda root: FakeCursorPlatform(root, mode), work_dir, target=f"fake-{mode}"
    )


@pytest.mark.parametrize(
    ("mode", "expected"),
    [
        ("naive", Outcome.SILENT_CORRUPTION),
        ("lag", Outcome.PASS),
        ("alert_then_repair", Outcome.DETECTED),
        ("alert_forever", Outcome.BROKEN),
    ],
)
def test_one_fault_every_verdict(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = _run(mode, tmp_path)

    assert result.outcome is expected, result.render()
    assert {p.outcome for p in result.properties} <= {expected, Outcome.PASS}


def test_a_lag_window_shorter_than_the_outage_loses_rows_like_naive(tmp_path: Path) -> None:
    # The lag window is a promise with a limit: the till is offline two days,
    # the platform only looks back twelve hours.
    result = run_experiment(
        EXPERIMENT,
        lambda root: FakeCursorPlatform(root, "lag", lag=timedelta(hours=12)),
        tmp_path,
        target="fake-lag-12h",
    )

    assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()


def test_naive_loses_rows_silently_and_recovery_cannot_bring_them_back(tmp_path: Path) -> None:
    result = _run("naive", tmp_path)
    loss = next(p for p in result.properties if p.name == "no_data_loss")

    assert not loss.at_f.holds
    assert not loss.at_r.holds
    assert "silver_orders: complete_keys violated by" in loss.at_f.evidence[0]
    assert all(r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs)
    assert [d.isoformat() for d, _ in result.runs["recovery"]] == [
        "2026-11-02",
        "2026-11-03",
        "2026-11-04",
    ]


def test_the_report_tells_the_story(tmp_path: Path) -> None:
    text = _run("naive", tmp_path).render()

    assert "Experiment: pos-offline-sync  (target: fake-naive)" in text
    assert "Fault:      POS of " in text
    assert "baseline 4/4 green, fault phase 4/4 green, recovery 3/3 green" in text
    assert "no_data_loss" in text
    assert "Verdict:    SILENT_CORRUPTION" in text
    assert "Evidence at F:" in text
    text.encode("ascii")  # printable on any console


def test_snapshots_are_kept_for_inspection(tmp_path: Path) -> None:
    _run("naive", tmp_path)

    for folder in ("baseline", "checkpoint_F", "checkpoint_R"):
        assert (tmp_path / folder / "silver_orders.parquet").is_file()


def test_a_platform_wrong_even_on_a_clean_run_is_an_error_not_a_verdict(tmp_path: Path) -> None:
    result = _run("lossy", tmp_path)

    assert result.outcome is Outcome.ERROR
    assert result.problems[0] == "steady state not established"
    assert not result.properties


def test_a_harness_failure_is_an_error_not_a_verdict(tmp_path: Path) -> None:
    def broken_factory(root: Path) -> FakeCursorPlatform:
        raise RuntimeError("cannot reach the platform")

    result = run_experiment(EXPERIMENT, broken_factory, tmp_path)

    assert result.outcome is Outcome.ERROR
    assert result.problems == ("RuntimeError: cannot reach the platform",)


def test_the_experiment_object_is_reusable(tmp_path: Path) -> None:
    first = _run("naive", tmp_path / "a")
    second = _run("naive", tmp_path / "b")

    assert first.faults == second.faults
    assert EXPERIMENT.faults[0].store is None  # the runner worked on a copy


class CountingFactory:
    """Builds fake platforms, counts every run they execute and how many overlap."""

    def __init__(self, mode: str, *, concurrent: bool = True) -> None:
        self.mode = mode
        self.concurrent = concurrent
        self.runs = 0
        self.max_active = 0
        self._active = 0
        self._lock = threading.Lock()

    def __call__(self, root: Path) -> FakeCursorPlatform:
        platform = FakeCursorPlatform(root, self.mode, concurrent=self.concurrent)
        original = platform.run

        def counted(request: RunRequest) -> RunResult:
            with self._lock:
                self.runs += 1
                self._active += 1
                self.max_active = max(self.max_active, self._active)
            try:
                time.sleep(0.005)  # long enough for a concurrent run to overlap
                return original(request)
            finally:
                with self._lock:
                    self._active -= 1

        platform.run = counted
        return platform


def test_a_reused_baseline_gives_the_same_verdict_with_fewer_runs(tmp_path: Path) -> None:
    fresh_factory, reuse_factory = CountingFactory("naive"), CountingFactory("naive")
    baseline = run_baseline(TINY, reuse_factory, tmp_path / "shared")
    assert baseline.established
    reuse_factory.runs = 0

    fresh = run_experiment(EXPERIMENT, fresh_factory, tmp_path / "fresh")
    reused = run_experiment(EXPERIMENT, reuse_factory, tmp_path / "reused", baseline=baseline)

    assert reused.outcome is fresh.outcome is Outcome.SILENT_CORRUPTION
    assert [p.outcome for p in reused.properties] == [p.outcome for p in fresh.properties]
    assert fresh_factory.runs - reuse_factory.runs == TINY.days  # the baseline's runs
    assert "baseline (reused) 4/4 green" in reused.render()


def test_a_baseline_from_another_dataset_is_refused(tmp_path: Path) -> None:
    other = run_baseline(
        DatasetSpec.from_profile("tiny", seed=7), CountingFactory("naive"), tmp_path / "other"
    )

    result = run_experiment(EXPERIMENT, CountingFactory("naive"), tmp_path / "exp", baseline=other)

    assert result.outcome is Outcome.ERROR
    assert "baseline was run on" in result.problems[0]


def test_a_baseline_from_another_target_is_refused(tmp_path: Path) -> None:
    naive = run_baseline(TINY, CountingFactory("naive"), tmp_path / "naive", target="fake-naive")

    result = run_experiment(
        EXPERIMENT, CountingFactory("lag"), tmp_path / "exp", target="fake-lag", baseline=naive
    )

    assert result.outcome is Outcome.ERROR
    assert "target 'fake-naive'" in result.problems[0]


def test_a_reused_baseline_that_is_not_healthy_stops_the_experiment(tmp_path: Path) -> None:
    unhealthy = run_baseline(TINY, CountingFactory("lossy"), tmp_path / "lossy")

    assert not unhealthy.established
    result = run_experiment(
        EXPERIMENT, CountingFactory("lossy"), tmp_path / "exp", baseline=unhealthy
    )
    assert result.outcome is Outcome.ERROR
    assert result.problems[0] == "steady state not established"


def test_the_two_sandboxes_really_run_at_the_same_time(tmp_path: Path) -> None:
    # Each sandbox waits at its first run until the other one gets there too.
    # Run one after the other, the barrier would time out and the result be ERROR.
    meet = threading.Barrier(2, timeout=10)

    result = run_experiment(
        EXPERIMENT,
        lambda root: FakeCursorPlatform(root, "naive", on_first_run=meet.wait),
        tmp_path,
    )

    assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()


def test_an_adapter_without_the_capability_gets_one_sandbox_at_a_time(tmp_path: Path) -> None:
    factory = CountingFactory("naive", concurrent=False)

    result = run_experiment(EXPERIMENT, factory, tmp_path)

    assert result.outcome is Outcome.SILENT_CORRUPTION
    assert factory.max_active == 1
    assert factory.runs == TINY.days * 2 + 3  # baseline, fault phase, three reruns


def test_one_after_the_other_a_broken_baseline_stops_before_the_fault_phase(
    tmp_path: Path,
) -> None:
    factory = CountingFactory("lossy", concurrent=False)

    result = run_experiment(EXPERIMENT, factory, tmp_path)

    assert result.outcome is Outcome.ERROR
    assert list(result.runs) == ["baseline"]
    assert factory.runs == TINY.days


def test_side_by_side_a_broken_baseline_is_still_an_error(tmp_path: Path) -> None:
    result = run_experiment(EXPERIMENT, CountingFactory("lossy"), tmp_path)

    assert result.outcome is Outcome.ERROR
    assert result.problems[0] == "steady state not established"
    assert list(result.runs) == ["baseline", "fault phase", "recovery"]  # it ran meanwhile
    assert not result.properties  # nothing is scored against a broken baseline


def test_when_one_sandbox_fails_the_other_still_finishes_and_is_torn_down(
    tmp_path: Path,
) -> None:
    torn_down: list[str] = []

    def factory(root: Path) -> FakeCursorPlatform:
        platform = FakeCursorPlatform(root, "naive")
        platform.teardown = lambda: torn_down.append(root.name)  # type: ignore[method-assign]
        if root.name == "faulted_input":

            def broken(request: RunRequest) -> RunResult:
                raise RuntimeError("the fault sandbox lost its connection")

            platform.run = broken  # type: ignore[method-assign]
        return platform

    result = run_experiment(EXPERIMENT, factory, tmp_path)

    assert result.outcome is Outcome.ERROR
    assert result.problems == ("RuntimeError: the fault sandbox lost its connection",)
    assert sorted(torn_down) == ["faulted_input", "generated"]
    assert (tmp_path / "baseline").is_dir()  # the clean half ran to the end


class NeedsLandingFiles(Fault):
    name = "needs_landing_files"
    injection_point = InjectionPoint.LANDING
    requires = frozenset({Capability.LANDING_FILES})

    def affected_partitions(self) -> set[date]:
        return {TINY.start_date}

    def describe(self) -> str:
        return "a landing fault"


def test_a_fault_the_adapter_cannot_host_is_skipped_before_anything_runs(tmp_path: Path) -> None:
    experiment = Experiment(
        name="landing-only",
        description="needs landing files",
        spec=TINY,
        faults=(NeedsLandingFiles(),),
        properties=(NoDataLoss(),),
    )
    factory = CountingFactory("naive")  # the toy cursor platform has no landing zone

    result = run_experiment(experiment, factory, tmp_path)

    assert result.outcome is Outcome.SKIPPED
    assert result.problems == ("the adapter does not offer landing.files",)
    assert "Skipped:    the adapter does not offer landing.files" in result.render()
    assert factory.runs == 0
    assert not (tmp_path / "generated").exists()  # nothing was even generated
