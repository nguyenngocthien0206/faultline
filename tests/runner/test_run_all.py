"""The whole matrix: every experiment on every target, one baseline each, side by side."""

import threading
import time
from pathlib import Path

import pytest
from fake_files import FakeFilePlatform

from faultline.adapters.base import RunRequest, RunResult
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_all
from faultline.safety import Target, UnsafeTargetError
from faultline.spec import load_experiment

EXPERIMENTS = Path(__file__).resolve().parents[2] / "experiments"
LATE, RERUN = (load_experiment(EXPERIMENTS / f"{n}.yaml") for n in ("late-file", "idempotency"))


class Counter:
    """Counts the runs of every sandbox a target builds, and how many sandboxes are open."""

    def __init__(self) -> None:
        self.runs = 0
        self.open = 0
        self.most_open = 0
        self.lock = threading.Lock()

    def target(self, mode: str, *, broken: bool = False) -> Target:
        counter = self

        class Counted(FakeFilePlatform):
            def setup(self) -> None:
                if broken:
                    raise RuntimeError("cannot reach the platform")
                with counter.lock:
                    counter.open += 1
                    counter.most_open = max(counter.most_open, counter.open)

            def teardown(self) -> None:
                with counter.lock:
                    counter.open -= 1

            def run(self, request: RunRequest) -> RunResult:
                with counter.lock:
                    counter.runs += 1
                time.sleep(0.005)  # long enough for sandboxes to overlap
                return super().run(request)

        return Target(
            f"files-{mode}",
            environment="test",
            allow_destructive=True,
            sandbox=lambda root: Counted(root, mode),
        )


def test_every_pair_runs_once_on_one_shared_baseline_per_target(tmp_path: Path) -> None:
    counter = Counter()
    seen: list[str] = []

    results = run_all(
        [LATE, RERUN],
        [counter.target("append"), counter.target("keyed")],
        tmp_path,
        on_result=lambda r, _: seen.append(f"{r.experiment}@{r.target}"),
    )

    assert [(r.experiment, r.target, r.outcome) for r in results] == [
        ("late-file", "files-append", Outcome.SILENT_CORRUPTION),
        ("late-file", "files-keyed", Outcome.PASS),
        ("idempotency", "files-append", Outcome.SILENT_CORRUPTION),
        ("idempotency", "files-keyed", Outcome.PASS),
    ]
    assert sorted(seen) == sorted(f"{r.experiment}@{r.target}" for r in results)
    assert len(list((tmp_path / "baselines").iterdir())) == 2  # one per target, not per pair
    for result in results:
        assert "baseline (reused)" in result.runs
    # each fault phase: late-file runs day 2 in two halves (5) plus one rerun, idempotency 4 + 1
    days = LATE.spec.days
    assert counter.runs == 2 * days + 2 * (days + 1 + 1) + 2 * (days + 1)


def test_no_more_pairs_run_at_once_than_asked(tmp_path: Path) -> None:
    counter = Counter()

    run_all([LATE, RERUN], [counter.target("keyed")], tmp_path, parallel=1)

    assert counter.most_open == 1


def test_pairs_run_side_by_side_when_allowed(tmp_path: Path) -> None:
    counter = Counter()

    run_all([LATE, RERUN], [counter.target("append"), counter.target("keyed")], tmp_path)

    assert counter.most_open > 1


def test_a_target_that_cannot_run_its_baseline_fails_only_its_own_pairs(tmp_path: Path) -> None:
    counter = Counter()

    results = run_all(
        [LATE], [counter.target("append", broken=True), counter.target("keyed")], tmp_path
    )

    broken, fine = results
    assert broken.outcome is Outcome.ERROR
    assert broken.problems == ("baseline: RuntimeError: cannot reach the platform",)
    assert fine.outcome is Outcome.PASS


def test_an_unsafe_target_is_refused_before_anything_runs(tmp_path: Path) -> None:
    counter = Counter()
    target = counter.target("keyed")
    object.__setattr__(target, "allow_destructive", False)

    with pytest.raises(UnsafeTargetError):
        run_all([LATE], [target], tmp_path)
    assert counter.runs == 0


def test_parallel_must_allow_at_least_one_pair(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="at least 1"):
        run_all([LATE], [Counter().target("keyed")], tmp_path, parallel=0)


def test_each_pair_and_baseline_gets_its_own_log(tmp_path: Path) -> None:
    logs: dict[str, list[str]] = {}

    def log_for(name: str):  # type: ignore[no-untyped-def]
        return logs.setdefault(name, []).append

    results: list[ExperimentResult] = run_all(
        [LATE], [Counter().target("keyed")], tmp_path, log_for=log_for
    )

    assert results[0].outcome is Outcome.PASS
    assert set(logs) == {"baseline__files-keyed-0", "late-file__files-keyed"}
    assert any("[fault] run 2026-11-02" in line for line in logs["late-file__files-keyed"])
