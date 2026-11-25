"""Probes: actions a property adds to exercise itself, run after the fault phase's schedule."""

from datetime import date
from pathlib import Path
from typing import ClassVar

from fake_files import FakeFilePlatform

from faultline.adapters.base import RunRequest, RunResult, RunStatus
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.properties.base import Checkpoint, Property, PropertyContext, PropertyResult
from faultline.runner import Experiment, run_experiment
from faultline.schedule import Run, Schedule, Snapshot

TINY = DatasetSpec.from_profile("tiny")
D2 = date(2026, 11, 2)


class SawProbe(Property):
    """Holds when its probe's snapshot reached the checkpoint; `holds=False` always breaks."""

    name: ClassVar[str] = "saw_probe"

    def __init__(self, holds: bool = True) -> None:
        self.holds = holds

    def probe(self) -> Schedule:
        return (Run(D2), Snapshot("probed"))

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult:
        return PropertyResult(
            self.name, checkpoint.name, self.holds and "probed" in checkpoint.marks
        )


class FailsReruns(FakeFilePlatform):
    """Fails any run of a day it already ran, as a platform refusing to redo one would."""

    def __init__(self, root: Path) -> None:
        super().__init__(root, "keyed")
        self.done: set[date] = set()

    def run(self, request: RunRequest) -> RunResult:
        if request.partition in self.done:
            return RunResult(RunStatus.FAILED, errors=("already done",))
        self.done.add(request.partition)
        return super().run(request)


def _experiment(prop: Property) -> Experiment:
    return Experiment("probed", "a probe and nothing else", TINY, faults=(), properties=(prop,))


def test_a_probe_runs_after_the_schedule_and_both_checkpoints_see_it(tmp_path: Path) -> None:
    lines: list[str] = []

    result = run_experiment(
        _experiment(SawProbe()),
        lambda root: FakeFilePlatform(root, "keyed"),
        tmp_path,
        log=lines.append,
    )

    assert result.outcome is Outcome.PASS, result.render()
    fault_phase = [day for day, _ in result.runs["fault phase"]]
    assert fault_phase == [*TINY.dates(), D2]  # the probe's rerun comes last
    assert [day for day, _ in result.runs["baseline"]] == list(TINY.dates())  # never probed
    assert "  [probe] run 2026-11-02: SUCCEEDED" in lines
    assert "  [probe] snapshot 'probed'" in lines
    assert result.runs["recovery"] == ()  # no fault, nothing to rerun


def test_a_probe_run_that_fails_is_a_signal(tmp_path: Path) -> None:
    result = run_experiment(_experiment(SawProbe(holds=False)), FailsReruns, tmp_path)

    assert result.outcome is Outcome.BROKEN, result.render()  # loud, and never healed
