"""Run one experiment end to end: baseline, fault phase, recovery, verdict.

    1. generate   the dataset, from its seed: this is the truth
    2. baseline   a fresh sandbox runs the clean schedule; snapshot; check the
                  steady state (key invariants). Not established -> ERROR, stop.
    3. fault      a second fresh sandbox runs the faulted schedule on a faulted
                  copy of the input; snapshot at checkpoint F
    4. recovery   heal actions, then rerun the affected partitions (the
                  operator's standard move); snapshot at checkpoint R
    5. score      every property at F and R, classified into one outcome

Sandboxes are always torn down. Snapshots stay in the work directory, so a
verdict can be inspected after the fact.
"""

from __future__ import annotations

import copy
import shutil
import traceback
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import date
from functools import reduce
from pathlib import Path

from faultline.adapters.base import Adapter, RunRequest, RunResult
from faultline.datagen import DatasetSpec, generate
from faultline.faults.base import Fault
from faultline.oracle.invariants import complete_keys, unique_keys
from faultline.oracle.snapshot import snapshot_all
from faultline.outcome import Outcome, classify, has_signal, worst
from faultline.properties.base import Checkpoint, Property, PropertyContext, PropertyResult
from faultline.schedule import Deliver, Run, Schedule, clean_schedule

AdapterFactory = Callable[[Path], Adapter]  # input root -> a fresh sandbox
Log = Callable[[str], None]
RunLog = tuple[tuple[date, RunResult], ...]


@dataclass(frozen=True)
class Experiment:
    name: str
    description: str
    spec: DatasetSpec
    faults: tuple[Fault, ...]
    properties: tuple[Property, ...]
    recovery_rounds: int = 1


@dataclass(frozen=True)
class PropertyVerdict:
    name: str
    at_f: PropertyResult
    at_r: PropertyResult
    outcome: Outcome


@dataclass(frozen=True)
class ExperimentResult:
    experiment: str
    target: str
    outcome: Outcome
    faults: tuple[str, ...] = ()
    properties: tuple[PropertyVerdict, ...] = ()
    runs: dict[str, RunLog] = field(default_factory=dict)
    problems: tuple[str, ...] = ()  # why the result is ERROR, if it is
    work_dir: Path | None = None

    def render(self) -> str:
        lines = [f"Experiment: {self.experiment}  (target: {self.target})"]
        lines += [f"Fault:      {f}" for f in self.faults]
        if self.runs:
            parts = []
            for phase, runs in self.runs.items():
                green = sum(1 for _, r in runs if r.succeeded)
                parts.append(f"{phase} {green}/{len(runs)} green")
            lines.append("Runs:       " + ", ".join(parts))
        if self.properties:
            lines.append(f"  {'property':<24}{'F':<10}{'R':<10}outcome")
            for p in self.properties:
                f = "holds" if p.at_f.holds else "violated"
                r = "holds" if p.at_r.holds else "violated"
                lines.append(f"  {p.name:<24}{f:<10}{r:<10}{p.outcome}")
        lines.append(f"Verdict:    {self.outcome}")
        lines += [f"Problem:    {p}" for p in self.problems]
        for checkpoint in ("F", "R"):
            evidence = sorted(
                {e for p in self.properties for e in self._at(p, checkpoint).evidence}
            )
            if evidence:
                lines.append(f"Evidence at {checkpoint}:")
                lines += [f"  - {e}" for e in evidence]
        if self.work_dir is not None:
            lines.append(f"Snapshots:  {self.work_dir}")
        return "\n".join(lines)

    @staticmethod
    def _at(p: PropertyVerdict, checkpoint: str) -> PropertyResult:
        return p.at_f if checkpoint == "F" else p.at_r


def execute(adapter: Adapter, schedule: Schedule, log: Log, phase: str) -> RunLog:
    runs: list[tuple[date, RunResult]] = []
    for action in schedule:
        if isinstance(action, Deliver):
            adapter.deliver(action.day)
        elif isinstance(action, Run):
            result = adapter.run(RunRequest(partition=action.partition))
            log(f"  [{phase}] run {action.partition}: {result.status}")
            runs.append((action.partition, result))
    return tuple(runs)


def steady_state_problems(adapter: Adapter, snapshots: dict[str, Path], truth: Path) -> list[str]:
    problems = []
    for contract in adapter.capabilities().tables:
        checks = [unique_keys(snapshots[contract.name], contract)]
        if contract.truth_dataset:
            checks.append(complete_keys(snapshots[contract.name], contract, truth))
        problems += [c.summary() for c in checks if not c.holds]
    return problems


def run_experiment(
    experiment: Experiment,
    adapter_factory: AdapterFactory,
    work_dir: Path,
    *,
    target: str = "platform",
    log: Log = lambda _: None,
) -> ExperimentResult:
    work_dir = Path(work_dir)
    faults = tuple(copy.deepcopy(f) for f in experiment.faults)  # keep the experiment reusable
    base = {"experiment": experiment.name, "target": target, "work_dir": work_dir}
    try:
        return _run(experiment, faults, adapter_factory, work_dir, base, log)
    except Exception as exc:  # a harness failure is never a verdict about the platform
        log(traceback.format_exc())
        problem = f"{type(exc).__name__}: {exc}"
        return ExperimentResult(**base, outcome=Outcome.ERROR, problems=(problem,))


def _run(
    experiment: Experiment,
    faults: tuple[Fault, ...],
    adapter_factory: AdapterFactory,
    work_dir: Path,
    base: dict,
    log: Log,
) -> ExperimentResult:
    truth = work_dir / "generated"
    log(f"generating {experiment.spec.profile} dataset")
    generate(experiment.spec, truth)
    descriptions: list[str] = []

    log("baseline: clean run")
    with adapter_factory(truth) as clean:
        baseline_runs = execute(clean, clean_schedule(experiment.spec), log, "baseline")
        baseline = snapshot_all(clean, work_dir / "baseline")
        contracts = clean.capabilities().tables
        problems = steady_state_problems(clean, baseline, truth)
    runs = {"baseline": baseline_runs}
    failed = [f"baseline run {d} failed: {r.errors}" for d, r in baseline_runs if not r.succeeded]
    if failed or problems:
        log("steady state not established")
        return ExperimentResult(
            **base,
            outcome=Outcome.ERROR,
            runs=runs,
            problems=("steady state not established", *failed, *problems),
        )

    faulted_input = work_dir / "faulted_input"
    shutil.copytree(truth, faulted_input)
    for fault in faults:
        fault.prepare_input(faulted_input)
        descriptions.append(fault.describe())
        log(f"fault: {fault.describe()}")
    schedule = reduce(lambda s, f: f.apply(s), faults, clean_schedule(experiment.spec))
    affected = sorted({p for f in faults for p in f.affected_partitions()})
    heal = tuple(a for f in faults for a in f.heal())

    with adapter_factory(faulted_input) as sandbox:
        runs["fault phase"] = execute(sandbox, schedule, log, "fault")
        at_f = Checkpoint(
            "F", snapshot_all(sandbox, work_dir / "checkpoint_F"), runs["fault phase"]
        )
        recovery = heal + tuple(Run(p) for _ in range(experiment.recovery_rounds) for p in affected)
        runs["recovery"] = execute(sandbox, recovery, log, "recovery")
        at_r = Checkpoint("R", snapshot_all(sandbox, work_dir / "checkpoint_R"), runs["recovery"])

    ctx = PropertyContext(contracts=contracts, baseline=baseline, truth_root=truth)
    signal_f, signal_r = has_signal(at_f.runs), has_signal(at_r.runs)
    verdicts = []
    for prop in experiment.properties:
        rf, rr = prop.check(ctx, at_f), prop.check(ctx, at_r)
        outcome = classify(rf, rr, signal_f=signal_f, signal_r=signal_r)
        verdicts.append(PropertyVerdict(prop.name, rf, rr, outcome))
        log(f"{prop.name}: {outcome}")

    return ExperimentResult(
        **base,
        outcome=worst(v.outcome for v in verdicts),
        faults=tuple(descriptions),
        properties=tuple(verdicts),
        runs=runs,
    )
