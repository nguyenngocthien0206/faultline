"""Run one experiment end to end: baseline, fault phase, recovery, verdict.

    1. generate   the dataset, from its seed: this is the truth; the faults
                  then transform a copy of it and the schedule
    2. baseline   a fresh sandbox runs the clean schedule; snapshot; check the
                  steady state (key invariants). Not established -> ERROR.
    3. fault      a second fresh sandbox runs the faulted schedule on the
                  faulted copy of the input; snapshot at checkpoint F
    4. recovery   heal actions, then rerun the affected partitions (the
                  operator's standard move); snapshot at checkpoint R
    5. score      every property at F and R, classified into one outcome

The two sandboxes only meet at step 5. When the adapter declares
CONCURRENT_SANDBOXES, step 2 runs side by side with steps 3 and 4, and an
experiment takes as long as its longer half. A baseline that turns out broken
then still yields ERROR, only after the fault phase has run as well: faultline
never kills a platform's runs midway. Without the capability (one shared test
environment, say) the steps run one after the other and stop at a broken
baseline.

A fault that needs a capability the adapter does not declare (landing files,
say) makes the experiment SKIPPED before anything is generated or run.

Sandboxes are always torn down. Snapshots stay in the work directory, so a
verdict can be inspected after the fact.

Steps 1 and 2 are `run_baseline`. Its result can be passed to several
experiments on the same dataset and target, so the clean run is paid for once.
A baseline from another dataset or another target is refused: comparing a
hardened platform against naive's clean run would score the difference
between the two platforms, not the fault.
"""

from __future__ import annotations

import copy
import shutil
import threading
import traceback
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import date
from functools import reduce
from pathlib import Path
from typing import TypeVar

from faultline.adapters.base import (
    Adapter,
    AdapterError,
    Capability,
    RunRequest,
    RunResult,
    TableContract,
)
from faultline.datagen import DatasetSpec, generate
from faultline.faults.base import Fault
from faultline.oracle.invariants import complete_keys, unique_keys
from faultline.oracle.snapshot import snapshot_all
from faultline.outcome import Outcome, classify, has_signal, worst
from faultline.properties.base import Checkpoint, Property, PropertyContext, PropertyResult
from faultline.schedule import Action, CopyLandingFiles, Deliver, Run, Schedule, clean_schedule

AdapterFactory = Callable[[Path], Adapter]  # input root -> a fresh sandbox
Log = Callable[[str], None]
RunLog = tuple[tuple[date, RunResult], ...]
A = TypeVar("A")
B = TypeVar("B")


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
    problems: tuple[str, ...] = ()  # why the result is ERROR or SKIPPED, if it is
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
        label = "Skipped:    " if self.outcome is Outcome.SKIPPED else "Problem:    "
        lines += [f"{label}{p}" for p in self.problems]
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
        elif isinstance(action, CopyLandingFiles):
            files = copy_landing_files(adapter, action)
            log(
                f"  [{phase}] {action.dataset}: {len(files)} file(s) of {action.from_partition} "
                f"landed again under {action.to_partition}"
            )
    return tuple(runs)


def copy_landing_files(adapter: Adapter, action: CopyLandingFiles) -> list[str]:
    """Copy byte for byte, so the platform sees exactly what it already loaded once."""
    files = adapter.landing_files(action.dataset, action.from_partition)
    if not files:
        raise AdapterError(
            f"no landing files of {action.dataset} for {action.from_partition} to copy"
        )
    for file in files:
        data = adapter.read_landing_file(action.dataset, action.from_partition, file)
        adapter.write_landing_file(action.dataset, action.to_partition, action.prefix + file, data)
    return files


def steady_state_problems(adapter: Adapter, snapshots: dict[str, Path], truth: Path) -> list[str]:
    problems = []
    for contract in adapter.capabilities().tables:
        checks = [unique_keys(snapshots[contract.name], contract)]
        if contract.truth_dataset:
            checks.append(complete_keys(snapshots[contract.name], contract, truth))
        problems += [c.summary() for c in checks if not c.holds]
    return problems


@dataclass(frozen=True)
class Baseline:
    """A clean run's outcome, reusable by every experiment on the same dataset and target."""

    spec: DatasetSpec
    truth_root: Path
    snapshots: dict[str, Path]
    contracts: tuple[TableContract, ...]
    runs: RunLog
    problems: tuple[str, ...] = ()
    target: str = "platform"

    @property
    def established(self) -> bool:
        """The steady state holds: every run green and every key invariant satisfied."""
        return not self.problems


def generate_truth(spec: DatasetSpec, work_dir: Path, log: Log = lambda _: None) -> Path:
    """Generate the dataset into `work_dir/generated` (step 1): what a correct platform shows."""
    truth = Path(work_dir) / "generated"
    log(f"generating {spec.profile} dataset")
    generate(spec, truth)
    return truth


def _clean_run(
    spec: DatasetSpec, truth: Path, adapter: Adapter, work_dir: Path, target: str, log: Log
) -> Baseline:
    log("baseline: clean run")
    with adapter as clean:
        runs = execute(clean, clean_schedule(spec), log, "baseline")
        snapshots = snapshot_all(clean, work_dir / "baseline")
        contracts = clean.capabilities().tables
        invariants = steady_state_problems(clean, snapshots, truth)
    failed = [f"baseline run {d} failed: {r.errors}" for d, r in runs if not r.succeeded]
    problems = (*failed, *invariants)
    return Baseline(spec, truth, snapshots, contracts, runs, tuple(problems), target)


def run_baseline(
    spec: DatasetSpec,
    adapter_factory: AdapterFactory,
    work_dir: Path,
    *,
    target: str = "platform",
    log: Log = lambda _: None,
) -> Baseline:
    """Generate the dataset and run it cleanly in a fresh sandbox (steps 1 and 2)."""
    work_dir = Path(work_dir)
    truth = generate_truth(spec, work_dir, log)
    return _clean_run(spec, truth, adapter_factory(truth), work_dir, target, log)


@dataclass(frozen=True)
class _FaultPlan:
    """What the fault phase needs, worked out from the truth before any sandbox runs."""

    input_root: Path
    schedule: Schedule
    recovery: tuple[Action, ...]
    descriptions: tuple[str, ...]


def _plan_faults(
    experiment: Experiment, faults: tuple[Fault, ...], truth: Path, work_dir: Path, log: Log
) -> _FaultPlan:
    faulted_input = work_dir / "faulted_input"
    shutil.copytree(truth, faulted_input)
    descriptions = []
    for fault in faults:
        fault.prepare_input(faulted_input)
        descriptions.append(fault.describe())
        log(f"fault: {fault.describe()}")
    schedule = reduce(lambda s, f: f.apply(s), faults, clean_schedule(experiment.spec))
    affected = sorted({p for f in faults for p in f.affected_partitions()})
    heal = tuple(a for f in faults for a in f.heal())
    recovery = heal + tuple(Run(p) for _ in range(experiment.recovery_rounds) for p in affected)
    return _FaultPlan(faulted_input, schedule, recovery, tuple(descriptions))


def _fault_phases(
    plan: _FaultPlan, adapter: Adapter, work_dir: Path, log: Log
) -> tuple[Checkpoint, Checkpoint]:
    """Steps 3 and 4 in one sandbox: the faulted schedule, then the recovery."""
    with adapter as sandbox:
        fault_runs = execute(sandbox, plan.schedule, log, "fault")
        at_f = Checkpoint("F", snapshot_all(sandbox, work_dir / "checkpoint_F"), fault_runs)
        recovery_runs = execute(sandbox, plan.recovery, log, "recovery")
        at_r = Checkpoint("R", snapshot_all(sandbox, work_dir / "checkpoint_R"), recovery_runs)
    return at_f, at_r


def _side_by_side(first: Callable[[], A], second: Callable[[], B]) -> tuple[A, B]:
    """Run both in their own thread and wait for both, even when one fails.

    Waiting matters: each half tears its own sandbox down, and a half left
    running would leave a sandbox behind. The first failure is raised after.
    """
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="sandbox") as pool:
        a, b = pool.submit(first), pool.submit(second)
    return a.result(), b.result()


def _synchronized(log: Log) -> Log:
    """One line at a time, when two sandboxes log at once."""
    lock = threading.Lock()

    def locked(line: str) -> None:
        with lock:
            log(line)

    return locked


def run_experiment(
    experiment: Experiment,
    adapter_factory: AdapterFactory,
    work_dir: Path,
    *,
    target: str = "platform",
    baseline: Baseline | None = None,
    log: Log = lambda _: None,
) -> ExperimentResult:
    """Run one experiment. Pass `baseline` to reuse a clean run of the same dataset."""
    work_dir = Path(work_dir)
    log = _synchronized(log)
    faults = tuple(copy.deepcopy(f) for f in experiment.faults)  # keep the experiment reusable
    base = {"experiment": experiment.name, "target": target, "work_dir": work_dir}
    try:
        return _run(experiment, faults, adapter_factory, work_dir, base, baseline, log)
    except Exception as exc:  # a harness failure is never a verdict about the platform
        log(traceback.format_exc())
        problem = f"{type(exc).__name__}: {exc}"
        return ExperimentResult(**base, outcome=Outcome.ERROR, problems=(problem,))


def _missing_capabilities(
    faults: tuple[Fault, ...], adapter_factory: AdapterFactory, work_dir: Path
) -> list[str]:
    """What the faults need that the adapter does not declare, checked before anything runs.

    Building an adapter opens nothing, so asking a throwaway one is free.
    """
    needed = frozenset().union(*(f.requires for f in faults))
    if not needed:
        return []
    offered = adapter_factory(work_dir / "generated").capabilities().features
    return sorted(str(c) for c in needed - offered)


def _check_reusable(baseline: Baseline, spec: DatasetSpec, target: str) -> None:
    if baseline.spec != spec:
        raise ValueError(f"baseline was run on {baseline.spec}, the experiment needs {spec}")
    if baseline.target != target:
        raise ValueError(
            f"baseline was run on target {baseline.target!r}, the experiment runs on {target!r}"
        )


def _not_established(
    base: dict, runs: dict[str, RunLog], baseline: Baseline, faults: tuple[str, ...] = ()
) -> ExperimentResult:
    return ExperimentResult(
        **base,
        outcome=Outcome.ERROR,
        faults=faults,
        runs=runs,
        problems=("steady state not established", *baseline.problems),
    )


def _run(
    experiment: Experiment,
    faults: tuple[Fault, ...],
    adapter_factory: AdapterFactory,
    work_dir: Path,
    base: dict,
    baseline: Baseline | None,
    log: Log,
) -> ExperimentResult:
    target = base["target"]
    missing = _missing_capabilities(faults, adapter_factory, work_dir)
    if missing:
        log(f"skipped: the adapter does not offer {', '.join(missing)}")
        return ExperimentResult(
            **base,
            outcome=Outcome.SKIPPED,
            faults=tuple(f.describe() for f in faults),
            problems=(f"the adapter does not offer {', '.join(missing)}",),
        )
    if baseline is not None:
        _check_reusable(baseline, experiment.spec, target)
        log("baseline: reused")
        phase = "baseline (reused)"
        if not baseline.established:
            log("steady state not established")
            return _not_established(base, {phase: baseline.runs}, baseline)
        plan = _plan_faults(experiment, faults, baseline.truth_root, work_dir, log)
        at_f, at_r = _fault_phases(plan, adapter_factory(plan.input_root), work_dir, log)
    else:
        phase = "baseline"
        truth = generate_truth(experiment.spec, work_dir, log)
        plan = _plan_faults(experiment, faults, truth, work_dir, log)
        # Building an adapter opens nothing (setup happens on entering it), so
        # both are built up front and the capability is read before any run.
        clean, sandbox = adapter_factory(truth), adapter_factory(plan.input_root)
        if clean.capabilities().supports(Capability.CONCURRENT_SANDBOXES):
            log("baseline and fault phase run side by side")
            baseline, (at_f, at_r) = _side_by_side(
                lambda: _clean_run(experiment.spec, truth, clean, work_dir, target, log),
                lambda: _fault_phases(plan, sandbox, work_dir, log),
            )
        else:
            baseline = _clean_run(experiment.spec, truth, clean, work_dir, target, log)
            if not baseline.established:
                log("steady state not established")
                return _not_established(base, {phase: baseline.runs}, baseline, plan.descriptions)
            at_f, at_r = _fault_phases(plan, sandbox, work_dir, log)

    runs = {phase: baseline.runs, "fault phase": at_f.runs, "recovery": at_r.runs}
    if not baseline.established:  # side by side: found out after the fault phase ran
        log("steady state not established")
        return _not_established(base, runs, baseline, plan.descriptions)

    ctx = PropertyContext(
        contracts=baseline.contracts, baseline=baseline.snapshots, truth_root=baseline.truth_root
    )
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
        faults=plan.descriptions,
        properties=tuple(verdicts),
        runs=runs,
    )
