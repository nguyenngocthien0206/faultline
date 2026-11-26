"""Hand-made experiment results, shared by the report tests."""

from dataclasses import replace
from datetime import date
from pathlib import Path

from faultline.adapters.base import RunResult, RunStatus
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.properties.base import PropertyResult, Recovery, Round
from faultline.runner import ExperimentResult, PropertyVerdict

TINY = DatasetSpec.from_profile("tiny")


def result(
    experiment: str,
    target: str,
    outcome: Outcome,
    evidence: tuple[str, ...] = (),
    problems: tuple[str, ...] = (),
) -> ExperimentResult:
    holds = outcome is Outcome.PASS
    prop = PropertyVerdict(
        "no_silent_corruption",
        PropertyResult("no_silent_corruption", "F", holds, evidence),
        PropertyResult("no_silent_corruption", "R", holds, ()),
        outcome,
    )
    return ExperimentResult(
        experiment,
        target,
        outcome,
        faults=("a fault",),
        properties=() if problems else (prop,),
        runs={"fault phase": ((date(2026, 11, 2), RunResult(RunStatus.SUCCEEDED, run_id="r1")),)},
        problems=problems,
        work_dir=Path("snaps"),
    )


MATRIX = [
    result("late-file", "naive", Outcome.SILENT_CORRUPTION, ("lines: 163 missing keys",)),
    result("late-file", "hardened", Outcome.PASS),
    result("truncated-file", "naive", Outcome.SILENT_CORRUPTION, ("lines: 27 missing keys",)),
    result("truncated-file", "hardened", Outcome.DETECTED, ("lines: 85 missing keys",)),
    result("backfill", "naive", Outcome.ERROR, problems=("steady state not established",)),
]


def converging(experiment: str, target: str, outcome: Outcome, *rounds: int) -> ExperimentResult:
    """A result that measured convergence: `rounds` are the keys off after each round."""
    plain = result(experiment, target, outcome)
    holds = bool(rounds) and rounds[-1] == 0
    prop = PropertyVerdict(
        "convergence",
        PropertyResult("convergence", "F", True),
        PropertyResult("convergence", "R", holds, () if holds else ("not converged",)),
        Outcome.PASS if holds else outcome,
    )
    recovery = Recovery(3, tuple(Round(runs=1, keys_off=k, rows_off=k) for k in rounds))
    return replace(plain, properties=(*plain.properties, prop), recovery=recovery)
