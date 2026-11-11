"""Outcomes: turning property results and run statuses into one verdict.

A signal is an error raised by the platform itself: a run that FAILED. A run
faultline killed on purpose is INTERRUPTED and is not a signal, because the
process dying says nothing about whether the platform noticed the problem.

Decision table (design section 6), for one property at checkpoints F and R:

    holds at F and at R                             -> PASS
    violated at F with no signal in the fault phase -> SILENT_CORRUPTION
    violated at R with no signal anywhere           -> SILENT_CORRUPTION
    violated at R, a signal was raised              -> BROKEN
    violated at F with a signal, holds at R         -> DETECTED

A violation at F without a signal stays SILENT_CORRUPTION even if R heals:
consumers read wrong numbers in between and nobody raised a hand.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from enum import StrEnum

from faultline.adapters.base import RunResult, RunStatus
from faultline.properties.base import PropertyResult


class Outcome(StrEnum):
    PASS = "PASS"
    DETECTED = "DETECTED"
    BROKEN = "BROKEN"
    SILENT_CORRUPTION = "SILENT_CORRUPTION"
    SKIPPED = "SKIPPED"
    ERROR = "ERROR"


# Worst last. SKIPPED and ERROR are reported apart and never mask a verdict.
SEVERITY = (Outcome.PASS, Outcome.DETECTED, Outcome.BROKEN, Outcome.SILENT_CORRUPTION)


def has_signal(runs: Iterable[tuple[date, RunResult]]) -> bool:
    return any(result.status is RunStatus.FAILED for _, result in runs)


def classify(
    at_f: PropertyResult, at_r: PropertyResult, *, signal_f: bool, signal_r: bool
) -> Outcome:
    if at_f.holds and at_r.holds:
        return Outcome.PASS
    if not at_f.holds and not signal_f:
        return Outcome.SILENT_CORRUPTION
    if not at_r.holds:
        return Outcome.BROKEN if (signal_f or signal_r) else Outcome.SILENT_CORRUPTION
    return Outcome.DETECTED


def worst(outcomes: Iterable[Outcome]) -> Outcome:
    """The experiment verdict: the worst of its property outcomes."""
    graded = [o for o in outcomes if o in SEVERITY]
    if not graded:
        return Outcome.SKIPPED
    return max(graded, key=SEVERITY.index)
