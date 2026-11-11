from datetime import date

import pytest

from faultline.adapters.base import RunResult, RunStatus
from faultline.outcome import Outcome, classify, has_signal, worst
from faultline.properties.base import PropertyResult

D = date(2026, 11, 1)


def _result(holds: bool, checkpoint: str) -> PropertyResult:
    return PropertyResult("p", checkpoint, holds)


@pytest.mark.parametrize(
    ("holds_f", "holds_r", "signal_f", "signal_r", "expected"),
    [
        (True, True, False, False, Outcome.PASS),
        (True, True, True, False, Outcome.PASS),  # a failed run alone is not a violation
        (False, True, True, False, Outcome.DETECTED),  # noticed, then healed
        (False, False, True, False, Outcome.BROKEN),  # noticed, never healed
        (False, False, False, True, Outcome.SILENT_CORRUPTION),  # wrong while green at F
        (False, True, False, False, Outcome.SILENT_CORRUPTION),  # healed, but silently wrong first
        (False, False, False, False, Outcome.SILENT_CORRUPTION),  # the worst case
        (True, False, False, False, Outcome.SILENT_CORRUPTION),  # recovery broke it, silently
        (True, False, False, True, Outcome.BROKEN),  # recovery broke it, loudly
    ],
)
def test_decision_table(
    holds_f: bool, holds_r: bool, signal_f: bool, signal_r: bool, expected: Outcome
) -> None:
    outcome = classify(
        _result(holds_f, "F"), _result(holds_r, "R"), signal_f=signal_f, signal_r=signal_r
    )

    assert outcome is expected


def test_only_platform_failures_are_signals() -> None:
    assert not has_signal([(D, RunResult(RunStatus.SUCCEEDED))])
    assert not has_signal([(D, RunResult(RunStatus.INTERRUPTED))])  # faultline's own kill
    assert has_signal([(D, RunResult(RunStatus.SUCCEEDED)), (D, RunResult(RunStatus.FAILED))])


def test_the_experiment_takes_the_worst_verdict() -> None:
    assert worst([Outcome.PASS, Outcome.DETECTED]) is Outcome.DETECTED
    assert worst([Outcome.BROKEN, Outcome.SILENT_CORRUPTION, Outcome.PASS]) is (
        Outcome.SILENT_CORRUPTION
    )
    assert worst([Outcome.SKIPPED, Outcome.PASS]) is Outcome.PASS  # skips never mask
    assert worst([Outcome.SKIPPED]) is Outcome.SKIPPED
