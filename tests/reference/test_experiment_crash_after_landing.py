"""A crash between landing and loading, on both editions: day 2 dies halfway.

Day 2's run is killed, hard, right after ingestion landed its files and saved
its cursor, before anything was loaded. The crash is faultline's own
(INTERRUPTED), so it is not a signal from the platform.

Naive: days 3 and 4 only load their own partition, and dlt's cursor has moved
past day 2, so day 2 never reaches the warehouse. Nothing the platform raises
says so: SILENT_CORRUPTION. Rerunning day 2 loads the files still waiting.

Hardened: day 3's lag window reads day 2's rows again from the source, and the
day is complete before checkpoint F: PASS.

Each edition is its own class, the unit pytest-xdist schedules. Every other
experiment in the suite is compared with its edition's shared clean run
(conftest.py, `baselines`); this one's naive class runs whole, its own
baseline side by side with the fault phase, so that path, the one
`faultline run` takes for a single experiment, is still tested on the real
stack.
"""

from datetime import date
from pathlib import Path

import pytest
from reference_stack import Stack, experiment, reference_target
from shared_baseline import Baselines

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")
CRASHED = date(2026, 11, 2)


def _run(edition: Edition, work_dir: Path, baselines: Baselines | None = None) -> ExperimentResult:
    result = run_experiment(
        experiment("crash-after-landing"),
        reference_target(edition),
        work_dir,
        baseline=baselines(edition) if baselines else None,
    )
    print(result.render())
    return result


def _statuses(result: ExperimentResult, phase: str) -> dict[date, list[RunStatus]]:
    statuses: dict[date, list[RunStatus]] = {}
    for day, run in result.runs[phase]:
        statuses.setdefault(day, []).append(run.status)
    return statuses


class TestNaive:
    def test_never_loads_the_crashed_day_and_nothing_says_so(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path)  # whole, see the module docstring
        assert "baseline" in result.runs  # its own clean run, not the shared one

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        assert _statuses(result, "fault phase")[CRASHED] == [RunStatus.INTERRUPTED]
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        assert not loss.at_f.holds  # day 2 is missing...
        assert loss.at_r.holds  # ...until the operator reruns it
        assert any(
            e.startswith("silver_orders: complete_keys violated") for e in loss.at_f.evidence
        )


@pytest.mark.hardened
class TestHardened:
    def test_reads_the_crashed_day_again_on_the_next_run(
        self, stack: Stack, baselines: Baselines, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path, baselines)

        assert result.outcome is Outcome.PASS, result.render()
        assert _statuses(result, "fault phase")[CRASHED] == [RunStatus.INTERRUPTED]
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
