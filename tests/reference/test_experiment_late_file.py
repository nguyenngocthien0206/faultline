"""A landing fault between ingest and load, on both editions: a file arrives late.

Day 2 runs in two halves. Between ingest and load, faultline holds the order
lines file back; it lands under dt=day 2 just before day 4 runs, two days late.

Naive: the load of day 2 finds no order lines, later runs only read their own
partition, and dlt's cursor has moved on, so day 2's lines never arrive. Every
run is green: SILENT_CORRUPTION. Rerunning day 2 picks the file up, but loads
the day's orders and payments a second time.

Hardened: the manifest of day 2's extract lists the lines file, so its load
fails on the missing file, loudly. The lag window re-reads day 2's lines from
the source on day 3, so consumers see the clean numbers: PASS, with the red
run as the notice. That holds because this source can be read again; a source
that only drops files would need a look back in bronze.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, experiment, reference_target

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition
from faultline.datagen import DatasetSpec
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    result = run_experiment(experiment("late-file"), reference_target(edition), work_dir)
    print(result.render())
    return result


def _all_green(result: ExperimentResult) -> bool:
    return all(r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs)


class TestNaive:
    def test_never_sees_the_late_lines(self, stack: Stack, tmp_path: Path) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        evidence = loss.at_f.evidence
        assert any(e.startswith("silver_order_items: complete_keys violated") for e in evidence)
        assert not any(e.startswith("silver_orders:") for e in evidence)  # orders all there
        assert _all_green(result)


@pytest.mark.hardened
class TestHardened:
    def test_flags_the_missing_file_and_reads_the_lines_again_from_the_source(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.PASS, result.render()
        for prop in result.properties:
            assert prop.at_f.holds, prop.at_f.evidence
            assert prop.at_r.holds, prop.at_r.evidence
        failed = [(d, r) for d, r in result.runs["fault phase"] if r.status is RunStatus.FAILED]
        assert len(failed) == 1  # the load of day 2
        assert any("is missing" in e for e in failed[0][1].errors), failed[0][1].errors
        assert all(r.succeeded for _, r in result.runs["recovery"])
