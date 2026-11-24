"""A cut file, on both editions: the last day's order lines lose a fifth of their rows.

The last day runs in two halves. Between ingest and load, faultline cuts the
order lines file to 80% of its rows, on a row boundary, so it still parses and
every value in it is valid. Recovery redelivers the full file and reruns the day.

Naive: loads what it finds. The missing lines go unnoticed and every run is
green: SILENT_CORRUPTION. The rerun loads the full file on top of the cut one.

Hardened: the extract's manifest promised more rows than the file holds, so
bronze refuses the day and the run fails. The day is wrong at F, with a
signal; with the full file back the rerun reconciles and the day is right:
DETECTED. The last day on purpose: on any earlier day the next run's lag window
would read the rows again from the source and hide the damage before F.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition, ReferenceAdapter, target_name
from faultline.datagen import DatasetSpec
from faultline.experiments import truncated_file
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    config = reference_config()
    result = run_experiment(
        truncated_file(TINY),
        lambda root: ReferenceAdapter(config, root, edition=edition),
        work_dir,
        target=target_name(edition),
    )
    print(result.render())
    return result


class TestNaive:
    def test_loads_the_short_file_without_a_word(self, stack: Stack, tmp_path: Path) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        evidence = loss.at_f.evidence
        assert any(e.startswith("silver_order_items: complete_keys violated") for e in evidence)
        assert all(
            r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs
        )


@pytest.mark.hardened
class TestHardened:
    def test_refuses_the_short_file_and_heals_with_the_full_one(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.DETECTED, result.render()
        failed = [(d, r) for d, r in result.runs["fault phase"] if r.status is RunStatus.FAILED]
        assert [d for d, _ in failed] == [TINY.end_date]  # the load of the cut day
        assert any("promised" in e and "rows" in e for e in failed[0][1].errors)
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        assert not loss.at_f.holds  # the day is missing until the file is right...
        for prop in result.properties:
            assert prop.at_r.holds, prop.at_r.evidence  # ...and right once it is
        assert all(r.succeeded for _, r in result.runs["recovery"])
