"""A renamed column, on both editions: the last day's payments lose their amount_vnd.

The last day runs in two halves. Between ingest and load, faultline renames
the payments file's amount_vnd to total_amount_vnd; values and types stay.
Recovery redelivers the file with the agreed schema and reruns the day.

Naive: ClickHouse reads the file through the bronze table's structure,
matching columns by name, so the renamed column is dropped and amount_vnd is
filled with zeros. Every run is green, the day's payments are worth nothing:
SILENT_CORRUPTION. (Moving the column to the end changes nothing here, since
nothing is matched by position; the toy platform tests that variant.)

Hardened: bronze checks every file against the schema contract before
loading, refuses the day on "missing amount_vnd, unexpected total_amount_vnd",
and the rerun with the agreed schema puts it right: DETECTED.

Each edition is its own class, the unit pytest-xdist schedules.
"""

from pathlib import Path

import pytest
from reference_stack import Stack, reference_config

from faultline.adapters.base import RunStatus
from faultline.adapters.reference import Edition, ReferenceAdapter, target_name
from faultline.datagen import DatasetSpec
from faultline.experiments import column_rename
from faultline.outcome import Outcome
from faultline.runner import ExperimentResult, run_experiment

pytestmark = pytest.mark.integration

TINY = DatasetSpec.from_profile("tiny")


def _run(edition: Edition, work_dir: Path) -> ExperimentResult:
    config = reference_config()
    result = run_experiment(
        column_rename(TINY),
        lambda root: ReferenceAdapter(config, root, edition=edition),
        work_dir,
        target=target_name(edition),
    )
    print(result.render())
    return result


class TestNaive:
    def test_fills_the_renamed_amount_with_zeros_without_a_word(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.NAIVE, tmp_path)

        assert result.outcome is Outcome.SILENT_CORRUPTION, result.render()
        loss = next(p for p in result.properties if p.name == "no_data_loss")
        corruption = next(p for p in result.properties if p.name == "no_silent_corruption")
        assert loss.at_f.holds  # every payment is there...
        assert not corruption.at_f.holds  # ...worth zero
        evidence = corruption.at_f.evidence
        assert any(e.startswith("silver_payments:") and "changed" in e for e in evidence)
        assert all(
            r.status is RunStatus.SUCCEEDED for runs in result.runs.values() for _, r in runs
        )


@pytest.mark.hardened
class TestHardened:
    def test_refuses_the_drifted_file_and_heals_with_the_agreed_schema(
        self, stack: Stack, tmp_path: Path
    ) -> None:
        result = _run(Edition.HARDENED, tmp_path)

        assert result.outcome is Outcome.DETECTED, result.render()
        failed = [(d, r) for d, r in result.runs["fault phase"] if r.status is RunStatus.FAILED]
        assert [d for d, _ in failed] == [TINY.end_date]
        errors = " ".join(failed[0][1].errors)
        assert "missing amount_vnd" in errors
        assert "unexpected total_amount_vnd" in errors
        for prop in result.properties:
            assert prop.at_r.holds, prop.at_r.evidence
        assert all(r.succeeded for _, r in result.runs["recovery"])
