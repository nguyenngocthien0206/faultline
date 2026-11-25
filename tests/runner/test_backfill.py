"""Backfills: the action, the deliveries-only schedule, and the experiment on toy platforms."""

from datetime import date
from pathlib import Path

import pytest
from fake_files import FakeFilePlatform
from fake_platform import FakeCursorPlatform

from faultline.datagen import DatasetSpec
from faultline.experiments import BUILTIN, backfill
from faultline.outcome import Outcome
from faultline.properties.core import BackfillEquivalence
from faultline.runner import execute, generate_truth, run_experiment
from faultline.schedule import Backfill, Deliver, deliveries_only

TINY = DatasetSpec.from_profile("tiny")
D1, D2, D4 = date(2026, 11, 1), date(2026, 11, 2), date(2026, 11, 4)


def test_a_backfill_covers_every_day_of_its_range() -> None:
    assert Backfill(D1, D4).days() == (D1, D2, date(2026, 11, 3), D4)
    assert Backfill(D2, D2).days() == (D2,)
    with pytest.raises(ValueError, match="covers no day"):
        Backfill(D4, D1).days()


def test_deliveries_only_brings_every_day_in_and_runs_nothing() -> None:
    assert deliveries_only(TINY) == tuple(Deliver(d) for d in TINY.dates())


def test_the_runner_logs_and_records_every_partition_of_a_backfill(tmp_path: Path) -> None:
    platform = FakeFilePlatform(generate_truth(TINY, tmp_path), "keyed")
    lines: list[str] = []

    runs = execute(platform, (*deliveries_only(TINY), Backfill(D1, D4)), lines.append, "probe")

    assert [day for day, _ in runs] == list(TINY.dates())
    assert all(r.succeeded for _, r in runs)
    assert "  [probe] backfill 2026-11-01: SUCCEEDED" in lines


def test_the_built_in_experiment_delivers_first_then_backfills_the_whole_range() -> None:
    experiment = BUILTIN["backfill"](TINY)

    assert experiment.faults == ()
    assert experiment.schedule(TINY) == deliveries_only(TINY)
    (prop, *_) = experiment.properties
    assert isinstance(prop, BackfillEquivalence)
    assert prop.probe() == (Backfill(D1, D4),)
    with pytest.raises(ValueError, match="at least 2 days"):
        backfill(DatasetSpec.from_profile("tiny", days=1))


@pytest.mark.parametrize(
    ("mode", "expected"), [("append", Outcome.SILENT_CORRUPTION), ("keyed", Outcome.PASS)]
)
def test_one_backfill_two_platforms(mode: str, expected: Outcome, tmp_path: Path) -> None:
    result = run_experiment(
        backfill(TINY), lambda root: FakeFilePlatform(root, mode), tmp_path, target=f"files-{mode}"
    )

    assert result.outcome is expected, result.render()


def test_side_by_side_extracts_from_one_cursor_load_the_history_once_each(
    tmp_path: Path,
) -> None:
    result = run_experiment(backfill(TINY), lambda root: FakeFilePlatform(root, "append"), tmp_path)

    equivalence = next(p for p in result.properties if p.name == "backfill_equivalence")
    assert not equivalence.at_f.holds
    assert any(
        e.startswith("backfill 2026-11-01..2026-11-04: silver_order_items: ")
        and "duplicated keys" in e
        for e in equivalence.at_f.evidence
    ), equivalence.at_f.evidence


def test_an_adapter_without_backfills_is_skipped_before_anything_runs(tmp_path: Path) -> None:
    result = run_experiment(backfill(TINY), lambda root: FakeCursorPlatform(root, "lag"), tmp_path)

    assert result.outcome is Outcome.SKIPPED
    assert result.problems == ("the adapter does not offer run.backfill",)
    assert not (tmp_path / "generated").exists()
