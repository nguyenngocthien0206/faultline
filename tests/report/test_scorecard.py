"""The scorecard and its expectations, on hand-made results."""

import json
from pathlib import Path

import pytest
from report_helpers import MATRIX

from faultline.report.results import result_record
from faultline.report.scorecard import (
    ExpectationError,
    Scorecard,
    check,
    evidence_line,
    load_expected,
)
from faultline.runner import ExperimentResult


def _card(results: list[ExperimentResult] = MATRIX) -> Scorecard:
    records = [result_record(r) for r in results]
    return Scorecard.from_records("20261126-101500", records, targets=("naive", "hardened"))


# --------------------------------------------------------------- scorecard


def test_the_scorecard_is_a_matrix_of_verdicts_with_totals_and_evidence() -> None:
    text = _card().to_markdown()

    assert text.splitlines()[:7] == [
        "# faultline scorecard, run 20261126-101500",
        "",
        "| experiment | naive | hardened |",
        "|---|---|---|",
        "| backfill | ERROR | - |",
        "| late-file | SILENT_CORRUPTION | PASS |",
        "| truncated-file | SILENT_CORRUPTION | DETECTED |",
    ]
    assert "- naive: 2 SILENT_CORRUPTION, 1 ERROR" in text
    assert "- hardened: 1 PASS, 1 DETECTED" in text
    assert (
        "- late-file on naive (SILENT_CORRUPTION): no_silent_corruption: lines: 163 missing keys"
        in text
    )
    assert "- backfill on naive (ERROR): steady state not established" in text
    assert "late-file on hardened" not in text  # a PASS needs no evidence
    text.encode("ascii")  # printable on any console


def test_the_json_scorecard_carries_the_same_matrix() -> None:
    data = _card().to_json()

    assert data["matrix"]["truncated-file"] == {
        "naive": "SILENT_CORRUPTION",
        "hardened": "DETECTED",
    }
    assert data["matrix"]["backfill"] == {"naive": "ERROR", "hardened": None}
    assert data["evidence"]["truncated-file__hardened"].endswith("lines: 85 missing keys")
    json.dumps(data)


def test_evidence_comes_from_the_property_that_decided_the_cell() -> None:
    record = result_record(MATRIX[0])
    record["properties"].insert(
        0,
        {
            "name": "no_data_loss",
            "outcome": "PASS",
            "at_f": {"holds": True, "evidence": ["not this one"]},
            "at_r": {"holds": True, "evidence": []},
        },
    )

    assert evidence_line(record) == "no_silent_corruption: lines: 163 missing keys"


# ------------------------------------------------------------ expectations


def _expected(tmp_path: Path, text: str) -> Path:
    path = tmp_path / "expected.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_a_scorecard_as_expected_passes_and_a_list_allows_any_of_its_verdicts(
    tmp_path: Path,
) -> None:
    expected = load_expected(
        _expected(
            tmp_path,
            "naive:\n  late-file: SILENT_CORRUPTION\n"
            "  truncated-file: [BROKEN, SILENT_CORRUPTION]\n"
            "hardened:\n  late-file: PASS\n  truncated-file: DETECTED\n",
        )
    )

    outcome = check(_card(), expected)

    assert outcome.passed
    assert outcome.unexpected == ("backfill on naive: ERROR, no expectation",)


def test_a_cell_that_moved_or_never_ran_fails_the_check(tmp_path: Path) -> None:
    expected = load_expected(
        _expected(
            tmp_path,
            "hardened:\n  truncated-file: PASS\n  backfill: PASS\n",
        )
    )

    outcome = check(_card(), expected)

    assert not outcome.passed
    assert outcome.mismatches == (
        "truncated-file on hardened: expected PASS, got DETECTED",
        "backfill on hardened: expected PASS, not run",
    )


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("naive:\n  late-file: FINE\n", "unknown verdict"),
        ("naive:\n  late-file: []\n", "unknown verdict"),
        ("- naive\n", "expected target -> experiment -> verdict"),
        ("naive: SILENT_CORRUPTION\n", "naive: expected experiment -> verdict"),
        ("naive: [unclosed\n", "expected.yaml"),
    ],
)
def test_an_expectations_file_it_cannot_use_is_refused(
    text: str, message: str, tmp_path: Path
) -> None:
    with pytest.raises(ExpectationError, match=message):
        load_expected(_expected(tmp_path, text))


def test_the_repositorys_expectations_cover_every_experiment_on_both_editions() -> None:
    root = Path(__file__).resolve().parents[2]
    expected = load_expected(root / "scorecard" / "expected.yaml")
    experiments = {p.stem for p in (root / "experiments").glob("*.yaml")}

    assert set(expected) == {"reference-naive", "reference-hardened"}
    for cells in expected.values():
        assert set(cells) == experiments
    assert expected["reference-naive"]["backfill"] == ("BROKEN", "SILENT_CORRUPTION")


def test_columns_follow_the_runs_order_and_any_other_target_comes_after() -> None:
    records = [result_record(r) for r in MATRIX]

    assert Scorecard.from_records("r", records).targets == ("hardened", "naive")
    assert Scorecard.from_records("r", records, targets=("naive",)).targets == ("naive", "hardened")
    assert Scorecard.from_records("r", records, targets=("x", "naive")).targets == (
        "naive",
        "hardened",
    )
