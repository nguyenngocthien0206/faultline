"""Result files on disk: what they hold and how a run is found again."""

import json
from datetime import datetime
from pathlib import Path

from report_helpers import MATRIX, TINY, converging

from faultline import __version__
from faultline.outcome import Outcome
from faultline.report.results import (
    latest_run,
    load_results,
    new_run_id,
    result_file,
    result_record,
    save_result,
)


def test_a_result_round_trips_through_its_json_file(tmp_path: Path) -> None:
    result = MATRIX[0]

    path = save_result(result, tmp_path / "run", dataset=TINY, seconds=12.345)
    (record,) = load_results(tmp_path / "run")

    assert path == result_file(tmp_path / "run", "late-file", "naive")
    assert path.name == "late-file__naive.json"
    assert record == json.loads(path.read_text(encoding="utf-8"))
    assert record["outcome"] == "SILENT_CORRUPTION"
    assert record["properties"][0]["at_f"] == {
        "holds": False,
        "evidence": ["lines: 163 missing keys"],
    }
    assert record["runs"]["fault phase"][0] == {
        "partition": "2026-11-02",
        "status": "SUCCEEDED",
        "errors": [],
        "run_id": "r1",
        "metadata": {},
    }
    assert record["dataset"]["profile"] == "tiny"
    assert (record["seconds"], record["faultline"]) == (12.3, __version__)
    assert record["snapshots"] == "snaps"


def test_results_load_sorted_and_only_result_files(tmp_path: Path) -> None:
    for result in MATRIX:
        save_result(result, tmp_path)
    (tmp_path / "logs").mkdir()
    (tmp_path / "notes.json").write_text("{}", encoding="utf-8")

    records = load_results(tmp_path)

    assert [(r["experiment"], r["target"]) for r in records] == sorted(
        (r.experiment, r.target) for r in MATRIX
    )


def test_run_ids_sort_in_time_and_the_latest_run_is_found(tmp_path: Path) -> None:
    early, late = (
        new_run_id(datetime(2026, 11, 26, 9, 5)),
        new_run_id(datetime(2026, 11, 26, 10, 0)),
    )
    for run_id in (late, early):
        (tmp_path / run_id).mkdir()

    assert early == "20261126-090500"
    assert latest_run(tmp_path) == tmp_path / late
    assert latest_run(tmp_path / "nothing-here") is None


def test_a_result_keeps_the_rounds_recovery_took(tmp_path: Path) -> None:
    save_result(converging("late-file", "naive", Outcome.SILENT_CORRUPTION, 12, 0), tmp_path)
    (record,) = load_results(tmp_path)

    assert record["recovery"] == {
        "max_rounds": 3,
        "converged": True,
        "summary": "converged after 2 rounds (2 runs)",
        "rounds": [
            {"runs": 1, "keys_off": 12, "rows_off": 12, "differences": []},
            {"runs": 1, "keys_off": 0, "rows_off": 0, "differences": []},
        ],
    }
    assert result_record(MATRIX[0])["recovery"] is None
