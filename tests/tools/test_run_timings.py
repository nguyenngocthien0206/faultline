"""reference/scripts/run_timings.py on canned GraphQL answers (no Docker)."""

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "reference" / "scripts" / "run_timings.py"
_spec = importlib.util.spec_from_file_location("run_timings", SCRIPT)
assert _spec is not None
assert _spec.loader is not None
rt = importlib.util.module_from_spec(_spec)
sys.modules["run_timings"] = rt
_spec.loader.exec_module(rt)

T = 1_800_000_000.0  # some moment, in epoch seconds


def _raw(run_id: str, created: float, *, job: str = "pos_daily", status: str = "SUCCESS"):
    start, end = created + 30, created + 80
    prefix = "hardened_" if job.endswith("hardened") else ""
    return {
        "runId": run_id,
        "jobName": job,
        "status": status,
        "creationTime": created,
        "startTime": start,
        "endTime": end,
        "stepStats": [
            {"stepKey": f"{prefix}pos_landing", "startTime": start, "endTime": start + 5},
            {"stepKey": f"{prefix}_bronze_orders", "startTime": start + 5, "endTime": start + 6},
            {"stepKey": f"{prefix}_bronze_payments", "startTime": start + 6, "endTime": start + 8},
            {"stepKey": f"{prefix}pos_dbt", "startTime": start + 8, "endTime": start + 48},
        ],
    }


def _events(created: float) -> list[dict[str, Any]]:
    def event(kind: str, at: float, message: str = "") -> dict[str, Any]:
        return {"__typename": kind, "timestamp": str(int(at * 1000)), "message": message}

    return [
        event("AssetMaterializationPlannedEvent", created, "intends to materialize"),
        event("RunEnqueuedEvent", created),
        event("RunStartingEvent", created + 10),  # 10 s in the queue
        event("EngineEvent", created + 18, "Started process for run (pid: 1)."),  # 8 s spawn
        event("RunStartEvent", created + 30, "Started execution of run"),  # 12 s imports
        event("EngineEvent", created + 30, "Executing steps in process (pid: 1)"),
    ]


class FakeDagster:
    """Answers the two queries the script asks, a page of `page` runs at a time."""

    def __init__(self, runs: list[dict[str, Any]], page: int = 2) -> None:
        self.runs = sorted(runs, key=lambda r: -r["creationTime"])  # newest first, as Dagster
        self.page = page
        self.calls: list[dict[str, Any]] = []

    def __call__(self, query: str, variables: dict[str, Any]) -> dict[str, Any]:
        self.calls.append(variables)
        if "logsForRun" in query:
            run = next(r for r in self.runs if r["runId"] == variables["runId"])
            return {
                "logsForRun": {
                    "__typename": "EventConnection",
                    "events": _events(run["creationTime"]),
                }
            }
        window = variables["filter"]
        chosen = [
            r
            for r in self.runs
            if window["createdAfter"] <= r["creationTime"] < window["createdBefore"]
        ]
        if variables["cursor"]:
            at = next(i for i, r in enumerate(chosen) if r["runId"] == variables["cursor"])
            chosen = chosen[at + 1 :]
        return {"runsOrError": {"__typename": "Runs", "results": chosen[: self.page]}}


@pytest.fixture(autouse=True)
def small_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(rt, "PAGE", 2)


def test_every_run_of_the_window_is_read_across_pages_oldest_first() -> None:
    dagster = FakeDagster([_raw(f"r{i}", T + 10 * i) for i in range(5)] + [_raw("late", T + 999)])

    runs = rt.fetch(dagster, T, T + 100)

    assert [r.run_id for r in runs] == ["r0", "r1", "r2", "r3", "r4"]
    assert sum(1 for c in dagster.calls if "filter" in c) == 3  # pages of 2, 2, 1


def test_a_runs_life_splits_into_its_phases_and_step_kinds() -> None:
    (run,) = rt.fetch(FakeDagster([_raw("r", T)]), T, T + 1)

    assert run.phases() == {"queue": 10.0, "launch": 8.0, "load": 12.0, "execute": 50.0}
    assert run.steps == {"landing": 5.0, "bronze": 3.0, "dbt": 40.0}


def test_hardened_steps_count_as_the_same_kinds() -> None:
    (run,) = rt.fetch(FakeDagster([_raw("h", T, job="pos_daily_hardened")]), T, T + 1)

    assert run.steps == {"landing": 5.0, "bronze": 3.0, "dbt": 40.0}


def test_a_phase_without_its_events_is_left_out() -> None:
    run = rt.Run("r", "pos_daily", "FAILURE", created=T, started=None, ended=T + 5)

    assert run.phases() == {}


def test_the_summary_has_spreads_concurrency_and_rate() -> None:
    # Four runs 25 s apart, each executing 50 s: two at once most of the time.
    runs = rt.fetch(FakeDagster([_raw(f"r{i}", T + 25 * i) for i in range(4)]), T, T + 1000)

    summary = rt.summarize(runs)

    assert summary["runs"] == 4
    assert summary["statuses"] == {"SUCCESS": 4}
    assert summary["wall_clock_s"] == 155.0  # first created to last ended
    assert summary["phases"]["load"] == {"median": 12.0, "p90": 12.0, "n": 4}
    assert summary["steps"]["dbt"]["median"] == 40.0
    assert summary["concurrency"] == {"mean": 200 / 125, "peak": 2}
    assert summary["runs_per_minute"] == pytest.approx(4 / (155 / 60))


def test_the_report_reads_as_a_table() -> None:
    runs = rt.fetch(FakeDagster([_raw("a", T), _raw("b", T + 5, status="FAILURE")]), T, T + 10)

    text = rt.render(rt.summarize(runs))

    assert text.splitlines()[0] == "runs        2 (1 SUCCESS, 1 FAILURE)"
    assert "executing   1.8 at once on average, 2 at most" in text  # 100 s busy over 55
    assert "load           12.0s   12.0s  importing the code" in text
    assert "  dbt          40.0s   40.0s  inside its steps" in text
    text.encode("ascii")
    assert rt.render(rt.summarize([])) == "no runs in that window"


def test_a_window_is_the_last_n_minutes_or_two_local_times() -> None:
    now = T

    assert rt.parse_window(None, None, "30m", now) == (now - 1800, now)
    assert rt.parse_window(None, None, "2h", now) == (now - 7200, now)
    since = datetime(2026, 11, 27, 8, 23).timestamp()
    until = datetime(2026, 11, 27, 8, 50).timestamp()
    assert rt.parse_window("2026-11-27T08:23", "2026-11-27T08:50", None, now) == (since, until)


@pytest.mark.parametrize(
    ("since", "last", "message"),
    [
        ("2026-11-27T08:23", "30m", "not both"),
        (None, None, "give --last or --since"),
        (None, "30 minutes", "--last takes"),
        ("yesterday", None, "not a date and time"),
    ],
)
def test_a_window_it_cannot_read_is_refused(
    since: str | None, last: str | None, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        rt.parse_window(since, None, last, T)


def test_the_command_prints_the_report(capsys: pytest.CaptureFixture[str]) -> None:
    dagster = FakeDagster([_raw("r", T)])

    window = ["--since", "2020-01-01T00:00", "--until", "2030-01-01T00:00"]
    assert rt.main([*window, "--format", "json"], post=dagster) == 0
    assert '"runs": 1' in capsys.readouterr().out
