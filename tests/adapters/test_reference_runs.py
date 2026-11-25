"""How the reference adapter launches a run and reads its outcome, with a fake Dagster."""

from datetime import date
from pathlib import Path

import pytest

from faultline.adapters.base import AdapterError, Capability, RunRequest, RunStatus
from faultline.adapters.reference import (
    FAILPOINT_REACHED,
    FAILPOINT_TAG,
    ReferenceAdapter,
    ReferenceStackConfig,
)
from faultline.connectors.dagster import RunOutcome

CONFIG = ReferenceStackConfig(
    source_dsn="postgresql://u:p@127.0.0.1:5433/db", clickhouse_user="u", clickhouse_password="p"
)
DAY = date(2026, 11, 2)


class FakeDagster:
    """Run r1 ends with `status`; each of `retries` (status, log) is the next attempt."""

    def __init__(
        self, status: str, log: list[str], retries: tuple[tuple[str, list[str]], ...] = ()
    ) -> None:
        self.attempts = [(status, log), *retries]
        self.launched: list[dict[str, str]] = []
        self.retried: list[str] = []

    def launch_run(
        self, job: str, *, partition: str, run_config: object, tags: dict[str, str]
    ) -> str:
        self.launched.append(tags)
        return "r1"

    def _attempt(self, run_id: str) -> tuple[str, list[str]]:
        return self.attempts[int(run_id[1:]) - 1]

    def wait_for_run(self, run_id: str, *, timeout: float) -> RunOutcome:
        status, _ = self._attempt(run_id)
        errors = () if status == "SUCCESS" else (f"{run_id} failed",)
        return RunOutcome(run_id=run_id, status=status, errors=errors)

    def run_logged(self, run_id: str, text: str) -> bool:
        return any(text in line for line in self._attempt(run_id)[1])

    def wait_for_retry(self, run_id: str) -> str:
        self.retried.append(run_id)
        return f"r{int(run_id[1:]) + 1}"

    def run_errors(self, run_id: str) -> tuple[str, ...]:
        return (f"{run_id} failed",)


class FakeBackfills(FakeDagster):
    """A backfill whose runs end with `runs` (partition -> (run id, status))."""

    def __init__(self, runs: dict[str, tuple[str, str]]) -> None:
        super().__init__("SUCCESS", [])
        self.runs = runs
        self.backfills: list[dict[str, object]] = []

    def launch_backfill(
        self, job: str, partitions: list[str], *, run_config: object, tags: dict[str, str]
    ) -> str:
        self.backfills.append(
            {"job": job, "partitions": partitions, "run_config": run_config, "tags": tags}
        )
        return "bf1"

    def wait_for_backfill(self, backfill_id: str, *, timeout: float) -> dict[str, tuple[str, str]]:
        return self.runs


def _adapter(dagster: FakeDagster) -> ReferenceAdapter:
    adapter = ReferenceAdapter(CONFIG, Path("generated"), workspace="fl_0123abcd")
    adapter.dagster = dagster  # type: ignore[assignment]
    return adapter


def test_failpoints_are_offered() -> None:
    assert _adapter(FakeDagster("SUCCESS", [])).capabilities().supports(Capability.FAILPOINTS)


def test_a_failpoint_travels_as_a_run_tag() -> None:
    dagster = FakeDagster("SUCCESS", [])

    _adapter(dagster).run(RunRequest(DAY, failpoint="after_landing"))
    _adapter(dagster).run(RunRequest(DAY))

    assert dagster.launched[0][FAILPOINT_TAG] == "after_landing"
    assert FAILPOINT_TAG not in dagster.launched[1]


def test_a_run_that_died_at_its_failpoint_is_interrupted_not_failed() -> None:
    dagster = FakeDagster("FAILURE", [FAILPOINT_REACHED + "after_landing"])

    result = _adapter(dagster).run(RunRequest(DAY, failpoint="after_landing"))

    assert result.status is RunStatus.INTERRUPTED  # faultline's crash, not a signal


def test_a_run_that_failed_before_its_failpoint_still_failed() -> None:
    # Asked to crash later, it broke first, on its own: that is the platform's failure.
    dagster = FakeDagster("FAILURE", ["extract failed: connection refused"])

    result = _adapter(dagster).run(RunRequest(DAY, failpoint="after_landing"))

    assert result.status is RunStatus.FAILED


def test_a_failed_run_without_a_failpoint_is_never_called_interrupted() -> None:
    dagster = FakeDagster("FAILURE", [FAILPOINT_REACHED + "after_landing"])

    assert _adapter(dagster).run(RunRequest(DAY)).status is RunStatus.FAILED


CRASHED = ("FAILURE", [FAILPOINT_REACHED + "after_bronze_write"])


def test_retries_are_offered_and_asked_for_by_the_run_alone() -> None:
    dagster = FakeDagster("SUCCESS", [])
    adapter = _adapter(dagster)

    adapter.run(RunRequest(DAY, retries=1))
    adapter.run(RunRequest(DAY))

    assert adapter.capabilities().supports(Capability.RETRIES)
    assert dagster.launched[0]["dagster/max_retries"] == "1"
    assert dagster.launched[0]["dagster/retry_strategy"] == "FROM_FAILURE"
    assert "dagster/max_retries" not in dagster.launched[1]  # the instance retries nothing


def test_a_crash_then_a_green_retry_is_a_green_run() -> None:
    dagster = FakeDagster(*CRASHED, retries=(("SUCCESS", []),))

    result = _adapter(dagster).run(RunRequest(DAY, failpoint="after_bronze_write", retries=1))

    assert result.status is RunStatus.SUCCEEDED
    assert result.run_id == "r2"
    assert result.metadata == {
        "attempts": "2",
        "statuses": "INTERRUPTED,SUCCEEDED",
        "root_run_id": "r1",
    }


def test_a_green_first_attempt_waits_for_no_retry() -> None:
    dagster = FakeDagster("SUCCESS", [])

    result = _adapter(dagster).run(RunRequest(DAY, retries=1))

    assert result.status is RunStatus.SUCCEEDED
    assert dagster.retried == []
    assert result.metadata == {}


def test_a_failure_of_the_platforms_own_stays_a_signal_after_a_green_retry() -> None:
    dagster = FakeDagster("FAILURE", ["connection refused"], retries=(("SUCCESS", []),))

    result = _adapter(dagster).run(RunRequest(DAY, retries=1))

    assert result.status is RunStatus.FAILED  # a failure alert fired on the first attempt
    assert result.errors == ("r1 failed",)


def test_a_failed_retry_is_a_failed_run() -> None:
    dagster = FakeDagster(*CRASHED, retries=(("FAILURE", ["disk full"]),))

    result = _adapter(dagster).run(RunRequest(DAY, failpoint="after_bronze_write", retries=1))

    assert result.status is RunStatus.FAILED
    assert result.errors == ("r2 failed",)


def test_retries_stop_at_the_number_asked_for() -> None:
    dagster = FakeDagster(*CRASHED, retries=(("FAILURE", []), ("SUCCESS", [])))

    result = _adapter(dagster).run(RunRequest(DAY, failpoint="after_bronze_write", retries=1))

    assert dagster.retried == ["r1"]
    assert result.status is RunStatus.FAILED


D1, D3 = date(2026, 11, 1), date(2026, 11, 3)


def test_a_backfill_is_dagsters_own_with_this_sandboxs_config_and_tag() -> None:
    dagster = FakeBackfills({"2026-11-01": ("a", "SUCCESS"), "2026-11-02": ("b", "SUCCESS")})
    adapter = _adapter(dagster)

    results = adapter.backfill((D1, DAY))

    assert adapter.capabilities().supports(Capability.BACKFILL)
    (launched,) = dagster.backfills
    assert launched["job"] == "pos_daily"
    assert launched["partitions"] == ["2026-11-01", "2026-11-02"]
    assert launched["run_config"] == adapter.run_config(None)
    assert launched["tags"] == {"faultline/workspace": "fl_0123abcd"}
    assert [(day, r.status, r.run_id) for day, r in results] == [
        (D1, RunStatus.SUCCEEDED, "a"),
        (DAY, RunStatus.SUCCEEDED, "b"),
    ]


def test_a_failed_backfill_run_is_the_platforms_failure_with_its_errors() -> None:
    dagster = FakeBackfills({"2026-11-01": ("a", "SUCCESS"), "2026-11-02": ("b", "FAILURE")})

    (_, first), (_, second) = _adapter(dagster).backfill((D1, DAY))

    assert first.succeeded
    assert second.status is RunStatus.FAILED  # a signal: nothing faultline did
    assert second.errors == ("b failed",)


def test_a_partition_the_backfill_never_ran_is_an_error_not_a_verdict() -> None:
    dagster = FakeBackfills({"2026-11-01": ("a", "SUCCESS")})

    with pytest.raises(AdapterError, match="backfill bf1 started no run for 2026-11-03"):
        _adapter(dagster).backfill((D1, D3))
