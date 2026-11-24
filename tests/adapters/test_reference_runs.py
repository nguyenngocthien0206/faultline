"""How the reference adapter launches a run and reads its outcome, with a fake Dagster."""

from datetime import date
from pathlib import Path

from faultline.adapters.base import Capability, RunRequest, RunStatus
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
    def __init__(self, status: str, log: list[str]) -> None:
        self.status = status
        self.log = log
        self.launched: list[dict[str, str]] = []

    def launch_run(
        self, job: str, *, partition: str, run_config: object, tags: dict[str, str]
    ) -> str:
        self.launched.append(tags)
        return "r1"

    def wait_for_run(self, run_id: str, *, timeout: float) -> RunOutcome:
        errors = () if self.status == "SUCCESS" else ("marked as failed from outside",)
        return RunOutcome(run_id=run_id, status=self.status, errors=errors)

    def run_logged(self, run_id: str, text: str) -> bool:
        return any(text in line for line in self.log)


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
