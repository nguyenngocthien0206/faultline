import pytest
from http_stub import StubServer

from faultline.connectors.dagster import (
    DagsterClient,
    DagsterError,
    RunOutcome,
    RunTimeoutError,
)


def client(stub: StubServer) -> DagsterClient:
    return DagsterClient(f"{stub.url}/graphql", location="coffee_platform", timeout=5)


def _status(status: str) -> dict[str, object]:
    return {"data": {"runOrError": {"__typename": "Run", "runId": "r1", "status": status}}}


def test_version(stub: StubServer) -> None:
    stub.reply(200, {"data": {"version": "1.13.29"}})

    assert client(stub).version() == "1.13.29"
    assert stub.requests[0].method == "POST"
    assert stub.requests[0].path == "/graphql"


def _locations(*entries: dict[str, object]) -> dict[str, object]:
    return {
        "data": {"workspaceOrError": {"__typename": "Workspace", "locationEntries": list(entries)}}
    }


def test_location_error_is_none_when_loaded(stub: StubServer) -> None:
    stub.reply(
        200,
        _locations(
            {
                "name": "coffee_platform",
                "loadStatus": "LOADED",
                "locationOrLoadError": {"__typename": "RepositoryLocation"},
            }
        ),
    )

    assert client(stub).location_error() is None


def test_location_error_surfaces_a_definitions_import_failure(stub: StubServer) -> None:
    # The code server is healthy, but its definitions did not import.
    stub.reply(
        200,
        _locations(
            {
                "name": "coffee_platform",
                "loadStatus": "LOADED",
                "locationOrLoadError": {
                    "__typename": "PythonError",
                    "message": "DagsterInvalidDefinitionError: bad annotation\n",
                },
            }
        ),
    )

    assert client(stub).location_error() == "DagsterInvalidDefinitionError: bad annotation"


def test_location_error_when_the_location_is_missing(stub: StubServer) -> None:
    stub.reply(200, _locations())

    assert client(stub).location_error() == "no code location named 'coffee_platform'"


def test_launch_run_sends_selector_partition_tag_and_run_config(stub: StubServer) -> None:
    stub.reply(
        200, {"data": {"launchRun": {"__typename": "LaunchRunSuccess", "run": {"runId": "r1"}}}}
    )

    run_id = client(stub).launch_run(
        "smoke_job",
        partition="2026-11-01",
        run_config={"resources": {"workspace": {"config": {"name": "fl_abcd1234"}}}},
        tags={"faultline/experiment": "smoke"},
    )

    assert run_id == "r1"
    params = stub.requests[0].json()["variables"]["executionParams"]
    assert params["selector"] == {
        "repositoryLocationName": "coffee_platform",
        "repositoryName": "__repository__",
        "jobName": "smoke_job",
    }
    assert params["runConfigData"]["resources"]["workspace"]["config"]["name"] == "fl_abcd1234"
    assert {"key": "dagster/partition", "value": "2026-11-01"} in params["executionMetadata"][
        "tags"
    ]
    assert {"key": "faultline/experiment", "value": "smoke"} in params["executionMetadata"]["tags"]


def test_rejected_launch_raises_with_the_validation_messages(stub: StubServer) -> None:
    stub.reply(
        200,
        {
            "data": {
                "launchRun": {
                    "__typename": "RunConfigValidationInvalid",
                    "errors": [{"message": "Missing required config entry workspace"}],
                }
            }
        },
    )

    with pytest.raises(DagsterError, match="Missing required config entry workspace"):
        client(stub).launch_run("smoke_job")


def test_graphql_errors_raise(stub: StubServer) -> None:
    stub.reply(200, {"errors": [{"message": "Cannot query field 'nope'"}]})

    with pytest.raises(DagsterError, match="Cannot query field"):
        client(stub).version()


def test_http_errors_raise_with_the_body(stub: StubServer) -> None:
    stub.reply(502, "bad gateway")

    with pytest.raises(DagsterError, match=r"HTTP 502.*bad gateway"):
        client(stub).version()


def test_unreachable_server_raises() -> None:
    dead = DagsterClient("http://127.0.0.1:9/graphql", location="x", timeout=1)

    with pytest.raises(DagsterError, match="cannot reach Dagster"):
        dead.version()


def test_wait_polls_until_success_without_fetching_errors(stub: StubServer) -> None:
    for status in ("QUEUED", "STARTING", "STARTED", "SUCCESS"):
        stub.reply(200, _status(status))
    sleeps: list[float] = []

    outcome = client(stub).wait_for_run("r1", poll_interval=0.5, sleep=sleeps.append)

    assert outcome == RunOutcome(run_id="r1", status="SUCCESS")
    assert outcome.succeeded
    assert sleeps == [0.5, 0.5, 0.5]
    assert len(stub.requests) == 4


def test_wait_returns_step_and_run_failure_messages(stub: StubServer) -> None:
    stub.reply(200, _status("FAILURE"))
    stub.reply(
        200,
        {
            "data": {
                "logsForRun": {
                    "__typename": "EventConnection",
                    "events": [
                        {"__typename": "RunStartEvent"},
                        {
                            "__typename": "ExecutionStepFailureEvent",
                            "stepKey": "smoke_ping",
                            "error": {
                                "message": 'Error occurred while executing op "smoke_ping":',
                                "errorChain": [
                                    {"error": {"message": "PipelineStepFailed: extract"}},
                                    {"error": {"message": "relation does not exist\n"}},
                                ],
                            },
                        },
                        {
                            "__typename": "ExecutionStepFailureEvent",
                            "stepKey": "other",
                            "error": {"message": "plain failure", "errorChain": []},
                        },
                        {"__typename": "RunFailureEvent", "message": "Run failed."},
                    ],
                }
            }
        },
    )

    outcome = client(stub).wait_for_run("r1", sleep=lambda _: None)

    assert not outcome.succeeded
    # Root cause, not Dagster's generic wrapper.
    assert outcome.errors == (
        "smoke_ping: relation does not exist",
        "other: plain failure",
        "Run failed.",
    )


def test_wait_times_out(stub: StubServer) -> None:
    for _ in range(3):
        stub.reply(200, _status("STARTED"))
    ticks = iter([0.0, 5.0, 11.0])

    with pytest.raises(RunTimeoutError, match="still STARTED"):
        client(stub).wait_for_run("r1", timeout=10, sleep=lambda _: None, clock=lambda: next(ticks))


def test_unknown_run_raises(stub: StubServer) -> None:
    stub.reply(
        200,
        {"data": {"runOrError": {"__typename": "RunNotFoundError", "message": "Run r9 not found"}}},
    )

    with pytest.raises(DagsterError, match="Run r9 not found"):
        client(stub).run_status("r9")
