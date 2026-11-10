"""Dagster GraphQL client: launch a partitioned run and wait for its outcome.

This is how production tooling talks to Dagster, and it is the only coupling
faultline has with a Dagster-based platform: no Dagster import, just HTTP.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

TERMINAL_STATUSES = frozenset({"SUCCESS", "FAILURE", "CANCELED"})

_VERSION = "query Version { version }"

_LOCATIONS = """
query Locations {
  workspaceOrError {
    __typename
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        locationOrLoadError { __typename ... on PythonError { message } }
      }
    }
    ... on Error { message }
  }
}
"""

_LAUNCH_RUN = """
mutation LaunchRun($executionParams: ExecutionParams!) {
  launchRun(executionParams: $executionParams) {
    __typename
    ... on LaunchRunSuccess { run { runId } }
    ... on RunConfigValidationInvalid { errors { message } }
    ... on Error { message }
  }
}
"""

_RUN_STATUS = """
query RunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    __typename
    ... on Run { runId status }
    ... on Error { message }
  }
}
"""

_RUN_ERRORS = """
query RunErrors($runId: ID!) {
  logsForRun(runId: $runId, limit: 1000) {
    __typename
    ... on EventConnection {
      events {
        __typename
        ... on ExecutionStepFailureEvent {
          stepKey
          error { message errorChain { error { message } } }
        }
        ... on RunFailureEvent { message }
      }
    }
    ... on Error { message }
  }
}
"""


class DagsterError(Exception):
    """The GraphQL call itself failed (transport error, schema error, rejected launch)."""


class RunTimeoutError(DagsterError):
    """The run did not reach a terminal status in time."""


@dataclass(frozen=True)
class RunOutcome:
    run_id: str
    status: str
    errors: tuple[str, ...] = ()

    @property
    def succeeded(self) -> bool:
        return self.status == "SUCCESS"


class DagsterClient:
    def __init__(
        self,
        url: str = "http://127.0.0.1:3000/graphql",
        *,
        location: str,
        repository: str = "__repository__",
        timeout: float = 30.0,
    ) -> None:
        self.url = url
        self.location = location
        self.repository = repository
        self.timeout = timeout

    # ---------------------------------------------------------------- transport

    def execute(self, query: str, variables: Mapping[str, Any] | None = None) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": dict(variables or {})}).encode()
        request = urllib.request.Request(
            self.url, data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                payload = json.loads(response.read())
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            raise DagsterError(f"HTTP {exc.code} from {self.url}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise DagsterError(f"cannot reach Dagster at {self.url}: {exc.reason}") from exc
        if payload.get("errors"):
            messages = "; ".join(e.get("message", str(e)) for e in payload["errors"])
            raise DagsterError(f"GraphQL error: {messages}")
        return payload["data"]

    # ------------------------------------------------------------------ queries

    def version(self) -> str:
        return str(self.execute(_VERSION)["version"])

    def location_error(self) -> str | None:
        """Why the code location failed to load, or None if it loaded fine.

        A code server can be up and healthy while its definitions failed to
        import; jobs then simply "do not exist". Check this before launching.
        """
        result = self.execute(_LOCATIONS)["workspaceOrError"]
        if result["__typename"] != "Workspace":
            return str(result.get("message", result["__typename"]))
        for entry in result["locationEntries"]:
            if entry["name"] != self.location:
                continue
            loaded = entry["locationOrLoadError"]
            if loaded is None:
                return f"location {self.location!r} is {entry['loadStatus'].lower()}"
            if loaded["__typename"] == "PythonError":
                return str(loaded["message"]).strip()
            return None
        return f"no code location named {self.location!r}"

    def launch_run(
        self,
        job: str,
        *,
        partition: str | None = None,
        run_config: Mapping[str, Any] | None = None,
        tags: Mapping[str, str] | None = None,
    ) -> str:
        """Launch `job` and return its run id. Does not wait for completion."""
        all_tags = dict(tags or {})
        if partition is not None:
            all_tags["dagster/partition"] = partition
        params = {
            "selector": {
                "repositoryLocationName": self.location,
                "repositoryName": self.repository,
                "jobName": job,
            },
            "runConfigData": dict(run_config or {}),
            "executionMetadata": {"tags": [{"key": k, "value": v} for k, v in all_tags.items()]},
        }
        result = self.execute(_LAUNCH_RUN, {"executionParams": params})["launchRun"]
        if result["__typename"] == "LaunchRunSuccess":
            return str(result["run"]["runId"])
        details = [e["message"] for e in result.get("errors", [])] or [result.get("message", "")]
        raise DagsterError(f"launch of {job!r} rejected ({result['__typename']}): {details}")

    def run_status(self, run_id: str) -> str:
        result = self.execute(_RUN_STATUS, {"runId": run_id})["runOrError"]
        if result["__typename"] != "Run":
            raise DagsterError(f"run {run_id}: {result.get('message', result['__typename'])}")
        return str(result["status"])

    def run_errors(self, run_id: str) -> tuple[str, ...]:
        """Human readable failure messages of a run, step failures first.

        A step failure is reported with its root cause: Dagster wraps the real
        exception in "Error occurred while executing op ...", which says nothing.
        """
        result = self.execute(_RUN_ERRORS, {"runId": run_id})["logsForRun"]
        if result["__typename"] != "EventConnection":
            raise DagsterError(f"run {run_id}: {result.get('message', result['__typename'])}")
        steps, runs = [], []
        for event in result["events"]:
            if event["__typename"] == "ExecutionStepFailureEvent":
                error = event["error"]
                chain = [link["error"]["message"] for link in error.get("errorChain") or []]
                root = (chain[-1] if chain else error["message"]).strip()
                steps.append(f"{event['stepKey']}: {root}")
            elif event["__typename"] == "RunFailureEvent":
                runs.append(event["message"].strip())
        return tuple(steps + runs)

    def wait_for_run(
        self,
        run_id: str,
        *,
        timeout: float = 600.0,
        poll_interval: float = 1.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> RunOutcome:
        """Poll until the run reaches SUCCESS, FAILURE or CANCELED."""
        deadline = clock() + timeout
        while True:
            status = self.run_status(run_id)
            if status in TERMINAL_STATUSES:
                errors = self.run_errors(run_id) if status != "SUCCESS" else ()
                return RunOutcome(run_id=run_id, status=status, errors=errors)
            if clock() >= deadline:
                raise RunTimeoutError(f"run {run_id} still {status} after {timeout:.0f}s")
            sleep(poll_interval)
