"""Dagster GraphQL client: launch a partitioned run and wait for its outcome.

This is how production tooling talks to Dagster, and it is the only coupling
faultline has with a Dagster-based platform: no Dagster import, just HTTP.

A busy webserver can be slow to answer while the run itself is fine. Waiting
rides that out: reading a run's status is safe to repeat. Launching is not (a
launch that timed out may have started the run anyway), so every launch
carries a unique tag, and a launch that got no answer first looks for its run
by that tag before trying again. Giving up early would be worse than slow: the
caller would tear its sandbox down under a run that is still going.
"""

from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar

T = TypeVar("T")
LAUNCH_TAG = "faultline/launch-id"

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

_RUN_MESSAGES = """
query RunMessages($runId: ID!) {
  logsForRun(runId: $runId, limit: 1000) {
    __typename
    ... on EventConnection { events { __typename ... on MessageEvent { message } } }
    ... on Error { message }
  }
}
"""

_FIND_RUN = """
query FindRun($tags: [ExecutionTag!]) {
  runsOrError(filter: {tags: $tags}, limit: 1) {
    __typename
    ... on Runs { results { runId } }
    ... on Error { message }
  }
}
"""

_LATEST_RUN = """
query LatestRun($statuses: [RunStatus!]) {
  runsOrError(filter: {statuses: $statuses}, limit: 1) {
    __typename
    ... on Runs { results { runId status updateTime } }
    ... on Error { message }
  }
}
"""

# Tags Dagster's run retries speak: a run asks for them, a retry names its parent.
MAX_RETRIES_TAG = "dagster/max_retries"
RETRY_STRATEGY_TAG = "dagster/retry_strategy"
PARENT_RUN_TAG = "dagster/parent_run_id"

ACTIVE_STATUSES = ("QUEUED", "NOT_STARTED", "STARTING", "STARTED", "CANCELING")

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


class DagsterUnavailable(DagsterError):
    """No answer in time, or no connection: the same request may succeed later."""


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
            raise DagsterUnavailable(f"cannot reach Dagster at {self.url}: {exc.reason}") from exc
        except (TimeoutError, ConnectionError) as exc:  # a read timeout is not wrapped in URLError
            raise DagsterUnavailable(
                f"no answer from Dagster at {self.url} within {self.timeout:.0f}s ({exc})"
            ) from exc
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
        attempts: int = 3,
        sleep: Callable[[float], None] = time.sleep,
    ) -> str:
        """Launch `job` once and return its run id. Does not wait for completion.

        Once, even when Dagster is slow: an unanswered launch is looked up by
        its launch tag, and only launched again if no run carries that tag.
        """
        launch_id = uuid.uuid4().hex
        all_tags = {**(tags or {}), LAUNCH_TAG: launch_id}
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
        for attempt in range(1, attempts + 1):
            try:
                result = self.execute(_LAUNCH_RUN, {"executionParams": params})["launchRun"]
                break
            except DagsterUnavailable:
                launched = self._find_launched(launch_id, sleep=sleep)
                if launched is not None:
                    return launched
                if attempt == attempts:
                    raise
                sleep(1.0)
        if result["__typename"] == "LaunchRunSuccess":
            return str(result["run"]["runId"])
        details = [e["message"] for e in result.get("errors", [])] or [result.get("message", "")]
        raise DagsterError(f"launch of {job!r} rejected ({result['__typename']}): {details}")

    def _find_launched(
        self, launch_id: str, *, sleep: Callable[[float], None], tries: int = 10
    ) -> str | None:
        """The run a launch started, if it did; None if no run carries the launch tag.

        A retry inherits the launch tag too, but it only exists once the run it
        retries has failed, long after its launch was answered or looked up.
        """
        return self.find_run({LAUNCH_TAG: launch_id}, sleep=sleep, tries=tries)

    def find_run(
        self,
        tags: Mapping[str, str],
        *,
        sleep: Callable[[float], None] = time.sleep,
        tries: int = 10,
    ) -> str | None:
        """The latest run carrying every one of `tags`, or None."""
        variables = {"tags": [{"key": k, "value": v} for k, v in tags.items()]}
        for attempt in range(1, tries + 1):
            try:
                result = self.execute(_FIND_RUN, variables)["runsOrError"]
            except DagsterUnavailable:
                if attempt == tries:
                    raise
                sleep(1.0)
                continue
            if result["__typename"] != "Runs":
                raise DagsterError(f"cannot look up runs: {result.get('message')}")
            found = result["results"]
            return str(found[0]["runId"]) if found else None
        return None  # pragma: no cover - the loop always returns or raises

    def wait_for_retry(
        self,
        run_id: str,
        *,
        timeout: float = 120.0,
        poll_interval: float = 2.0,
        sleep: Callable[[float], None] = time.sleep,
        clock: Callable[[], float] = time.monotonic,
    ) -> str:
        """The run the daemon launched to retry `run_id`, once it exists.

        Dagster's daemon notices a failed run that asked for retries and
        launches a re-execution tagged with its parent: measured 4 to 12 s on
        an idle stack, so the default leaves room for a busy one.
        """
        deadline = clock() + timeout
        while True:
            try:
                retry = self.find_run({PARENT_RUN_TAG: run_id}, sleep=sleep, tries=1)
            except DagsterUnavailable:
                retry = None
            if retry is not None:
                return retry
            if clock() >= deadline:
                raise RunTimeoutError(
                    f"run {run_id} asked for a retry and none started within {timeout:.0f}s "
                    "(is run_retries enabled on the Dagster instance?)"
                )
            sleep(poll_interval)

    def activity(self) -> tuple[bool, float | None]:
        """Whether any run is in progress, and when the latest run last changed (epoch s)."""

        def latest(statuses: list[str] | None) -> dict[str, Any] | None:
            result = self.execute(_LATEST_RUN, {"statuses": statuses})["runsOrError"]
            if result["__typename"] != "Runs":
                raise DagsterError(f"cannot list runs: {result.get('message')}")
            return result["results"][0] if result["results"] else None

        active = latest(list(ACTIVE_STATUSES)) is not None
        last = latest(None)
        return active, (float(last["updateTime"]) if last and last["updateTime"] else None)

    def run_status(self, run_id: str) -> str:
        result = self.execute(_RUN_STATUS, {"runId": run_id})["runOrError"]
        if result["__typename"] != "Run":
            raise DagsterError(f"run {run_id}: {result.get('message', result['__typename'])}")
        return str(result["status"])

    def run_logged(self, run_id: str, text: str, *, tries: int = 10) -> bool:
        """Whether any message in the run's event log contains `text`.

        Asked right after a run ends, when the webserver may be busy: an
        unanswered request is tried again a few times.
        """
        for attempt in range(1, tries + 1):
            try:
                result = self.execute(_RUN_MESSAGES, {"runId": run_id})["logsForRun"]
                break
            except DagsterUnavailable:
                if attempt == tries:
                    raise
                time.sleep(1.0)
        if result["__typename"] != "EventConnection":
            raise DagsterError(f"run {run_id}: {result.get('message', result['__typename'])}")
        return any(text in (event.get("message") or "") for event in result["events"])

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
        """Poll until the run reaches SUCCESS, FAILURE or CANCELED.

        A poll Dagster does not answer is tried again until the deadline: the
        run goes on regardless, and reading its status twice is harmless.
        """
        deadline = clock() + timeout

        def patiently(call: Callable[[], T]) -> T:
            while True:
                try:
                    return call()
                except DagsterUnavailable as exc:
                    if clock() >= deadline:
                        raise RunTimeoutError(
                            f"run {run_id}: Dagster stopped answering ({exc})"
                        ) from exc
                    sleep(poll_interval)

        while True:
            status = patiently(lambda: self.run_status(run_id))
            if status in TERMINAL_STATUSES:
                errors = patiently(lambda: self.run_errors(run_id)) if status != "SUCCESS" else ()
                return RunOutcome(run_id=run_id, status=status, errors=errors)
            if clock() >= deadline:
                raise RunTimeoutError(f"run {run_id} still {status} after {timeout:.0f}s")
            sleep(poll_interval)
