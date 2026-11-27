"""Where the reference stack's Dagster runs spend their time, over a window.

    uv run python reference/scripts/run_timings.py --last 30m
    uv run python reference/scripts/run_timings.py --since 2026-11-27T08:23 --until 2026-11-27T08:50
    make ref-timings LAST=30m

For every run created in the window it reads, from Dagster's GraphQL API, the
run's lifecycle events and its step timings, and splits the run's life into:

    queue     enqueued -> STARTING       waiting for one of the queue's slots
    launch    STARTING -> process        the code server spawning a run worker
    load      process -> RUN_START       the worker importing the code (dlt, dbt, Dagster)
    execute   RUN_START -> end           the steps themselves, split below
      landing, bronze, dbt               time inside steps of that kind

and prints the median and the 90th percentile of each, with how many runs
executed at once (mean and peak) and how many finished a minute. Comparing
two windows (a suite on a stack just restarted, the same suite again) shows
which part moves.

It only reads, only from the GraphQL endpoint, and needs nothing but the
standard library: a tool for the stack, not part of faultline.
"""

from __future__ import annotations

import argparse
import json
import re
import statistics
import sys
import time
import urllib.request
from collections import Counter
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

URL = "http://127.0.0.1:3000/graphql"
PAGE = 100
EVENTS = 60  # a run's lifecycle events come first, well within this many

Post = Callable[[str, dict[str, Any]], dict[str, Any]]  # query, variables -> data

RUNS_QUERY = """
query Runs($filter: RunsFilter, $cursor: String, $limit: Int) {
  runsOrError(filter: $filter, cursor: $cursor, limit: $limit) {
    __typename
    ... on Runs {
      results {
        runId jobName status creationTime startTime endTime
        stepStats { stepKey startTime endTime }
      }
    }
    ... on PythonError { message }
  }
}
"""

EVENTS_QUERY = """
query Events($runId: ID!, $limit: Int) {
  logsForRun(runId: $runId, limit: $limit) {
    __typename
    ... on EventConnection { events { __typename ... on MessageEvent { timestamp message } } }
    ... on PythonError { message }
  }
}
"""

PHASES = ("queue", "launch", "load", "execute")
STEP_KINDS = ("landing", "bronze", "dbt")


@dataclass(frozen=True)
class Run:
    run_id: str
    job: str
    status: str
    created: float
    started: float | None = None  # RUN_START
    ended: float | None = None
    enqueued: float | None = None
    starting: float | None = None  # dequeued, handed to the launcher
    process: float | None = None  # the run worker's process started
    steps: dict[str, float] = field(default_factory=dict)  # step kind -> seconds

    def phases(self) -> dict[str, float]:
        """The run's life in phases; a phase whose ends are unknown is left out."""
        bounds = {
            "queue": (self.enqueued, self.starting),
            "launch": (self.starting, self.process),
            "load": (self.process, self.started),
            "execute": (self.started, self.ended),
        }
        return {p: b - a for p, (a, b) in bounds.items() if a is not None and b is not None}


# ------------------------------------------------------------------- reading


def post_to(url: str, timeout: float = 120.0) -> Post:
    def post(query: str, variables: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps({"query": query, "variables": variables}).encode("utf-8")
        request = urllib.request.Request(
            url, data=body, headers={"Content-Type": "application/json"}
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            answer = json.load(response)
        if answer.get("errors"):
            raise RuntimeError(f"Dagster GraphQL error: {answer['errors'][0].get('message')}")
        return answer["data"]

    return post


def fetch(post: Post, after: float, before: float) -> list[Run]:
    """Every run created in [after, before), oldest first, with its lifecycle events."""
    raw: list[dict[str, Any]] = []
    cursor = None
    while True:
        variables = {
            "filter": {"createdAfter": after, "createdBefore": before},
            "cursor": cursor,
            "limit": PAGE,
        }
        page = post(RUNS_QUERY, variables)["runsOrError"]
        if page["__typename"] != "Runs":
            raise RuntimeError(f"Dagster could not list runs: {page.get('message')}")
        raw += page["results"]
        if len(page["results"]) < PAGE:
            break
        cursor = page["results"][-1]["runId"]
    runs = [_run(r, _events(post, r["runId"])) for r in raw]
    return sorted(runs, key=lambda r: r.created)


def _events(post: Post, run_id: str) -> list[dict[str, Any]]:
    answer = post(EVENTS_QUERY, {"runId": run_id, "limit": EVENTS})["logsForRun"]
    return answer.get("events", []) if answer["__typename"] == "EventConnection" else []


def _run(raw: dict[str, Any], events: list[dict[str, Any]]) -> Run:
    def first(kind: str, starts: str = "") -> float | None:
        for e in events:
            if e["__typename"] == kind and (e.get("message") or "").startswith(starts):
                return float(e["timestamp"]) / 1000  # milliseconds, as a string
        return None

    steps: Counter[str] = Counter()
    for step in raw.get("stepStats") or []:
        kind = step_kind(step["stepKey"])
        if kind and step["startTime"] is not None and step["endTime"] is not None:
            steps[kind] += step["endTime"] - step["startTime"]
    return Run(
        run_id=raw["runId"],
        job=raw["jobName"],
        status=raw["status"],
        created=raw["creationTime"],
        started=raw["startTime"],
        ended=raw["endTime"],
        enqueued=first("RunEnqueuedEvent"),
        starting=first("RunStartingEvent"),
        process=first("EngineEvent", "Started process for run"),
        steps=dict(steps),
    )


def step_kind(step_key: str) -> str | None:
    """landing, bronze or dbt, whichever edition the step belongs to."""
    for kind in STEP_KINDS:
        if kind in step_key:
            return kind
    return None


# ------------------------------------------------------------------ summing up


def summarize(runs: Sequence[Run]) -> dict[str, Any]:
    if not runs:
        return {"runs": 0}
    executed = [(r.started, r.ended) for r in runs if r.started and r.ended]
    finished = [r.ended for r in runs if r.ended]
    first, last = min(r.created for r in runs), max(finished or [r.created for r in runs])
    summary: dict[str, Any] = {
        "runs": len(runs),
        "statuses": dict(Counter(r.status for r in runs).most_common()),
        "first_created": first,
        "last_ended": last,
        "wall_clock_s": last - first,
        "runs_per_minute": len(finished) / ((last - first) / 60) if last > first else None,
        "concurrency": _concurrency(executed),
        "phases": {},
        "steps": {},
    }
    for phase in PHASES:
        values = [p[phase] for r in runs if phase in (p := r.phases())]
        if values:
            summary["phases"][phase] = _spread(values)
    for kind in STEP_KINDS:
        values = [r.steps[kind] for r in runs if kind in r.steps]
        if values:
            summary["steps"][kind] = _spread(values)
    return summary


def _spread(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    p90 = ordered[min(len(ordered) - 1, round(0.9 * (len(ordered) - 1)))]
    return {"median": statistics.median(ordered), "p90": p90, "n": len(ordered)}


def _concurrency(intervals: list[tuple[float, float]]) -> dict[str, float] | None:
    """How many runs executed at once: mean over the busy span, and the peak."""
    if not intervals:
        return None
    span = max(e for _, e in intervals) - min(s for s, _ in intervals)
    edges = sorted([(s, 1) for s, _ in intervals] + [(e, -1) for _, e in intervals])
    running = peak = 0
    for _, step in edges:
        running += step
        peak = max(peak, running)
    busy = sum(e - s for s, e in intervals)
    return {"mean": busy / span if span else float(len(intervals)), "peak": peak}


def render(summary: dict[str, Any]) -> str:
    if not summary["runs"]:
        return "no runs in that window"
    statuses = ", ".join(f"{n} {s}" for s, n in summary["statuses"].items())
    rate = summary["runs_per_minute"]
    lines = [
        f"runs        {summary['runs']} ({statuses})",
        f"window      {_clock(summary['first_created'])} .. {_clock(summary['last_ended'])}, "
        f"{summary['wall_clock_s'] / 60:.1f} min" + (f", {rate:.1f} runs/min" if rate else ""),
    ]
    if summary["concurrency"]:
        c = summary["concurrency"]
        lines.append(f"executing   {c['mean']:.1f} at once on average, {c['peak']} at most")
    lines.append(f"{'':12}{'median':>8}{'p90':>8}")
    notes = {
        "queue": "waiting for a slot",
        "launch": "spawning the run worker",
        "load": "importing the code",
        "execute": "running the steps",
    }
    for phase, spread in summary["phases"].items():
        lines.append(_row(phase, spread, notes[phase]))
    for kind, spread in summary["steps"].items():
        lines.append(_row(f"  {kind}", spread, "inside its steps"))
    return "\n".join(lines)


def _row(name: str, spread: dict[str, float], note: str) -> str:
    return f"{name:<12}{spread['median']:>7.1f}s{spread['p90']:>7.1f}s  {note}"


def _clock(epoch: float) -> str:
    return datetime.fromtimestamp(epoch).strftime("%Y-%m-%d %H:%M:%S")


# ------------------------------------------------------------------- the CLI


def parse_window(
    since: str | None, until: str | None, last: str | None, now: float
) -> tuple[float, float]:
    """[after, before) in epoch seconds; times are local, like the timestamps printed."""
    if last and since:
        raise ValueError("give --last or --since, not both")
    before = _local(until) if until else now
    if last:
        match = re.fullmatch(r"(\d+)([mhd])", last)
        if not match:
            raise ValueError(f"--last takes a number and m, h or d (30m, 2h, 1d); got {last!r}")
        seconds = int(match[1]) * {"m": 60, "h": 3600, "d": 86400}[match[2]]
        return before - seconds, before
    if not since:
        raise ValueError("give --last or --since")
    return _local(since), before


def _local(text: str) -> float:
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError as exc:
        raise ValueError(f"not a date and time: {text!r} (2026-11-27T08:23)") from exc


def main(argv: Sequence[str] | None = None, post: Post | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--last", help="the last 30m, 2h or 1d")
    parser.add_argument("--since", help="local date and time, 2026-11-27T08:23")
    parser.add_argument("--until", help="local date and time (default: now)")
    parser.add_argument("--url", default=URL)
    parser.add_argument("--format", choices=("text", "json"), default="text")
    args = parser.parse_args(argv)
    try:
        after, before = parse_window(args.since, args.until, args.last, time.time())
    except ValueError as exc:
        parser.error(str(exc))
    summary = summarize(fetch(post or post_to(args.url), after, before))
    print(json.dumps(summary, indent=2) if args.format == "json" else render(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
