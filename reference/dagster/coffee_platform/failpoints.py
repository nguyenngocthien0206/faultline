"""Failpoints: named places where a run can be made to die, for crash experiments.

A run launched with the tag faultline/failpoint=<name> exits hard when it
reaches the failpoint of that name: os._exit, no exception, no cleanup, no
finally block, as an out-of-memory kill or a lost machine would leave it.
Every run without the tag goes straight past. A well known technique
(FoundationDB, TiKV and etcd test themselves this way) that keeps crash
experiments deterministic: the crash happens exactly there, every time.

Just before dying the run logs one line, which is how faultline tells its own
crash from a genuine failure: a run that failed without that line failed for
a reason of its own.

No `from __future__ import annotations` in Dagster code (see smoke.py).
"""

import os

from dagster import AssetExecutionContext

FAILPOINT_TAG = "faultline/failpoint"
REACHED = "faultline failpoint reached: "


def failpoint(context: AssetExecutionContext, name: str) -> None:
    """Die here, hard, if this run was asked to; otherwise do nothing."""
    if context.run.tags.get(FAILPOINT_TAG) != name:
        return
    context.log.error(REACHED + name)
    os._exit(1)
