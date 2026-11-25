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

Failpoints inside dbt (gold_mid_write) live in dbt macros (reference/dbt/
macros/failpoint_mid_write.sql): dbt runs in a subprocess, so the macro raises
with the same marker and the dbt asset dies here once it sees it.

No `from __future__ import annotations` in Dagster code (see smoke.py).
"""

import os

from dagster import AssetExecutionContext

FAILPOINT_TAG = "faultline/failpoint"
REACHED = "faultline failpoint reached: "


def armed(context: AssetExecutionContext) -> str | None:
    """The failpoint this run was asked to die at, if any."""
    return context.run.tags.get(FAILPOINT_TAG)


def failpoint(context: AssetExecutionContext, name: str) -> None:
    """Die here, hard, if this run was asked to; otherwise do nothing."""
    if context.run.tags.get(FAILPOINT_TAG) != name:
        return
    context.log.error(REACHED + name)
    os._exit(1)
