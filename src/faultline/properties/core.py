"""The core properties: nothing lost, nothing silently different, nothing done twice."""

from __future__ import annotations

from datetime import date
from typing import ClassVar

from faultline.adapters.base import Capability
from faultline.oracle.diff import diff
from faultline.oracle.invariants import complete_keys
from faultline.properties.base import Checkpoint, Property, PropertyContext, PropertyResult
from faultline.schedule import Backfill, Run, Schedule, Snapshot


class NoDataLoss(Property):
    """Every record that entered the system is still there.

    Tables fed straight from a generated dataset are checked against the
    generator's own keys (absolute truth). Derived tables, such as gold
    aggregates, are checked against the baseline: a key it has must not vanish.
    """

    name: ClassVar[str] = "no_data_loss"

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult:
        evidence: list[str] = []
        for contract in ctx.contracts:
            actual = checkpoint.snapshots[contract.name]
            if contract.truth_dataset:
                result = complete_keys(actual, contract, ctx.truth_root)
                if not result.holds:
                    evidence.append(result.summary())
            else:
                d = diff(ctx.baseline[contract.name], actual, contract)
                if missing := d.counts["missing"]:
                    examples = ", ".join(d.samples["missing"])
                    evidence.append(
                        f"{contract.name}: {missing:,} keys of the baseline missing, "
                        f"e.g. {examples}"
                    )
        return PropertyResult(self.name, checkpoint.name, not evidence, tuple(evidence))


class NoSilentCorruption(Property):
    """Every consumer-visible table is exactly what a clean run produces.

    The catch-all: missing, extra, duplicated or changed rows all count. Whether
    a violation is silent or was signalled is decided by the outcome classifier,
    which also looks at the runs.
    """

    name: ClassVar[str] = "no_silent_corruption"

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult:
        evidence = []
        for contract in ctx.contracts:
            d = diff(ctx.baseline[contract.name], checkpoint.snapshots[contract.name], contract)
            if not d.is_empty:
                evidence.append(d.summary())
        return PropertyResult(self.name, checkpoint.name, not evidence, tuple(evidence))


class NoVanishingRows(Property):
    """A row a reader could see does not disappear while the platform works on it.

    Refreshing a table replaces rows; it never has to take one away first. A
    refresh in two steps (delete, then insert) that dies between them leaves
    readers with rows gone that were there a moment before; one that builds the
    new data aside and swaps it in does not. No baseline is needed, so the
    check holds on a clean run by construction.

    At F it compares the snapshots named `before` and `after` (taken around
    the fault, by Snapshot actions); at R, the end of the fault phase with the
    end of recovery. A row whose values changed has not vanished; one whose
    key is gone has.
    """

    name: ClassVar[str] = "no_vanishing_rows"

    def __init__(self, before: str = "before_crash", after: str = "after_crash") -> None:
        self.before = before
        self.after = after

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult:
        if checkpoint.name == "R":
            first, then = checkpoint.marks["F"], checkpoint.snapshots
        else:
            missing = {self.before, self.after} - set(checkpoint.marks)
            if missing:
                raise ValueError(
                    f"no snapshot named {', '.join(sorted(missing))} at {checkpoint.name}"
                )
            first, then = checkpoint.marks[self.before], checkpoint.marks[self.after]
        evidence = []
        for contract in ctx.contracts:
            d = diff(first[contract.name], then[contract.name], contract)
            if vanished := d.counts["missing"]:
                examples = ", ".join(d.samples["missing"])
                evidence.append(f"{contract.name}: {vanished:,} keys vanished, e.g. {examples}")
        return PropertyResult(self.name, checkpoint.name, not evidence, tuple(evidence))


class Idempotency(Property):
    """Running an already processed day again changes nothing a reader can see.

    The operator's everyday move (after an incident, a fix, or just to be sure)
    must be safe. The probe reruns `rerun`, an old day, once every day has been
    processed, with a snapshot on either side; any difference between the two
    (a row missing, extra, duplicated or changed) breaks the promise. Rerunning
    the last day is the easy case, which is why the probe picks an older one.

    The probe runs in the fault phase's sandbox, so R sees the same snapshots
    and gives the same answer: the state after the rerun is the state readers
    keep.
    """

    name: ClassVar[str] = "idempotency"
    BEFORE: ClassVar[str] = "before_rerun"
    AFTER: ClassVar[str] = "after_rerun"

    def __init__(self, rerun: date) -> None:
        self.rerun = rerun

    def probe(self) -> Schedule:
        return (Snapshot(self.BEFORE), Run(self.rerun), Snapshot(self.AFTER))

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult:
        missing = {self.BEFORE, self.AFTER} - set(checkpoint.marks)
        if missing:
            raise ValueError(f"no snapshot named {', '.join(sorted(missing))} at {checkpoint.name}")
        before, after = checkpoint.marks[self.BEFORE], checkpoint.marks[self.AFTER]
        evidence = []
        for contract in ctx.contracts:
            d = diff(before[contract.name], after[contract.name], contract)
            if not d.is_empty:
                evidence.append(f"rerun of {self.rerun}: {d.summary()}")
        return PropertyResult(self.name, checkpoint.name, not evidence, tuple(evidence))


class BackfillEquivalence(Property):
    """Loading a range of days in one backfill gives what running them day by day gives.

    A team loading its history, or rebuilding after an incident, backfills
    instead of replaying every day. The probe asks the orchestrator for one
    backfill of `start` to `end`, run however it runs backfills (Dagster: one
    run per day, side by side); the tables afterwards must equal the baseline,
    which ran the same days one at a time. Meant for an experiment that only
    delivers the days (`deliveries_only`), so the backfill is the first load.
    """

    name: ClassVar[str] = "backfill_equivalence"
    requires: ClassVar[frozenset[Capability]] = frozenset({Capability.BACKFILL})

    def __init__(self, start: date, end: date) -> None:
        self.start = start
        self.end = end

    def probe(self) -> Schedule:
        return (Backfill(self.start, self.end),)

    def check(self, ctx: PropertyContext, checkpoint: Checkpoint) -> PropertyResult:
        evidence = []
        for contract in ctx.contracts:
            d = diff(ctx.baseline[contract.name], checkpoint.snapshots[contract.name], contract)
            if not d.is_empty:
                evidence.append(f"backfill {self.start}..{self.end}: {d.summary()}")
        return PropertyResult(self.name, checkpoint.name, not evidence, tuple(evidence))
