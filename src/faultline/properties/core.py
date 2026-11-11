"""The first properties: nothing lost, nothing silently different."""

from __future__ import annotations

from typing import ClassVar

from faultline.oracle.diff import diff
from faultline.oracle.invariants import complete_keys
from faultline.properties.base import Checkpoint, Property, PropertyContext, PropertyResult


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
