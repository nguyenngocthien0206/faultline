"""The scorecard: every experiment by every target, one verdict per cell.

It answers the question a data team actually has ("what does our platform
survive, and where does it go wrong without anyone noticing?") in one table,
with one line of evidence for every cell that is not a PASS. Markdown for
people and pull requests, JSON for CI.

Checked against a file of expected verdicts (scorecard/expected.yaml) it is a
regression test for the platform: a cell that moves is a change in how the
platform behaves under a fault, wanted or not.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from faultline.outcome import Outcome

Record = dict[str, Any]  # one result file, as faultline.report.results reads it
Expected = dict[str, dict[str, tuple[str, ...]]]  # target -> experiment -> allowed verdicts


@dataclass(frozen=True)
class Scorecard:
    run_id: str
    experiments: tuple[str, ...]
    targets: tuple[str, ...]
    cells: dict[tuple[str, str], Record]  # (experiment, target) -> record

    @classmethod
    def from_records(
        cls, run_id: str, records: Sequence[Record], targets: Sequence[str] = ()
    ) -> Scorecard:
        """`targets` orders the columns (as the run listed them); any other target follows."""
        experiments = tuple(sorted({r["experiment"] for r in records}))
        present = {r["target"] for r in records}
        ordered = tuple(t for t in targets if t in present)
        columns = ordered + tuple(sorted(present - set(ordered)))
        cells = {(r["experiment"], r["target"]): r for r in records}
        return cls(run_id, experiments, columns, cells)

    def outcome(self, experiment: str, target: str) -> str | None:
        record = self.cells.get((experiment, target))
        return None if record is None else str(record["outcome"])

    def totals(self, target: str) -> str:
        counts = Counter(o for e in self.experiments if (o := self.outcome(e, target)) is not None)
        order = [*(str(o) for o in Outcome), *sorted(counts)]
        return ", ".join(f"{counts[o]} {o}" for o in dict.fromkeys(order) if counts[o])

    def to_markdown(self) -> str:
        lines = [
            f"# faultline scorecard, run {self.run_id}",
            "",
            "| experiment | " + " | ".join(self.targets) + " |",
            "|---|" + "---|" * len(self.targets),
        ]
        for e in self.experiments:
            cells = [self.outcome(e, t) or "-" for t in self.targets]
            lines.append(f"| {e} | " + " | ".join(cells) + " |")
        lines += ["", *(f"- {t}: {self.totals(t)}" for t in self.targets)]
        evidence = [
            f"- {e} on {t} ({record['outcome']}): {line}"
            for e in self.experiments
            for t in self.targets
            if (record := self.cells.get((e, t))) and (line := evidence_line(record))
        ]
        if evidence:
            lines += ["", "## Evidence", "", *evidence]
        return "\n".join(lines) + "\n"

    def to_json(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "targets": list(self.targets),
            "matrix": {e: {t: self.outcome(e, t) for t in self.targets} for e in self.experiments},
            "totals": {t: self.totals(t) for t in self.targets},
            "evidence": {
                f"{e}__{t}": line
                for (e, t), record in sorted(self.cells.items())
                if (line := evidence_line(record))
            },
        }


def evidence_line(record: Record) -> str | None:
    """Why a cell is not a PASS, in one line: the first evidence of its worst property."""
    if record["outcome"] == Outcome.PASS:
        return None
    if record["problems"]:
        return str(record["problems"][0])
    worst = [p for p in record["properties"] if p["outcome"] == record["outcome"]]
    for prop in worst or record["properties"]:
        for checkpoint in ("at_f", "at_r"):
            if prop[checkpoint]["evidence"]:
                return f"{prop['name']}: {prop[checkpoint]['evidence'][0]}"
    return None


# ------------------------------------------------------------------ expectations


class ExpectationError(ValueError):
    """An expected-verdicts file that cannot be read."""


def load_expected(path: Path) -> Expected:
    """target -> experiment -> verdicts the cell may take (one, or a list)."""
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ExpectationError(f"{path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise ExpectationError(f"{path}: expected target -> experiment -> verdict")
    known = {str(o) for o in Outcome}
    expected: Expected = {}
    for target, cells in raw.items():
        if not isinstance(cells, dict):
            raise ExpectationError(f"{path}: {target}: expected experiment -> verdict")
        expected[str(target)] = {}
        for experiment, verdicts in cells.items():
            allowed = tuple(verdicts) if isinstance(verdicts, list) else (verdicts,)
            unknown = [v for v in allowed if v not in known]
            if unknown or not allowed:
                raise ExpectationError(
                    f"{path}: {target}: {experiment}: unknown verdict {unknown or allowed}; "
                    f"known: {', '.join(sorted(known))}"
                )
            expected[str(target)][str(experiment)] = tuple(str(v) for v in allowed)
    return expected


@dataclass(frozen=True)
class Check:
    mismatches: tuple[str, ...]  # a cell off its expectation, or expected and missing
    unexpected: tuple[str, ...]  # a cell no expectation covers (reported, not failed)

    @property
    def passed(self) -> bool:
        return not self.mismatches


def check(card: Scorecard, expected: Mapping[str, Mapping[str, Sequence[str]]]) -> Check:
    mismatches, covered = [], set()
    for target, cells in expected.items():
        for experiment, allowed in cells.items():
            covered.add((experiment, target))
            actual = card.outcome(experiment, target)
            if actual is None:
                mismatches.append(f"{experiment} on {target}: expected {_any(allowed)}, not run")
            elif actual not in allowed:
                mismatches.append(
                    f"{experiment} on {target}: expected {_any(allowed)}, got {actual}"
                )
    unexpected = tuple(
        f"{e} on {t}: {card.outcome(e, t)}, no expectation"
        for e, t in sorted(set(card.cells) - covered)
    )
    return Check(tuple(mismatches), unexpected)


def _any(allowed: Sequence[str]) -> str:
    return " or ".join(allowed)
