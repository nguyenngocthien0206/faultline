"""Experiment results as JSON files: one per experiment and target, one folder per run.

    .faultline/results/<run_id>/<experiment>__<target>.json
    .faultline/results/<run_id>/run.json    what the run was asked to do

A result file holds everything a scorecard or a CI gate needs (the verdict,
every property at F and R with its evidence, every run, the rounds recovery
took, the dataset, how long it took) and points to the snapshots that explain
it. Reading results back
gives plain dicts: the scorecard needs the data, not the objects that made it.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from faultline import __version__
from faultline.datagen.spec import DatasetSpec
from faultline.properties.base import PropertyResult, Recovery
from faultline.runner import ExperimentResult

RESULTS_DIR = Path(".faultline/results")
SEPARATOR = "__"  # <experiment>__<target>.json; neither name contains it
RUN_FILE = "run.json"


def new_run_id(now: datetime | None = None) -> str:
    """A run's id: when it started, to the second, so ids sort in time order."""
    return f"{now or datetime.now():%Y%m%d-%H%M%S}"


def result_file(folder: Path, experiment: str, target: str) -> Path:
    return Path(folder) / f"{experiment}{SEPARATOR}{target}.json"


def _checkpoint(result: PropertyResult) -> dict[str, Any]:
    return {"holds": result.holds, "evidence": list(result.evidence)}


def _recovery(recovery: Recovery | None) -> dict[str, Any] | None:
    if recovery is None:
        return None
    return {
        "max_rounds": recovery.max_rounds,
        "converged": recovery.converged,
        "summary": recovery.summary(),
        "rounds": [
            {
                "runs": r.runs,
                "keys_off": r.keys_off,
                "rows_off": r.rows_off,
                "differences": list(r.differences),
            }
            for r in recovery.rounds
        ],
    }


def result_record(
    result: ExperimentResult,
    *,
    dataset: DatasetSpec | None = None,
    seconds: float | None = None,
) -> dict[str, Any]:
    """The JSON-ready record of one result."""
    return {
        "experiment": result.experiment,
        "target": result.target,
        "outcome": str(result.outcome),
        "faults": list(result.faults),
        "properties": [
            {
                "name": p.name,
                "outcome": str(p.outcome),
                "at_f": _checkpoint(p.at_f),
                "at_r": _checkpoint(p.at_r),
            }
            for p in result.properties
        ],
        "runs": {
            phase: [
                {
                    "partition": day.isoformat(),
                    "status": str(run.status),
                    "errors": list(run.errors),
                    "run_id": run.run_id,
                    "metadata": dict(run.metadata),
                }
                for day, run in runs
            ]
            for phase, runs in result.runs.items()
        },
        "recovery": _recovery(result.recovery),
        "problems": list(result.problems),
        "dataset": dataset.to_dict() if dataset else None,
        "seconds": None if seconds is None else round(seconds, 1),
        "faultline": __version__,
        "snapshots": str(result.work_dir) if result.work_dir else None,
    }


def save_result(result: ExperimentResult, folder: Path, **extra: Any) -> Path:
    """Write the result's record into `folder`; returns the file."""
    path = result_file(folder, result.experiment, result.target)
    path.parent.mkdir(parents=True, exist_ok=True)
    record = result_record(result, **extra)
    path.write_text(json.dumps(record, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    return path


def load_results(folder: Path) -> list[dict[str, Any]]:
    """Every result record of one run, sorted by experiment then target."""
    records = [
        json.loads(path.read_text(encoding="utf-8"))
        for path in Path(folder).glob(f"*{SEPARATOR}*.json")
    ]
    return sorted(records, key=lambda r: (r["experiment"], r["target"]))


def save_run(folder: Path, *, experiments: Sequence[str], targets: Sequence[str]) -> Path:
    """Record what a run was asked to do, in order: the scorecard keeps the targets' order."""
    path = Path(folder) / RUN_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    record = {"experiments": list(experiments), "targets": list(targets), "faultline": __version__}
    path.write_text(json.dumps(record, indent=2) + "\n", encoding="utf-8")
    return path


def load_run(folder: Path) -> dict[str, Any] | None:
    path = Path(folder) / RUN_FILE
    return json.loads(path.read_text(encoding="utf-8")) if path.is_file() else None


def latest_run(results_dir: Path = RESULTS_DIR) -> Path | None:
    """The folder of the most recent run, or None when there is none."""
    runs = sorted(p for p in Path(results_dir).glob("*") if p.is_dir()) if results_dir else []
    return runs[-1] if runs else None
