"""One clean run per edition for the whole integration session, shared by every worker.

pytest-xdist runs each worker in its own process, so a session fixture alone
would run the same baseline once per worker. Instead the first worker that
needs an edition's baseline runs it, under a file lock, in the session's
shared temporary folder, and pickles the result there; every other worker
waits for the lock and reads it. The pattern the xdist documentation gives
for "run this once per session".

A baseline is only run when a test asks for it, so a CI job that runs one
edition never pays for the other's. One that is not established is shared
too: every test then fails on the same "steady state not established",
instead of each worker trying again.
"""

import pickle
from pathlib import Path

from filelock import FileLock
from reference_stack import reference_config, reference_target

from faultline.datagen import DatasetSpec
from faultline.runner import Baseline, run_baseline

TINY = DatasetSpec.from_profile("tiny")
# Long enough for a baseline on a busy stack (four runs, each up to the adapter's
# run timeout in the worst case), short enough that a hung one fails the suite.
LOCK_TIMEOUT = 4 * reference_config().run_timeout_seconds


def shared_baseline(edition: str, folder: Path) -> Baseline:
    """The edition's clean run of the tiny dataset, run by whichever worker asks first."""
    home = Path(folder) / f"baseline-{edition}"
    with FileLock(f"{home}.lock", timeout=LOCK_TIMEOUT):
        done = home / "baseline.pickle"
        if done.is_file():
            return pickle.loads(done.read_bytes())
        baseline = run_baseline(TINY, reference_target(edition), home / "run")
        done.write_bytes(pickle.dumps(baseline))
        return baseline


class Baselines:
    """Each edition's shared baseline, read once per worker and kept."""

    def __init__(self, folder: Path) -> None:
        self.folder = Path(folder)
        self._kept: dict[str, Baseline] = {}

    def __call__(self, edition: str) -> Baseline:
        if edition not in self._kept:
            self._kept[edition] = shared_baseline(edition, self.folder)
        return self._kept[edition]
