"""The safety guard: faultline destroys data on purpose, so it must be hard to aim wrong.

Three layers, each enough to stop a mistake on its own:

1. A target spec (targets/*.yaml, read by faultline.spec) is refused unless it
   declares `environment: test` and `allow_destructive: true`. No command line
   flag overrides that.
2. The runner checks the Target again right before every sandbox it enters
   and every action it carries out (Target.check), so a Target built in code,
   or changed after it was built, cannot skip the first layer.
3. Each adapter checks its own scope. The reference adapter only touches
   workspaces named fl_ + 8 hex characters, and only drops one that carries
   the sandbox mark it set when it created it (faultline.connectors.workspace).

The guard protects against mistakes, not against a spec that says
`environment: test` while pointing at production; that is what the third
layer is for, in every adapter.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from faultline.adapters.base import Adapter

AdapterFactory = Callable[[Path], Adapter]  # input root -> a fresh sandbox
SAFE_ENVIRONMENT = "test"


class UnsafeTargetError(Exception):
    """A target that does not declare itself a test environment open to destruction."""


@dataclass(frozen=True)
class Target:
    """Where an experiment runs: a named platform, the promise that it is safe, its sandboxes."""

    name: str
    environment: str
    allow_destructive: bool
    sandbox: AdapterFactory

    def __post_init__(self) -> None:
        self.check()

    def check(self) -> None:
        """Raise UnsafeTargetError unless this target may be broken on purpose."""
        if self.environment != SAFE_ENVIRONMENT:
            raise UnsafeTargetError(
                f"target {self.name!r} declares environment {self.environment!r}: faultline "
                f"only runs against environment {SAFE_ENVIRONMENT!r}"
            )
        if self.allow_destructive is not True:
            raise UnsafeTargetError(
                f"target {self.name!r} does not declare allow_destructive: true, and faultline "
                "deletes and rewrites data by design"
            )
