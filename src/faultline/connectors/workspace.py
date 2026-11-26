"""Workspace names: one isolated environment per experiment.

A workspace name is used verbatim as the Postgres schema, the ClickHouse
database and the S3 prefix of one experiment. Because those are exactly the
things faultline drops afterwards, the name doubles as a safety guard: every
destructive operation validates it first, so a typo or an injected value can
never turn into `DROP DATABASE default`.

A name can still match by accident, so faultline also marks what it creates
(SANDBOX_MARK: a comment on the Postgres schema and the ClickHouse database,
a marker object in the S3 prefix) and only ever drops a workspace that carries
the mark. A schema someone happened to call fl_0123abcd is left alone.
"""

from __future__ import annotations

import re
import secrets

WORKSPACE_PATTERN = re.compile(r"^fl_[0-9a-f]{8}$")
SANDBOX_MARK = "faultline sandbox"


class UnsafeWorkspaceError(ValueError):
    """Raised when a name does not look like a faultline-created workspace."""


class UnmarkedWorkspaceError(UnsafeWorkspaceError):
    """A workspace name that matches, on something faultline did not mark as its own."""


def new_workspace_name() -> str:
    return f"fl_{secrets.token_hex(4)}"


def is_workspace_name(name: str) -> bool:
    return WORKSPACE_PATTERN.fullmatch(name) is not None


def validate_workspace_name(name: str) -> str:
    """Return `name` unchanged if it is a faultline workspace, else raise."""
    if not WORKSPACE_PATTERN.fullmatch(name):
        raise UnsafeWorkspaceError(
            f"refusing to touch {name!r}: not a faultline workspace (expected fl_ + 8 hex chars)"
        )
    return name
