"""Workspace names: one isolated environment per experiment.

A workspace name is used verbatim as the Postgres schema, the ClickHouse
database and the S3 prefix of one experiment. Because those are exactly the
things faultline drops afterwards, the name doubles as a safety guard: every
destructive operation validates it first, so a typo or an injected value can
never turn into `DROP DATABASE default`.
"""

from __future__ import annotations

import re
import secrets

WORKSPACE_PATTERN = re.compile(r"^fl_[0-9a-f]{8}$")


class UnsafeWorkspaceError(ValueError):
    """Raised when a name does not look like a faultline-created workspace."""


def new_workspace_name() -> str:
    return f"fl_{secrets.token_hex(4)}"


def validate_workspace_name(name: str) -> str:
    """Return `name` unchanged if it is a faultline workspace, else raise."""
    if not WORKSPACE_PATTERN.fullmatch(name):
        raise UnsafeWorkspaceError(
            f"refusing to touch {name!r}: not a faultline workspace (expected fl_ + 8 hex chars)"
        )
    return name
