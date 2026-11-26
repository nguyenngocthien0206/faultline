"""ClickHouse HTTP client over the standard library.

The HTTP interface is ClickHouse's most portable entry point: no native driver,
no compiled dependency, and it can stream results in any output format
(JSONEachRow here, Parquet for snapshots later).
"""

from __future__ import annotations

import json
import shutil
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, BinaryIO

from faultline.connectors.workspace import (
    SANDBOX_MARK,
    UnmarkedWorkspaceError,
    is_workspace_name,
    validate_workspace_name,
)


class ClickHouseError(Exception):
    """ClickHouse rejected a statement or could not be reached."""


class ClickHouseClient:
    def __init__(
        self,
        url: str = "http://127.0.0.1:8123",
        *,
        user: str = "default",
        password: str = "",
        timeout: float = 60.0,
    ) -> None:
        self.url = url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout = timeout

    def _post(
        self, sql: str, settings: dict[str, str] | None = None, *, sink: BinaryIO | None = None
    ) -> bytes:
        """POST a statement. With `sink`, the response is streamed into it instead of returned."""
        params = urllib.parse.urlencode(settings or {})
        request = urllib.request.Request(
            f"{self.url}/?{params}" if params else f"{self.url}/",
            data=sql.encode(),
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                if sink is None:
                    return response.read()
                shutil.copyfileobj(response, sink, length=1 << 20)
                return b""
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode(errors="replace").strip()
            raise ClickHouseError(detail or f"HTTP {exc.code}") from exc
        except urllib.error.URLError as exc:
            raise ClickHouseError(f"cannot reach ClickHouse at {self.url}: {exc.reason}") from exc

    def ping(self) -> bool:
        try:
            with urllib.request.urlopen(f"{self.url}/ping", timeout=self.timeout) as response:
                return response.read().strip() == b"Ok."
        except (urllib.error.URLError, OSError):
            return False

    def command(self, sql: str) -> None:
        """Run a statement that returns no rows (DDL, INSERT ... SELECT)."""
        self._post(sql)

    def query(self, sql: str) -> list[dict[str, Any]]:
        """Run a SELECT and return rows as dicts. 64-bit integers come back as numbers."""
        raw = self._post(
            sql,
            {"default_format": "JSONEachRow", "output_format_json_quote_64bit_integers": "0"},
        )
        return [json.loads(line) for line in raw.splitlines() if line.strip()]

    def export_parquet(self, sql: str, dest: Path) -> Path:
        """Stream the result of a SELECT into a Parquet file at `dest`.

        Written to a temporary name first and renamed at the end, so a failed
        export never leaves a truncated file that looks like a valid snapshot.
        """
        dest.parent.mkdir(parents=True, exist_ok=True)
        partial = dest.with_name(dest.name + ".partial")
        try:
            with partial.open("wb") as sink:
                self._post(sql, {"default_format": "Parquet"}, sink=sink)
            partial.replace(dest)
        finally:
            partial.unlink(missing_ok=True)
        return dest

    def list_workspaces(self) -> list[str]:
        """Databases that are faultline workspaces, sorted."""
        rows = self.query("SELECT name FROM system.databases")
        return sorted(r["name"] for r in rows if is_workspace_name(r["name"]))

    def create_workspace(self, name: str) -> None:
        """Create a workspace's database, marked as faultline's own (the platform reuses it)."""
        self.command(
            f"CREATE DATABASE IF NOT EXISTS {validate_workspace_name(name)} "
            f"COMMENT '{SANDBOX_MARK}'"
        )

    def mark(self, name: str) -> str | None:
        """The comment on a workspace's database, or None when there is no such database."""
        rows = self.query(
            f"SELECT comment FROM system.databases WHERE name = '{validate_workspace_name(name)}'"
        )
        return str(rows[0]["comment"]) if rows else None

    def is_marked(self, name: str) -> bool:
        return self.mark(name) == SANDBOX_MARK

    def drop_workspace(self, name: str) -> None:
        """Drop the database of a faultline workspace.

        Refuses any other name, and a database with the name but not the mark.
        """
        validated = validate_workspace_name(name)
        mark = self.mark(validated)
        if mark is None:
            return
        if mark != SANDBOX_MARK:
            raise UnmarkedWorkspaceError(f"refusing to drop database {validated}: not marked")
        self.command(f"DROP DATABASE IF EXISTS {validated}")
