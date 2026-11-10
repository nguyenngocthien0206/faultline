"""ClickHouse HTTP client over the standard library.

The HTTP interface is ClickHouse's most portable entry point: no native driver,
no compiled dependency, and it can stream results in any output format
(JSONEachRow here, Parquet for snapshots later).
"""

from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

from faultline.connectors.workspace import validate_workspace_name


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

    def _post(self, sql: str, settings: dict[str, str] | None = None) -> bytes:
        params = urllib.parse.urlencode(settings or {})
        request = urllib.request.Request(
            f"{self.url}/?{params}" if params else f"{self.url}/",
            data=sql.encode(),
            headers={"X-ClickHouse-User": self.user, "X-ClickHouse-Key": self.password},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return response.read()
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

    def drop_workspace(self, name: str) -> None:
        """Drop the database of a faultline workspace. Refuses any other name."""
        self.command(f"DROP DATABASE IF EXISTS {validate_workspace_name(name)}")
