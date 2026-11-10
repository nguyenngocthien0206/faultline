"""The simulated source database: a Postgres the platform's ingestion reads from.

faultline owns this database. It replays business changes into it day by day,
and later mutates it on purpose (rows with an old `updated_at`, edits to rows
that were already extracted). Requires the optional `reference` extra.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Any

from faultline.connectors.workspace import validate_workspace_name

if TYPE_CHECKING:
    import psycopg


def _psycopg() -> Any:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - depends on the environment
        raise ImportError(
            "the Postgres connector needs psycopg: pip install 'faultline[reference]'"
        ) from exc
    return psycopg


class SourceDatabase:
    """A lazily opened connection in autocommit mode."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        self._conn: psycopg.Connection[Any] | None = None

    # ------------------------------------------------------------- connection

    @property
    def conn(self) -> psycopg.Connection[Any]:
        if self._conn is None:
            self._conn = _psycopg().connect(self.dsn, autocommit=True)
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> SourceDatabase:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def ping(self) -> bool:
        try:
            self.execute("SELECT 1")
        except Exception:
            return False
        return True

    # ------------------------------------------------------------- statements

    def execute(self, sql: str | Any, params: Sequence[Any] | None = None) -> None:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)

    def fetch_all(
        self, sql: str | Any, params: Sequence[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        with self.conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()

    def copy_rows(
        self, schema: str, table: str, columns: Sequence[str], rows: Iterable[Sequence[Any]]
    ) -> int:
        """Bulk insert with COPY, the fast path for replaying a day of changes."""
        sql = _psycopg().sql
        statement = sql.SQL("COPY {}.{} ({}) FROM STDIN").format(
            sql.Identifier(schema),
            sql.Identifier(table),
            sql.SQL(", ").join(sql.Identifier(c) for c in columns),
        )
        count = 0
        with self.conn.cursor() as cur, cur.copy(statement) as copy:
            for row in rows:
                copy.write_row(row)
                count += 1
        return count

    # -------------------------------------------------------------- workspaces

    def create_workspace(self, name: str) -> None:
        sql = _psycopg().sql
        self.execute(
            sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(validate_workspace_name(name)))
        )

    def drop_workspace(self, name: str) -> None:
        """Drop a faultline workspace schema and everything in it. Refuses any other name."""
        validated = validate_workspace_name(name)
        sql = _psycopg().sql
        self.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(validated)))
