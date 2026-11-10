import pytest

from faultline.connectors.postgres import SourceDatabase
from faultline.connectors.workspace import UnsafeWorkspaceError

# Nothing listens here: the guard must fire before any connection attempt.
UNREACHABLE = "postgresql://nobody:nothing@127.0.0.1:9/none?connect_timeout=1"


@pytest.mark.parametrize("name", ["public", "pg_catalog", "information_schema", "pos"])
def test_create_and_drop_refuse_non_workspace_schemas(name: str) -> None:
    db = SourceDatabase(UNREACHABLE)

    with pytest.raises(UnsafeWorkspaceError):
        db.create_workspace(name)
    with pytest.raises(UnsafeWorkspaceError):
        db.drop_workspace(name)
    assert db._conn is None


def test_ping_is_false_when_unreachable() -> None:
    assert not SourceDatabase(UNREACHABLE).ping()
