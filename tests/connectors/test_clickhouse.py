from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
from http_stub import StubServer

from faultline.connectors.clickhouse import ClickHouseClient, ClickHouseError
from faultline.connectors.workspace import UnsafeWorkspaceError


def client(stub: StubServer) -> ClickHouseClient:
    return ClickHouseClient(stub.url, user="faultline", password="secret", timeout=5)


def test_query_parses_json_each_row_with_numeric_integers(stub: StubServer) -> None:
    stub.reply(200, '{"store_id":"HCM-001","net_vnd":3202000}\n{"store_id":"HN-001","net_vnd":1}\n')

    rows = client(stub).query("SELECT store_id, net_vnd FROM gold")

    assert rows == [
        {"store_id": "HCM-001", "net_vnd": 3202000},
        {"store_id": "HN-001", "net_vnd": 1},
    ]
    request = stub.requests[0]
    settings = parse_qs(urlparse(request.path).query)
    assert settings["default_format"] == ["JSONEachRow"]
    assert settings["output_format_json_quote_64bit_integers"] == ["0"]
    assert request.body == b"SELECT store_id, net_vnd FROM gold"


def test_credentials_travel_in_headers_not_in_the_url(stub: StubServer) -> None:
    stub.reply(200, "")

    client(stub).command("SELECT 1")

    request = stub.requests[0]
    assert request.headers["x-clickhouse-user"] == "faultline"
    assert request.headers["x-clickhouse-key"] == "secret"
    assert "secret" not in request.path


def test_errors_carry_the_clickhouse_message(stub: StubServer) -> None:
    stub.reply(404, "Code: 60. DB::Exception: Unknown table expression identifier 'gold'.")

    with pytest.raises(ClickHouseError, match="Code: 60"):
        client(stub).query("SELECT * FROM gold")


def test_empty_result(stub: StubServer) -> None:
    stub.reply(200, "")

    assert client(stub).query("SELECT 1 WHERE 0") == []


def test_ping(stub: StubServer) -> None:
    stub.reply(200, "Ok.\n")

    assert client(stub).ping()
    assert stub.requests[0].path == "/ping"


def test_ping_is_false_when_unreachable() -> None:
    assert not ClickHouseClient("http://127.0.0.1:9", timeout=1).ping()


def test_drop_workspace_only_drops_faultline_databases(stub: StubServer) -> None:
    stub.reply(200, "")
    ch = client(stub)

    ch.drop_workspace("fl_abcd1234")
    with pytest.raises(UnsafeWorkspaceError):
        ch.drop_workspace("default")

    assert [r.body for r in stub.requests] == [b"DROP DATABASE IF EXISTS fl_abcd1234"]


def test_export_parquet_streams_the_body_into_the_file(stub: StubServer, tmp_path: Path) -> None:
    payload = b"PAR1" + bytes(range(256)) * 4096 + b"PAR1"  # ~1 MB, larger than one chunk
    stub.reply(200, payload)
    dest = tmp_path / "snap" / "gold.parquet"

    result = client(stub).export_parquet("SELECT * FROM gold", dest)

    assert result == dest
    assert dest.read_bytes() == payload
    assert parse_qs(urlparse(stub.requests[0].path).query)["default_format"] == ["Parquet"]


def test_failed_export_leaves_no_file_behind(stub: StubServer, tmp_path: Path) -> None:
    stub.reply(500, "Code: 60. DB::Exception: Unknown table")
    dest = tmp_path / "gold.parquet"

    with pytest.raises(ClickHouseError, match="Unknown table"):
        client(stub).export_parquet("SELECT * FROM gold", dest)

    assert list(tmp_path.iterdir()) == []
