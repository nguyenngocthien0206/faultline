from collections.abc import Iterator

import pytest
from http_stub import StubServer


@pytest.fixture
def stub() -> Iterator[StubServer]:
    server = StubServer()
    yield server
    server.close()
