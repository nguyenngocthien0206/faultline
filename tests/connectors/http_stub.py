"""A tiny HTTP server for connector tests: canned responses, recorded requests."""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


@dataclass
class Recorded:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes

    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class StubServer:
    """Answers each request with the next queued (status, body) pair."""

    responses: list[tuple[int, bytes]] = field(default_factory=list)
    requests: list[Recorded] = field(default_factory=list)

    def __post_init__(self) -> None:
        stub = self

        class Handler(BaseHTTPRequestHandler):
            def _answer(self) -> None:
                length = int(self.headers.get("Content-Length") or 0)
                stub.requests.append(
                    Recorded(
                        self.command,
                        self.path,
                        {k.lower(): v for k, v in self.headers.items()},
                        self.rfile.read(length),
                    )
                )
                status, body = stub.responses.pop(0) if stub.responses else (500, b"no response")
                self.send_response(status)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            do_GET = _answer
            do_POST = _answer

            def log_message(self, format: str, *args: object) -> None:
                pass  # keep test output clean

        self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, args=(0.05,), daemon=True
        )
        self._thread.start()

    @property
    def url(self) -> str:
        host, port = self._server.server_address[:2]
        return f"http://{host!s}:{port}"

    def reply(self, status: int, body: bytes | str | dict[str, Any]) -> None:
        if isinstance(body, dict):
            body = json.dumps(body)
        self.responses.append((status, body.encode() if isinstance(body, str) else body))

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
