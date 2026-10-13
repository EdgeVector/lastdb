"""Unix-socket HTTP client for the Mini node."""

from __future__ import annotations

import json
import socket
import http.client
from dataclasses import dataclass
from pathlib import Path
from typing import Any


class DeliverError(RuntimeError):
    pass


class _UnixHttpResponse(http.client.HTTPResponse):
    def __init__(self, sock: socket.socket) -> None:
        super().__init__(sock)


@dataclass
class MiniClient:
    socket_path: str
    user_hash: str | None = None
    timeout_s: float = 120.0

    def __post_init__(self) -> None:
        self.socket_path = str(Path(self.socket_path).expanduser())

    def _auto_identity(self) -> str | None:
        if self.user_hash:
            return self.user_hash
        body = self.request("GET", "/api/system/auto-identity", attach_identity=False)
        if isinstance(body, dict) and body.get("user_hash"):
            self.user_hash = str(body["user_hash"])
            return self.user_hash
        # unwrap common envelope shapes
        data = body.get("data") if isinstance(body, dict) else None
        if isinstance(data, dict) and data.get("user_hash"):
            self.user_hash = str(data["user_hash"])
            return self.user_hash
        return None

    def request(
        self,
        method: str,
        path: str,
        body: Any | None = None,
        *,
        attach_identity: bool = True,
    ) -> Any:
        payload = b"" if body is None else json.dumps(body, separators=(",", ":")).encode("utf-8")
        headers = [
            f"{method} {path} HTTP/1.1",
            "Host: lastdb",
            "X-LastDB-Client: admin-kanban-hourly-deliver",
            "Connection: close",
        ]
        if body is not None:
            headers.append("Content-Type: application/json")
            headers.append(f"Content-Length: {len(payload)}")
        if attach_identity:
            uh = self._auto_identity()
            if uh:
                headers.append(f"X-User-Hash: {uh}")
        raw = ("\r\n".join(headers) + "\r\n\r\n").encode("ascii") + payload
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            sock.settimeout(self.timeout_s)
            sock.connect(self.socket_path)
            sock.sendall(raw)
            resp = _UnixHttpResponse(sock)
            resp.begin()
            data = resp.read()
            text = data.decode("utf-8", errors="replace")
            status = resp.status
            try:
                parsed = json.loads(text) if text else None
            except json.JSONDecodeError:
                parsed = text
            if status >= 400:
                raise DeliverError(f"{method} {path} HTTP {status}: {text[:1200]}")
            return parsed
        except OSError as error:
            raise DeliverError(f"socket {self.socket_path}: {error}") from error
        finally:
            sock.close()
