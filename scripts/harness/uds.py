"""Unix-domain HTTP client helpers for owned LastDB daemons."""

import http.client
import json
import socket
import time

SOCKET_OPERATIONS = {
    "/health": "health",
    "/api/status": "status",
    "/api/system/boot-identity": "boot_identity",
    "/api/db/native-status": "native_status",
    "/api/queries/batch": "query_batch",
    "/api/mutations/batch": "mutation_batch",
}
SOCKET_REFUSALS = frozenset({
    "cas_conflict",
    "compaction_behind",
    "field_value_too_large",
    "persist_queue_full",
    "read_resource_limit",
    "storage_full",
    "write_resource_limit",
})


class UnixConnection(http.client.HTTPConnection):
    """HTTP/1.1 client connection over an AF_UNIX stream socket."""

    def __init__(self, path, timeout=30):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


def send_unix_request(socket_path, method, path, body=None, headers=None, *, timeout=30, max_bytes=None):
    """Execute one HTTP request over a Unix socket and return status, raw bytes, headers, and timing."""
    payload = body
    if payload is not None and not isinstance(payload, (bytes, bytearray)):
        payload = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    connection = UnixConnection(socket_path, timeout=timeout)
    started = time.perf_counter_ns()
    try:
        connection.request(method, path, payload, headers or {})
        response = connection.getresponse()
        raw = response.read() if max_bytes is None else response.read(max_bytes + 1)
        elapsed_ms = (time.perf_counter_ns() - started) / 1e6
        response_headers = {name.lower(): value for name, value in response.getheaders()}
        return response.status, raw, response_headers, elapsed_ms, len(payload or b"")
    finally:
        connection.close()


def classify_socket_refusal(path, response, *, decode_json_fn=json.loads, ignored_decode_errors=(ValueError,)):
    """Extract only status and closed-set refusal labels without leaking response bodies or paths."""
    refusal = {
        "operation": SOCKET_OPERATIONS.get(path, "other"),
        "status": response.status,
        "reason": "unrecognized",
    }
    try:
        raw = response.read(4097)
    except (OSError, http.client.HTTPException):
        return refusal
    if len(raw) > 4096:
        return refusal
    if raw.startswith(b"node is busy: too many concurrent reads; retry after "):
        refusal["reason"] = "read_busy"
        return refusal
    try:
        body = decode_json_fn(raw)
    except ignored_decode_errors:
        return refusal
    code = body.get("error") if isinstance(body, dict) else None
    if isinstance(code, str) and code in SOCKET_REFUSALS:
        refusal["reason"] = code
    return refusal
