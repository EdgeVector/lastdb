"""Forty keyed reads at once on the app socket.

Every connection is open before any request is sent, so all forty occupy the
app pool together. Each read is one key. The app pool must answer all of them
and report no queue-full refusal.
"""
import json
import socket
from pathlib import Path

from record_app_api_calls import BY_ID
from record_runs import require

COUNT = 40
WORKERS = 4
QUEUE = 256


def parallel_reads(node):
    """A registered app opens forty connections and reads one key on each."""
    access = {"read": [BY_ID], "write": [], "extensions": []}
    app = node.call("/api/apps/register", {"app": "queue", "access": access})[0]["app"]
    before = app_pool(node)
    replies = exchange(Path(node.socket).with_name("apps.sock"), app)
    require(
        len(replies) == COUNT and all(ok_read(reply) for reply in replies),
        "the app socket did not answer every keyed read: " + repr(replies),
    )
    after = app_pool(node)
    require(
        after["queue_full_rejects"] == 0,
        "the app socket refused a connection: " + repr(after),
    )
    require(
        after["submitted"] == before["submitted"] + COUNT,
        "the app socket did not count the reads: "
        + repr((before["submitted"], after["submitted"])),
    )
    print(
        "App socket queue: "
        f"{COUNT} keyed reads answered 200, "
        f"workers={after['workers']} queue_capacity={after['queue_capacity']} "
        f"in_flight={after['in_flight']} submitted={after['submitted']} "
        f"queue_full_rejects={after['queue_full_rejects']}",
        flush=True,
    )


def app_pool(node):
    pool = node.call("/api/status")[0]["status"].get("app_socket")
    require(
        isinstance(pool, dict)
        and pool.get("workers") == WORKERS
        and pool.get("queue_capacity") == QUEUE,
        "status app_socket is not the app pool: " + repr(pool),
    )
    return pool


def ok_read(reply):
    status, body = reply
    results = body.get("results") if isinstance(body, dict) else None
    if (
        status != 200
        or not isinstance(results, list)
        or not results
        or not isinstance(results[0], dict)
    ):
        return False
    records = results[0].get("records")
    return isinstance(records, list) and len(records) == 1


def exchange(sock, app):
    payload = request_bytes(app)
    conns = []
    try:
        for _ in range(COUNT):
            conn = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            conn.settimeout(30)
            conn.connect(str(sock))
            conns.append(conn)
        for conn in conns:
            try:
                conn.sendall(payload)
            except OSError:
                pass
        return [read_reply(conn) for conn in conns]
    finally:
        for conn in conns:
            conn.close()


def request_bytes(app):
    body = json.dumps(
        {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}}]},
        separators=(",", ":"),
    ).encode()
    head = (
        "POST /v1/read HTTP/1.1\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(body)}\r\n"
        f"X-LastDB-App: {app}\r\n"
        "Connection: close\r\n"
        "\r\n"
    )
    return head.encode() + body


def read_reply(conn):
    raw = b""
    try:
        while len(raw) < 1_000_000:
            chunk = conn.recv(8192)
            if not chunk:
                break
            raw += chunk
    except OSError:
        pass
    head, _, body = raw.partition(b"\r\n\r\n")
    parts = head.split(b"\r\n", 1)[0].split()
    status = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 0
    try:
        payload = json.loads(body) if body else {}
    except json.JSONDecodeError:
        payload = {}
    return status, payload
