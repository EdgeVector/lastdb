"""The log check and the trim calls that follow a tier."""

from logcheck import ack_holds, collect_pages, verify_log
from plan import TRIM_WRITERS
from routes import (
    PAYLOAD, RouteError, prove_trimmed_list, request_body, require_ack,
    require_head, require_snapshot, require_trim,
)


def _device(prefix, index):
    return "{0}-d{1}".format(prefix, index)


def request_for(session, operation, fields, limit):
    """JSON body for one mutation-log call. An ack does not grow the ledger."""
    database = session.database_id
    if "index" in fields or "device_id" in fields:
        device = fields.get("device_id") or _device(session.device_prefix, fields["index"])
        session.ledger.note_device(device)
    if operation in ("head", "snapshot", "trim", "create-database"):
        body = {"database_id": database}
        if operation == "create-database":
            body["kind"] = "personal"
        return request_body(operation, body)
    device = fields.get("device_id") or _device(session.device_prefix, fields["index"])
    if operation == "append":
        return request_body(operation, {
            "database_id": database,
            "device_id": device,
            "entries": [{
                "authored_at_ms": session.clock(),
                "namespace": "load",
                "payload": PAYLOAD,
                "writer_seq": fields["seq"],
            }],
        })
    if operation == "list":
        return request_body(operation, {
            "after": fields.get("after", 0),
            "database_id": database,
            "device_id": device,
            "limit": limit,
        })
    if operation == "ack":
        return request_body(operation, {
            "database_id": database,
            "device_id": device,
            "through": fields["seq"],
        })
    raise RouteError("route")


def ack_cursor(session, index):
    """Ack the head's delivery cursor. ``through`` is never a writer sequence."""
    if session.call("list", required=True, index=index, after=0) is None:
        raise RouteError("ack")
    head = session.call("head", required=True, index=index)
    if head is None:
        raise RouteError("ack")
    cursor = require_head(head)
    if cursor is None or cursor < 1:
        raise RouteError("ack")
    acked = session.call("ack", required=True, index=index, seq=cursor)
    through = None if acked is None else require_ack(acked)
    if through is None or through < cursor:
        raise RouteError("ack")
    with session._lock:
        if through > session.acked_through:
            session.acked_through = through


def read_log(session):
    """List until ``next`` reaches the head cursor."""
    head = session.call("head", required=True, index=0)
    if head is None:
        raise RouteError("log")
    session.audit_cursor = require_head(head) or 0

    def fetch(after):
        page = session.call("list", required=True, index=0, after=after)
        if page is None:
            raise RouteError("log")
        return page

    return collect_pages(session.audit_cursor, fetch)


def check_log(session):
    """Reject a short page, a broken invariant, or a lost acknowledgement."""
    entries = read_log(session)
    if session.audit_cursor and len(entries) < session.audit_cursor:
        raise RouteError("log")
    with session._lock:
        accepted = {device: set(seqs) for device, seqs in session.accepted.items()}
        unresolved = list(session.unresolved)
        acked = session.acked_through
    verify_log(entries, accepted)
    session.ack_holds = ack_holds(entries, unresolved, acked)


def trim_appends(session):
    """Appends this trim tier stored. Earlier tiers on the same database do not count."""
    prefix = session.device_prefix + "-"
    return sum(1 for device, seqs in session.accepted.items() if device.startswith(prefix) and seqs)


def trim_log(session):
    """Ack every device through the head cursor, then snapshot and trim."""
    if trim_appends(session) < TRIM_WRITERS:
        raise RouteError("trim")
    head = session.call("head", required=True)
    cursor = require_head(head)
    if cursor is None or cursor < TRIM_WRITERS:
        raise RouteError("trim")
    for device in sorted(session.accepted):
        acked = session.call("ack", required=True, device_id=device, seq=cursor)
        if acked is None or require_ack(acked) < cursor:
            raise RouteError("trim")
    require_snapshot(session.call("snapshot", required=True))
    through = require_trim(session.call("trim", required=True))
    if through < cursor:
        raise RouteError("trim")
    status, listed = session.exchange(
        "list", required=True, accept=(409,), index=0, after=0)
    prove_trimmed_list(through, status, listed)
