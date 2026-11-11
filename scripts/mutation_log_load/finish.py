"""The log check and the trim calls that follow a tier."""

from logcheck import ack_holds, collect_pages, verify_log
from plan import TRIM_WRITERS
from routes import (
    RouteError, prove_trimmed_list, require_ack, require_head,
    require_snapshot, require_trim,
)


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


def trim_log(session):
    """Ack every device through the head cursor, then snapshot and trim."""
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
