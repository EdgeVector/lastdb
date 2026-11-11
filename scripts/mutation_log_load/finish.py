"""The log check and the trim calls that follow a tier."""

from faults import RetriesExhausted, quiet_backoff_seconds
from logcheck import ack_holds, collect_pages, verify_log
from stats import finding_message, tier_summary
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


def audit_log(session):
    """Read the log. A drained recheck follows a read throttle, once."""
    session.in_logcheck = True
    try:
        check_log(session)
    except RetriesExhausted:
        if not session.log_read_throttled:
            raise
        session.logcheck_recheck = True
        session.quiet = True
        _drain(session)
        check_log(session)
    finally:
        session.in_logcheck = False
    session.audited = True


def _drain(session):
    """Writers have stopped. Wait before the read-only logcheck runs again."""
    with session._lock:
        delay = quiet_backoff_seconds(1, session.rng.randrange)
    session.sleeper(delay)


def after_exhausted(session):
    """Re-read after an exhausted snapshot or trim, then apply the intact-log rule."""
    if not session.audited:
        return "error"
    from drive import LoadError
    try:
        check_log(session)
    except (LoadError, RouteError, RetriesExhausted):
        return "error"
    if session.ack_holds:
        return "scaling"
    return "error"


def finding_text(session, spec):
    if session.trim_skipped and session.ack_holds:
        return "trim not exercised"
    message = finding_message(
        session.throttle_503, spec.kind, session.contention_503,
        session.write_attempts, session.contention_unresolved, session.throttle_unresolved)
    pending = len(session.unresolved)
    if not pending:
        return message
    note = "unresolved appends {0}".format(pending)
    return note if not message else "{0}, {1}".format(message, note)


def tier_report(session, elapsed_ms, stopped, message):
    estimate = session.ledger.snapshot()
    counters = {
        "contention_503": session.contention_503,
        "throttle_503": session.throttle_503,
        "other_503": session.other_503,
        "write_attempts": session.write_attempts,
        "contention_unresolved": session.contention_unresolved,
        "throttle_unresolved": session.throttle_unresolved,
        "unresolved_appends": len(session.unresolved),
        "logcheck_recheck": session.logcheck_recheck,
    }
    summary = tier_summary(
        session.samples, session.errors, elapsed_ms, counters, estimate, session.tier_reads)
    report = {"stopped": stopped, "summary": summary, "estimate": estimate}
    if message:
        report["message"] = message
    return report


def trim_appends(session):
    """Appends this trim tier stored. Earlier tiers on the same database do not count."""
    prefix = session.device_prefix + "-"
    return sum(1 for device, seqs in session.accepted.items() if device.startswith(prefix) and seqs)


def trim_log(session):
    """Ack every stored device through the head cursor, then snapshot and trim.

    The caller does not call this when the trim tier stored nothing. A tier
    that stored at least one append still has to move ``through`` to the
    head cursor, and that cursor has to cover the appends that landed.
    """
    landed = trim_appends(session)
    if landed < 1:
        raise RouteError("trim")
    head = session.call("head", required=True)
    cursor = require_head(head)
    if cursor is None or cursor < landed:
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
