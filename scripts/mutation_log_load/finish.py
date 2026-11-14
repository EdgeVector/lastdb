"""Build load requests and verify the log after each writer tier."""

from faults import RetriesExhausted, quiet_backoff_seconds
from logcheck import unresolved_writes_absent, collect_pages, verify_log
from stats import finding_message, tier_summary
from routes import PAYLOAD, RouteError, request_body, require_head


def _device(prefix, index):
    return "{0}-d{1}".format(prefix, index)


def request_for(session, operation, fields, limit):
    """JSON body for one mutation-log call."""
    database = session.database_id
    if operation == "head":
        return request_body(operation, {"database_id": database})
    if operation == "create-database":
        return request_body(operation, {
            "database_id": database,
            "kind": "shareable",
            "spec": {"name": "social", "version": 1},
        })
    if operation == "append":
        device = fields.get("device_id") or _device(session.device_prefix, fields["index"])
        return request_body(operation, {
            "database_id": database,
            "device_id": device,
            "entries": [{
                "authored_at_ms": fields["authored_at_ms"],
                "namespace": "social",
                "payload_b64": PAYLOAD,
                "writer_seq": fields["seq"],
            }],
        })
    if operation == "list":
        return request_body(operation, {
            "after": fields.get("after", 0),
            "database_id": database,
            "limit": limit,
        })
    raise RouteError("route")


def read_log(session):
    """List until ``next`` reaches the head cursor."""
    head = session.call("head", required=True)
    if head is None:
        raise RouteError("log")
    session.audit_cursor = require_head(head) or 0

    def fetch(after):
        page = session.call("list", required=True, after=after)
        if page is None:
            raise RouteError("log")
        return page

    return collect_pages(session.audit_cursor, fetch)


def check_log(session):
    """Reject a short page, a broken invariant, or a lost accepted entry."""
    entries = read_log(session)
    if len(entries) != session.audit_cursor:
        raise RouteError("log")
    with session._lock:
        accepted = {device: set(seqs) for device, seqs in session.accepted.items()}
        unresolved = list(session.unresolved)
    verify_log(entries, accepted)
    session.no_partial_writes = unresolved_writes_absent(entries, unresolved)


def _read_with_recheck(session):
    """Read the log. A drained recheck follows a read throttle, once."""
    session.in_logcheck = True
    session.log_read_throttled = False
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
        session.quiet = False


def audit_log(session):
    """Read the log after the writers have stopped."""
    _read_with_recheck(session)
    session.audited = True


def _drain(session):
    """Writers have stopped. Wait before the read-only logcheck runs again."""
    with session._lock:
        delay = quiet_backoff_seconds(1, session.rng.randrange)
    session.sleeper(delay)


def verify_after_cap(session):
    """A write already exhausted. Read the log before the budget stop wins."""
    from cost import CapExceeded
    from drive import LoadError
    try:
        audit_log(session)
    except (LoadError, RouteError, RetriesExhausted, CapExceeded):
        return "error"
    if session.no_partial_writes:
        return "scaling"
    return "error"


def finding_text(session, spec):
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
