"""Signed load routes, request builders, and post-tier log verification."""

from __future__ import annotations

import base64
import json
import time

from faults import RetriesExhausted, finding_message, quiet_backoff_seconds, tier_summary
from guard import allow_stream

PAYLOAD = base64.urlsafe_b64encode(b"load").rstrip(b"=").decode("ascii")
ROOT = "/api/cloud/backups/v2/mutation-log"
READS = frozenset({"head", "list"})
ENTRY_FIELDS = (
    "accepted_at_ms", "authored_at_ms", "delivery_seq", "namespace", "payload",
    "writer_device", "writer_key", "writer_seq",
)
FIELDS = {
    "create-database": ("database_id", "kind", "spec"),
    "append": ("database_id", "device_id", "entries"),
    "head": ("database_id",),
    "list": ("after", "database_id", "limit"),
}
ENTRY_KEYS = ("delivery_seq", "writer_device", "writer_seq")


class RouteError(Exception):
    """The response did not prove what the call asked for."""


def path_for(operation):
    if operation not in FIELDS:
        raise RouteError("route")
    return ROOT + "/" + operation


def request_body(operation, fields):
    allowed = FIELDS[operation]
    if set(fields) != set(allowed):
        raise RouteError("body")
    allow_stream(fields["database_id"])
    if operation == "create-database" and fields["kind"] not in ("personal", "shareable"):
        raise RouteError("body")
    if operation == "append":
        _entries(fields["entries"])
        _check_device(fields["device_id"])
    if operation == "list":
        _u64(fields["after"])
        _u64(fields["limit"])
    wire = fields.copy()
    for field in ("after", "limit"):
        if field in wire:
            wire[field] = str(wire[field])
    if operation == "append":
        wire["entries"] = [{**entry, "writer_seq": str(entry["writer_seq"]),
                            "authored_at_ms": str(entry["authored_at_ms"])}
                           for entry in wire["entries"]]
    encoded = json.dumps(wire, separators=(",", ":"), sort_keys=True).encode("utf-8")
    if set(json.loads(encoded)) != set(allowed):
        raise RouteError("body")
    return encoded


def decimal(value):
    if isinstance(value, (bool, float)):
        raise RouteError("ambiguous")
    if isinstance(value, int) and value >= 0:
        return value
    if isinstance(value, str) and value.isdigit() and (value == "0" or not value.startswith("0")):
        return int(value)
    raise RouteError("ambiguous")


def require_object(value, required):
    if not isinstance(value, dict) or not set(required).issubset(value):
        raise RouteError("ambiguous")
    return value


def check_success(operation, payload, writer_seq=None):
    if operation == "head":
        require_head(payload)
    elif operation == "list":
        require_list(payload)
    elif operation == "create-database":
        require_object(payload, ("database_id", "state"))
    elif operation == "append":
        result = require_object(payload, ("result", "delivery_seqs", "next_seq"))
        delivered = result["delivery_seqs"]
        if (result["result"] != "accepted" or not isinstance(delivered, list)
                or len(delivered) != 1 or not decimal(delivered[0])
                or decimal(result["next_seq"]) != writer_seq + 1):
            raise RouteError("append")
    else:
        raise RouteError("route")


def require_head(body):
    body = require_object(body, ("cursor",))
    return None if body["cursor"] is None else decimal(body["cursor"])


def _lift_entry(entry):
    """Copy the nested signed entry onto the row the log audit reads."""
    nested = entry.get("entry")
    if not isinstance(nested, dict):
        return
    if "device_id" in nested:
        entry["writer_device"] = nested["device_id"]
    if "node_public_key" in nested:
        entry["writer_key"] = nested["node_public_key"]
    for key in ("writer_seq", "authored_at_ms", "namespace"):
        if key in nested:
            entry[key] = nested[key]
    if "payload" in nested:
        entry["payload"] = nested["payload"]
    elif "payload_b64" in nested:
        entry["payload"] = nested["payload_b64"]


def require_list(body):
    page = require_object(body, ("entries", "next"))
    if not isinstance(page["entries"], list):
        raise RouteError("ambiguous")
    for entry in page["entries"]:
        if not isinstance(entry, dict):
            raise RouteError("ambiguous")
        _lift_entry(entry)
        require_object(entry, ENTRY_FIELDS)
    if page["next"] is not None:
        decimal(page["next"])
    return page


def _entries(entries):
    if not isinstance(entries, list) or not entries:
        raise RouteError("body")
    for entry in entries:
        if set(entry) != {"authored_at_ms", "namespace", "payload_b64", "writer_seq"}:
            raise RouteError("body")
        _u64(entry["writer_seq"])
        _u64(entry["authored_at_ms"])
        if not isinstance(entry["namespace"], str) or not 0 < len(entry["namespace"]) <= 128:
            raise RouteError("body")
        if not isinstance(entry["payload_b64"], str):
            raise RouteError("body")


def _u64(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RouteError("body")


def _check_device(value):
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-_"
    if not isinstance(value, str) or not 0 < len(value) <= 64 or any(ch not in allowed for ch in value):
        raise RouteError("body")


def collect_pages(cursor, fetch):
    """Read list pages until ``next`` reaches the head cursor."""
    cursor, after, found = cursor or 0, 0, []
    while True:
        page = fetch(after)
        found.extend(page["entries"])
        if cursor == 0:
            return found
        nxt = page.get("next")
        if nxt is None:
            raise RouteError("log")
        nxt = decimal(nxt)
        if nxt >= cursor:
            return found
        if nxt <= after:
            raise RouteError("log")
        after = nxt


def unresolved_writes_absent(entries, unresolved):
    """An exhausted conflict cannot leave a partially accepted entry."""
    present = {(entry["writer_device"], decimal(entry["writer_seq"])) for entry in entries}
    return all(key not in present for key in unresolved)


def verify_log(entries, accepted):
    if not isinstance(entries, list) or not isinstance(accepted, dict):
        raise RouteError("log")
    seen = _seen(entries)
    ordered = sorted(seen.values())
    if ordered != list(range(1, len(ordered) + 1)):
        raise RouteError("gap")
    for device, seqs in accepted.items():
        got = sorted(seq for (found, seq) in seen if found == device)
        expect = list(range(1, len(seqs) + 1))
        if sorted(seqs) != expect or got != expect:
            raise RouteError("writer")
    if len(seen) != sum(len(seqs) for seqs in accepted.values()):
        raise RouteError("lost")


def _seen(entries):
    found, deliveries = {}, set()
    for entry in entries:
        body = require_object(entry, ENTRY_KEYS)
        device = body["writer_device"]
        if not isinstance(device, str) or not device:
            raise RouteError("log")
        key = (device, decimal(body["writer_seq"]))
        delivery = decimal(body["delivery_seq"])
        if key in found or delivery in deliveries:
            raise RouteError("duplicate")
        found[key] = delivery
        deliveries.add(delivery)
    return found


def device_name(prefix, index):
    return "{0}-d{1}".format(prefix, index)


def request_for(session, operation, fields, limit):
    """JSON body for one mutation-log call."""
    database = session.database_id
    if operation == "head":
        return request_body(operation, {"database_id": database})
    if operation == "create-database":
        return request_body(operation, {
            "database_id": database, "kind": "shareable",
            "spec": {"name": "whole-database", "version": 1},
        })
    if operation == "append":
        device = fields.get("device_id") or device_name(session.device_prefix, fields["index"])
        return request_body(operation, {
            "database_id": database, "device_id": device,
            "entries": [{"authored_at_ms": fields["authored_at_ms"], "namespace": "load",
                         "payload_b64": PAYLOAD, "writer_seq": fields["seq"]}],
        })
    if operation == "list":
        return request_body(operation, {"after": fields.get("after", 0), "database_id": database, "limit": limit})
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


def audit_log(session):
    """Read the log after the writers have stopped, with one drained recheck on read throttle."""
    session.in_logcheck = True
    session.log_read_throttled = False
    try:
        check_log(session)
    except RetriesExhausted:
        if not session.log_read_throttled:
            raise
        session.logcheck_recheck = True
        session.quiet = True
        with session._lock:
            delay = quiet_backoff_seconds(1, session.rng.randrange)
        time.sleep(delay)
        check_log(session)
    finally:
        session.in_logcheck = False
        session.quiet = False
    session.audited = True


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
    summary = tier_summary(session.samples, session.errors, elapsed_ms, counters, estimate, session.tier_reads)
    report = {"stopped": stopped, "summary": summary, "estimate": estimate}
    if message:
        report["message"] = message
    return report
