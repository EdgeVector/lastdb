"""Mutation-log routes and the exact bodies those routes accept.

Every path is under ``/api/cloud/backups/v2/mutation-log/``. A body is a
fixed set of fields. ``database_id`` and ``stream_id`` are the same load id.
Snapshot bytes stay in the table; the snapshot ref is only a label.
"""

import json

from guard import allow_stream


ROOT = "/api/cloud/backups/v2/mutation-log"
OPERATIONS = (
    "create-database", "append", "head", "list", "ack", "revoke", "snapshot", "trim",
)
READS = frozenset({"head", "list"})
WRITES = frozenset(OPERATIONS) - READS

FIELDS = {
    "create-database": ("account_id", "database_id", "stream_id", "version"),
    "append": (
        "account_id", "authored_at_ms", "database_id", "device", "namespace",
        "payload", "stream_id", "version", "writer_key", "writer_seq",
    ),
    "head": ("account_id", "database_id", "stream_id", "version"),
    "list": ("account_id", "after", "database_id", "limit", "stream_id", "version"),
    "ack": (
        "account_id", "database_id", "device", "stream_id", "through", "version",
        "writer_key",
    ),
    "revoke": ("account_id", "database_id", "stream_id", "version", "writer_key"),
    "snapshot": (
        "account_id", "at_ms", "database_id", "snapshot_ref", "stream_id", "version",
    ),
    "trim": ("account_id", "database_id", "now_ms", "stream_id", "version"),
}


class RouteError(Exception):
    """The response did not prove what the call asked for."""


def path_for(operation):
    if operation not in OPERATIONS:
        raise RouteError("route")
    return ROOT + "/" + operation


def request_body(operation, fields):
    allowed = FIELDS[operation]
    if set(fields) != set(allowed):
        raise RouteError("body")
    if fields["version"] != 2 or fields["database_id"] != fields["stream_id"]:
        raise RouteError("body")
    allow_stream(fields["database_id"])
    encoded = json.dumps(fields, separators=(",", ":"), sort_keys=True).encode("utf-8")
    parsed = json.loads(encoded)
    if set(parsed) != set(allowed):
        raise RouteError("body")
    return encoded


def decimal(value):
    if isinstance(value, bool) or isinstance(value, float):
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


def require_trim(before_floor, response):
    """The floor moved and at least one stored entry was dropped."""
    body = require_object(response, ("through", "dropped"))
    through = decimal(body["through"])
    dropped = decimal(body["dropped"])
    if through <= decimal(before_floor) or dropped < 1:
        raise RouteError("trim")
    return through, dropped


def require_entries_above(floor, response):
    body = require_object(response, ("entries",))
    if not isinstance(body["entries"], list):
        raise RouteError("ambiguous")
    for entry in body["entries"]:
        if decimal(require_object(entry, ("delivery_seq",))["delivery_seq"]) <= floor:
            raise RouteError("trim")
