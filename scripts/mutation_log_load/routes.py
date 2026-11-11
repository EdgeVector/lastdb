"""Mutation-log routes and the bodies those routes accept.

Every path is under ``/api/cloud/backups/v2/mutation-log/``. Every action,
including head, snapshot, and trim, sends ``{"database_id": ...}``. The
server dispatch reads that field. Counters are JSON numbers. The payload
is the base64url (no padding) of the bytes ``load``, not the literal word.
Snapshot bytes stay in the table; the snapshot ref is a label.
"""

import base64
import json

from guard import allow_stream


PAYLOAD = base64.urlsafe_b64encode(b"load").rstrip(b"=").decode("ascii")


ROOT = "/api/cloud/backups/v2/mutation-log"
OPERATIONS = (
    "create-database", "append", "head", "list", "ack", "revoke", "snapshot", "trim",
)
READS = frozenset({"head", "list"})
ENTRY_FIELDS = (
    "accepted_at_ms", "authored_at_ms", "delivery_seq", "namespace", "payload",
    "writer_device", "writer_key", "writer_seq",
)

FIELDS = {
    "create-database": ("database_id", "kind"),
    "append": ("database_id", "device_id", "entries"),
    "head": ("database_id",),
    "list": ("after", "database_id", "device_id", "limit"),
    "ack": ("database_id", "device_id", "through"),
    "revoke": ("capability", "database_id", "principal"),
    "snapshot": ("database_id",),
    "trim": ("database_id",),
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
    allow_stream(fields["database_id"])
    if operation == "create-database" and fields["kind"] not in ("personal", "shareable"):
        raise RouteError("body")
    if operation == "append":
        _entries(fields["entries"])
        _device(fields["device_id"])
    if operation == "list":
        _u64(fields["after"])
        _u64(fields["limit"])
        _device(fields["device_id"])
    if operation == "ack":
        _u64(fields["through"])
        _device(fields["device_id"])
    if operation == "revoke":
        _text(fields["principal"])
        _text(fields["capability"])
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


def check_success(operation, payload):
    if operation == "head":
        require_head(payload)
    elif operation == "list":
        require_list(payload)
    elif operation == "ack":
        require_ack(payload)
    elif operation == "snapshot":
        require_snapshot(payload)
    elif operation == "trim":
        require_trim(payload)
    elif operation == "revoke":
        require_revoke(payload)
    elif operation in ("create-database", "append"):
        require_object(payload, ())
    else:
        raise RouteError("route")


def require_head(body):
    body = require_object(body, ("cursor",))
    if body["cursor"] is None:
        return None
    return decimal(body["cursor"])


def require_list(body):
    page = require_object(body, ("entries", "next"))
    if not isinstance(page["entries"], list):
        raise RouteError("ambiguous")
    for entry in page["entries"]:
        require_object(entry, ENTRY_FIELDS)
    if page["next"] is not None:
        decimal(page["next"])
    return page


def require_ack(body):
    return decimal(require_object(body, ("through",))["through"])


def require_revoke(body):
    body = require_object(body, ("revoked",))
    if body["revoked"] is not True:
        raise RouteError("revoke")
    return body


def require_snapshot(body):
    body = require_object(body, ("at_ms", "snapshot_ref", "through_delivery_seq"))
    decimal(body["through_delivery_seq"])
    decimal(body["at_ms"])
    if not isinstance(body["snapshot_ref"], str) or not body["snapshot_ref"]:
        raise RouteError("snapshot")
    return body


def require_trim(response):
    """The trim floor moved past zero. There is no dropped count."""
    body = require_object(response, ("snapshot_due", "through"))
    if not isinstance(body["snapshot_due"], bool):
        raise RouteError("ambiguous")
    through = decimal(body["through"])
    if through <= 0:
        raise RouteError("trim")
    return through


def prove_trimmed_list(through, status, body):
    """A list from zero conflicts at the snapshot, or starts after the floor."""
    if status == 409:
        page = require_object(body, ("code", "snapshot_through"))
        if page.get("code") != "predecessor_conflict":
            raise RouteError("trim")
        if decimal(page["snapshot_through"]) < through:
            raise RouteError("trim")
        return
    if status != 200:
        raise RouteError("trim")
    page = require_list(body)
    for entry in page["entries"]:
        if decimal(entry["delivery_seq"]) <= through:
            raise RouteError("trim")


def _entries(entries):
    if not isinstance(entries, list) or not entries:
        raise RouteError("body")
    for entry in entries:
        if set(entry) != {"authored_at_ms", "namespace", "payload", "writer_seq"}:
            raise RouteError("body")
        _u64(entry["writer_seq"])
        _u64(entry["authored_at_ms"])
        _text(entry["namespace"])
        if not isinstance(entry["payload"], str):
            raise RouteError("body")


def _u64(value):
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise RouteError("body")


def _device(value):
    if not isinstance(value, str) or not value or len(value) > 64:
        raise RouteError("body")
    allowed = "abcdefghijklmnopqrstuvwxyz0123456789-_"
    if any(ch not in allowed for ch in value):
        raise RouteError("body")


def _text(value):
    if not isinstance(value, str) or not value or len(value) > 128:
        raise RouteError("body")
