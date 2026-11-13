"""Mutation-log routes and the bodies those routes accept.

These are the exemem-infra #778 routes. Every path is under
``/api/cloud/backups/v2/mutation-log/``. The server also serves ``access``,
``reject``, ``writer-tip``, and ``cutover``. This load calls
``create-database``, ``append``, ``head``, ``list``, and ``ack``. The trim
tier also calls ``snapshot`` and ``trim``. Every action, including head,
snapshot, and trim, sends ``{"database_id": ...}``. The server dispatch
reads that field. Counters are JSON numbers. The payload is the base64url
(no padding) of the bytes ``load``, not the literal word. Snapshot bytes
stay in the table; the snapshot ref is a label.
"""

import base64
import json

from guard import allow_stream


PAYLOAD = base64.urlsafe_b64encode(b"load").rstrip(b"=").decode("ascii")


ROOT = "/api/cloud/backups/v2/mutation-log"
OPERATIONS = (
    "create-database", "append", "head", "list", "revoke", "snapshot", "trim",
)
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
    if operation == "revoke":
        _text(fields["principal"])
        _text(fields["capability"])
    wire = fields.copy()
    for field in ("after", "limit"):
        if field in wire:
            wire[field] = str(wire[field])
    if operation == "append":
        wire["entries"] = [{**entry, "writer_seq": str(entry["writer_seq"]),
                            "authored_at_ms": str(entry["authored_at_ms"])}
                           for entry in wire["entries"]]
    encoded = json.dumps(wire, separators=(",", ":"), sort_keys=True).encode("utf-8")
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


def _lift_entry(entry):
    """Copy the nested #778 entry onto the row the log check reads."""
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
    """A list from zero conflicts at the snapshot, or starts after the floor.

    The conflict body is ``predecessor_conflict`` on ``mutation_log.cursor``,
    with ``snapshot_through`` and ``snapshot_ref``. The ref is null or a
    snapshot body.
    """
    if status == 409:
        page = require_object(body, ("code", "field", "snapshot_ref", "snapshot_through"))
        if page.get("code") != "predecessor_conflict":
            raise RouteError("trim")
        if page.get("field") != "mutation_log.cursor":
            raise RouteError("trim")
        if decimal(page["snapshot_through"]) < through:
            raise RouteError("trim")
        ref = page["snapshot_ref"]
        if ref is not None:
            require_snapshot(ref)
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
        if set(entry) != {"authored_at_ms", "namespace", "payload_b64", "writer_seq"}:
            raise RouteError("body")
        _u64(entry["writer_seq"])
        _u64(entry["authored_at_ms"])
        _text(entry["namespace"])
        if not isinstance(entry["payload_b64"], str):
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
