"""Signed create, append, head, and list requests for synthetic DEV load."""

import base64
import json

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
        _device(fields["device_id"])
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
    if body["cursor"] is None:
        return None
    return decimal(body["cursor"])


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
