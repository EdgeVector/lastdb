"""Complete-record request builders and batch revision pre-readers."""

from collections import defaultdict
import copy


def key(hash_value, range_value=None):
    return {"hash": hash_value, "range": range_value}


def coordinate(schema, key_value):
    return schema, key_value.get("hash"), key_value.get("range")


def create(schema, key_value, record, *, alternates=None, durability="durable"):
    return {
        "type": "mutation",
        "schema": schema,
        "key_value": key_value,
        "record": copy.deepcopy(record),
        "alternates": copy.deepcopy(alternates) if alternates else {},
        "mutation_type": "create",
        "expected": {"type": "absent"},
        "durability": durability,
    }


def replace_row(schema, row, record, *, definition_id=None, durability="durable"):
    """Build a revision-conditioned complete-record replacement preserving file metadata columns."""
    request = {
        "type": "mutation",
        "schema": schema,
        "key_value": copy.deepcopy(row["key"]),
        "record": copy.deepcopy(record),
        "alternates": copy.deepcopy(row["alternates"]),
        "mutation_type": "replace",
        "durability": durability,
        "expected": {"type": "revision", "revision": row["revision"]},
    }
    if definition_id is not None:
        request["definition_id"] = definition_id
    metadata = row.get("metadata") or {}
    for name in ("source_file_name", "metadata", "file"):
        if metadata.get(name) is not None:
            request[name] = copy.deepcopy(metadata[name])
    return request


def mutation(schema, key_value, record, operation="replace", *, alternates=None, expected=None,
             durability="durable", convergence="sync"):
    return {
        "type": "mutation",
        "schema": schema,
        "key_value": key_value,
        "record": record,
        "alternates": alternates or {},
        "mutation_type": operation,
        "expected": expected if expected is not None else {"type": "absent"} if operation == "create" else None,
        "durability": durability,
        "convergence": convergence,
    }


def _unwrap_reply(reply):
    return reply[0] if isinstance(reply, tuple) else reply


def read_records(node, requests):
    grouped = defaultdict(list)
    for request in requests:
        grouped[request["schema"]].append(request["key_value"])
    queries = [
        {
            "schema_name": schema,
            "limit": len(keys),
            "filter": {"HashRangeKeys": [[item["hash"], item.get("range") or ""] for item in keys]},
        }
        for schema, keys in grouped.items()
    ]
    if not queries:
        return {}
    reply = _unwrap_reply(node.call("/api/queries/batch", {"queries": queries}))
    found = {}
    for schema, item in zip(grouped, reply["results"]):
        if item["status"] != 200:
            raise RuntimeError(f"complete record preparation: HTTP {item['status']}")
        value = item["response"].get("data", item["response"])
        for row in value["results"]:
            found[coordinate(schema, row["key"])] = row
    return found


def conditioned(node, requests):
    """Read current revisions in one batch query and populate `expected` preconditions."""
    current = read_records(node, [req for req in requests if req["mutation_type"] != "create"])
    for request in requests:
        if request["mutation_type"] == "create":
            request["expected"] = request["expected"] or {"type": "absent"}
            continue
        row = current.get(coordinate(request["schema"], request["key_value"]))
        if request["expected"] is None:
            if row is None:
                raise RuntimeError("complete record target is absent")
            request["expected"] = {"type": "revision", "revision": row["revision"]}
        if request["mutation_type"] in ("delete", "purge") and row is not None:
            request["alternates"] = row["alternates"]
    return requests
