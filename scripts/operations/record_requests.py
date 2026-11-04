"""Native complete-record requests used by the existing generated-home operations.

This client supplies revision conditions and explicit addresses. It never merges
payload fields or translates an older mutation dialect.
"""
from collections import defaultdict


def mutation(schema, key, record, operation="replace", *, alternates=None, expected=None):
    return {"type": "mutation", "schema": schema,
            "key_value": key, "record": record, "alternates": alternates or {},
            "mutation_type": operation,
            "expected": expected if expected is not None else {"type": "absent"} if operation == "create" else None,
            "durability": "durable", "convergence": "sync"}


def coordinate(schema, key):
    return schema, key.get("hash"), key.get("range")


def read_records(node, requests):
    grouped = defaultdict(list)
    for request in requests:
        grouped[request["schema"]].append(request["key_value"])
    queries = [{"schema_name": schema, "fields": [], "limit": len(keys),
                "filter": {"HashRangeKeys": [[key["hash"], key.get("range") or ""] for key in keys]}}
               for schema, keys in grouped.items()]
    if not queries:
        return {}
    reply = node.call("/api/queries/batch", {"queries": queries})[0]
    found = {}
    for schema, item in zip(grouped, reply["results"]):
        if item["status"] != 200:
            raise RuntimeError(f"complete record preparation: HTTP {item['status']}")
        value = item["response"].get("data", item["response"])
        for row in value["results"]:
            found[coordinate(schema, row["key"])] = row
    return found


def conditioned(node, requests):
    """Collect keys first, then read revision conditions in one batch request."""
    current = read_records(node, [request for request in requests
                                  if request["mutation_type"] != "create"])
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
