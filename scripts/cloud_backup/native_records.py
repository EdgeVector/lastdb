"""Explicit native record requests for the owned cloud-recovery homes."""

import copy


def key(value, range_value=None):
    return {"hash": value, "range": range_value}


def create(schema, target, record):
    return {"type": "mutation", "schema": schema, "key_value": target,
            "record": copy.deepcopy(record), "alternates": {}, "mutation_type": "create",
            "expected": {"type": "absent"}, "durability": "durable"}


def replace(schema, row, record, definition_id=None):
    request = {"type": "mutation", "schema": schema, "key_value": copy.deepcopy(row["key"]),
               "record": copy.deepcopy(record), "alternates": copy.deepcopy(row["alternates"]),
               "mutation_type": "replace", "durability": "durable",
               "expected": {"type": "revision", "revision": row["revision"]}}
    if definition_id is not None:
        request["definition_id"] = definition_id
    # These are complete-record columns. Omission would remove a retained file.
    metadata = row.get("metadata") or {}
    for name in ("source_file_name", "metadata", "file"):
        if metadata.get(name) is not None:
            request[name] = copy.deepcopy(metadata[name])
    return request
