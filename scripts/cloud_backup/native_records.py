"""Explicit native record requests for the owned cloud-recovery homes."""

import copy
import hashlib
import os
from pathlib import Path
import tempfile

from support import canonical_uuid, require, save_json, sync_directory


def key(value, range_value=None):
    return {"hash": value, "range": range_value}


def create(schema, target, record):
    return {"type": "mutation", "schema": schema, "key_value": target,
            "record": copy.deepcopy(record), "alternates": {}, "mutation_type": "create",
            "expected": {"type": "absent"}, "durability": "durable"}


def read_records(node, schema, targets):
    """Read all exact keys together and retain their complete revision-bound rows."""
    require(isinstance(targets, list) and 0 < len(targets) <= 64
            and all(isinstance(target, dict) and set(target) == {"hash", "range"}
                    and isinstance(target["hash"], str)
                    and (target["range"] is None or isinstance(target["range"], str))
                    for target in targets), "native_record_targets")
    require(len({(target["hash"], target["range"]) for target in targets}) == len(targets),
            "native_record_distinct_targets")
    queries = [{"schema_name": schema, "limit": 2, "offset": 0,
                "filter": ({"HashKey": target["hash"]} if target["range"] is None
                           else {"HashRangeKey": target})} for target in targets]
    replies = node.call("/api/queries/batch", {"queries": queries}).get("results")
    require(isinstance(replies, list) and len(replies) == len(targets), "native_record_batch_count")
    rows = []
    for target, reply in zip(targets, replies):
        value = reply.get("response")
        require(reply.get("status") == 200 and isinstance(value, dict) and value.get("ok") is True
                and value.get("returned_count") == 1 and isinstance(value.get("results"), list)
                and len(value["results"]) == 1 and value.get("has_more") is False
                and value.get("next_cursor") is None, "native_record_complete_read")
        row = value["results"][0]
        require(row.get("key") == target and isinstance(row.get("fields"), dict)
                and isinstance(row.get("revision"), str) and bool(row["revision"])
                and row.get("alternates") == {}
                and (row.get("metadata") is None or isinstance(row["metadata"], dict)),
                "native_record_identity_revision")
        rows.append(row)
    return rows


def replace(schema, row, record):
    request = {"type": "mutation", "schema": schema, "key_value": copy.deepcopy(row["key"]),
               "record": copy.deepcopy(record), "alternates": copy.deepcopy(row["alternates"]),
               "mutation_type": "replace", "durability": "durable",
               "expected": {"type": "revision", "revision": row["revision"]}}
    # These are complete-record columns. Omission would remove a retained file.
    metadata = row.get("metadata") or {}
    for name in ("source_file_name", "metadata", "file"):
        if metadata.get(name) is not None:
            request[name] = copy.deepcopy(metadata[name])
    return request


def delete(schema, row):
    return {"type": "mutation", "schema": schema, "key_value": copy.deepcopy(row["key"]),
            "record": {}, "alternates": copy.deepcopy(row["alternates"]),
            "mutation_type": "delete", "durability": "durable",
            "expected": {"type": "revision", "revision": row["revision"]}}


def put_file(node, schema, record, payload, *, target=None):
    target = target if target is not None else key(record["id"])
    row = read_records(node, schema, [target])[0]
    require(row["fields"] == record, "file_write_complete_preimage")
    request = replace(schema, row, record)
    # The file executor issues the replacement marker from the supplied bytes.
    request.pop("file", None)
    path = payload.with_name(payload.name + ".record.json")
    hydrate = "--hydrate" in node.programs.help("get")
    if hydrate:
        save_json(path, {"mutation": request, "file": {"source": payload.name}})
        arguments = ["mutate", "--request-file", str(path)]
    else:
        save_json(path, request)
        arguments = ["db", "put-blob", "--request-file", str(path), "--file", str(payload)]
    reply = node.programs.command(node.home, arguments)
    require(isinstance(reply.get("file"), dict), "native_file_write_receipt")
    canonical_uuid(reply.get("mutation_id"))
    if hydrate:
        verify_hydrated_file(node, schema, target, record, payload, reply["file"]["sha256"])
    os.unlink(path)
    sync_directory(path.parent)
    return reply


def verify_hydrated_file(node, schema, target, record, payload, expected_digest):
    with tempfile.TemporaryDirectory(dir=payload.parent, prefix="hydrated-") as folder:
        output = Path(folder) / "file"
        arguments = ["get", schema, "--key-hash", target["hash"],
                     "--hydrate", str(output), "--json"]
        if target["range"] is not None:
            arguments += ["--key-range", target["range"]]
        hydrated = node.programs.command(node.home, arguments)["results"]
        require(len(hydrated) == 1 and hydrated[0]["key"] == target
                and hydrated[0]["fields"] == record
                and hydrated[0]["hydrated_file"] == str(output)
                and hydrated[0]["metadata"]["file"]["sha256"] == expected_digest,
                "hydrated_record_and_file_identity")
        with output.open("rb") as stream:
            require(hashlib.file_digest(stream, "sha256").hexdigest() == expected_digest,
                    "hydrated_file_digest")
