#!/usr/bin/env python3
"""Rehearse the supported format 12 live-record copy into a fresh format 13 home."""

import argparse
import base64
from contextlib import ExitStack
from dataclasses import dataclass
import hashlib
import os
from pathlib import Path
import sys
import time
from urllib.parse import quote, urlencode

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from home_format import HOME_FORMAT  # noqa: E402
from node import Node  # noqa: E402
from record_requests import conditioned, mutation  # noqa: E402


HASH_SCHEMA = "FormatCopyHash"
RANGE_SCHEMA = "FormatCopyRange"
TTL_SECONDS = 600
PAGE_SIZE = 2
MAX_PAGES = 32
READ_BATCH = 64
FILE_DATA = bytes(range(256)) * 2
SHARED_ALIAS = {"by_group": {"hash": "group", "range": "same"}}


def require(value, message):
    if not value:
        raise RuntimeError(message)


def install(node, definition):
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})


def declaration(name, ranged):
    range_key = {"kind": "hash_range", "range_type": "String"}
    return {
        "name": name, "descriptive_name": name, "source": "system_seed",
        "schema_type": "HashRange" if ranged else "Hash",
        "record_keys": {
            "primary": range_key if ranged else {"kind": "hash"},
            "alternates": {} if ranged else {"by_group": {**range_key, "allow_absent": True}},
        },
        "fields": ["text", "optional"],
        "field_types": {"text": "String", "optional": {"OneOf": ["String", "Null"]}},
        "field_descriptions": {"text": "Generated copy value", "optional": "Generated nullable value"},
    }


def retention(node, schema, action="get", **fields):
    return node.call("/api/db/schema-retention", {
        "action": action, "schema": schema, **fields,
    })[0]


def key_pair(key):
    require(isinstance(key["hash"], str) and key["hash"], "copy requires an explicit API hash")
    return key["hash"], key.get("range") or ""


def list_keys(node, schema, key_name=None, lookup_hash=None):
    """Use the owner's bounded key-list API, including alternate memberships."""
    params = {"schema": schema, "limit": PAGE_SIZE}
    if key_name is not None:
        params.update(key_name=key_name, hash=lookup_hash)
    entries, seen_cursors = [], set()
    for _ in range(MAX_PAGES):
        page = node.call("/api/list?" + urlencode(params))[0]["list"]
        matches = [key_pair(key) for key in page["keys"]]
        if key_name is None:
            entries.extend(matches)
        else:
            primaries = [key_pair(key) for key in page["primary_keys"]]
            require(len(matches) == len(primaries), "incomplete alternate key page")
            entries.extend(zip(matches, primaries))
        if not page["has_more"]:
            require(len(entries) == len(set(entries)), "key list repeated an entry")
            return set(entries)
        cursor = page["next_cursor"]
        require(cursor and cursor not in seen_cursors, "key listing failed to advance")
        seen_cursors.add(cursor)
        params["cursor"] = cursor
    raise RuntimeError("copy key listing exceeded its page budget")


def read_records(node, schema):
    keys = sorted(list_keys(node, schema))
    records = {}
    for start in range(0, len(keys), READ_BATCH):
        group = keys[start:start + READ_BATCH]
        page = node.call("/api/query", {
            "schema_name": schema, "filter": {"HashRangeKeys": group}, "limit": len(group),
        })[0]
        rows = page["results"]
        require(not page["has_more"] and len(rows) == len(group)
                and {key_pair(row["key"]) for row in rows} == set(group),
                "record read did not return exactly the listed keys")
        records.update((key_pair(row["key"]), row) for row in rows)
    return records


def policy_for_copy(node, definition):
    schema = definition["name"]
    report = retention(node, schema)
    policy = report["retention_policy"]
    if policy is None:
        return None
    if definition["schema_type"] in ("Hash", "Single"):
        raise RuntimeError("copy refuses write-time TTL: format 12 has no complete age transfer")
    require(report["anchor"] == schema and policy["source"] == "operator"
            and "overridden_manifest_ttl_seconds" not in policy,
            "copy requires an explicit, unshared operator range TTL")
    return policy


@dataclass
class SchemaImage:
    definition: dict
    policy: dict | None
    records: dict


def capture(node, schema):
    definition = node.call("/api/schema/" + quote(schema, safe=""))[0]["schema"]
    require(definition.pop("state").lower() == "available" and definition["name"] == schema,
            "copy requires an exact available source definition")
    return SchemaImage(definition, policy_for_copy(node, definition), read_records(node, schema))


def fetch_file(node, schema, key, marker):
    fetched = node.call("/api/db/fetch-blob", {"schema": schema, "key": key})[0]["file"]
    body_b64 = fetched.pop("bytes_b64")
    body = base64.b64decode(body_b64, validate=True)
    require(fetched == marker and hashlib.sha256(body).hexdigest() == marker["sha256"]
            and len(body) == marker["size_bytes"], "file bytes or metadata disagree with the marker")
    return body_b64


def put_file(node, request, body_b64, marker):
    reply = node.call("/api/db/put-blob", {
        "mutation": request, "bytes_b64": body_b64,
        "name": marker.get("name"), "media_type": marker.get("media_type"),
    })[0]
    return reply["file"]


def copy_live_records(source, target, image):
    schema = image.definition["name"]
    install(target, image.definition)
    require(not list_keys(target, schema), "copy target schema already contains data")
    if image.policy is not None:
        policy = retention(target, schema, "set", ttl_seconds=image.policy["ttl_seconds"],
                           hash_partitions=image.policy.get("hash_partitions", []))
        require(policy["retention_policy"] == image.policy, "copied range TTL policy changed")
    ordinary = []
    for row in image.records.values():
        metadata = row["metadata"] or {}
        request = mutation(schema, row["key"], row["fields"], "create", alternates=row["alternates"])
        request.update(source_file_name=metadata.get("source_file_name"),
                       metadata=metadata.get("metadata"))
        marker = metadata.get("file")
        if marker is None:
            ordinary.append(request)
        else:
            body_b64 = fetch_file(source, schema, row["key"], marker)
            require(put_file(target, request, body_b64, marker) == marker, "copied file marker changed")
    for start in range(0, len(ordinary), READ_BATCH):
        batch = ordinary[start:start + READ_BATCH]
        reply = target.call("/api/mutations/batch", {"mutations": batch})[0]
        require(len(reply["mutation_ids"]) == len(batch), "copy did not durably acknowledge the batch")


def comparable(records):
    return {key: {name: value for name, value in row.items() if name != "revision"}
            for key, row in records.items()}


def verify_copy(source, target, image):
    schema = image.definition["name"]
    restored = capture(target, schema)
    require(restored.definition == image.definition and restored.policy == image.policy,
            "schema definition or retention policy changed")
    require(comparable(restored.records) == comparable(image.records),
            "copy changed key membership, fields, metadata or alternate declarations")
    groups = set()
    for row in restored.records.values():
        for name, key in row["alternates"].items():
            if key is not None:
                groups.add((name, key["hash"]))
        marker = (row["metadata"] or {}).get("file")
        if marker is not None:
            fetch_file(target, schema, row["key"], marker)
    for name, lookup_hash in sorted(groups):
        require(list_keys(source, schema, name, lookup_hash)
                == list_keys(target, schema, name, lookup_hash),
                "copy changed the complete named alternate membership")
    print(f"{schema}: copied {len(restored.records)} records and verified complete key, "
          f"payload, metadata, file and {len(groups)} alternate groups", flush=True)


def seed(source):
    install(source, declaration(HASH_SCHEMA, ranged=False))
    install(source, declaration(RANGE_SCHEMA, ranged=True))
    file_request = mutation(HASH_SCHEMA, {"hash": "with-file"}, {"text": "file", "optional": None},
                            "create", alternates=SHARED_ALIAS)
    file_request.update(metadata={"origin": "format 12"}, source_file_name="source.txt")
    marker = {"name": "body.bin", "media_type": "application/octet-stream",
              "size_bytes": len(FILE_DATA), "sha256": hashlib.sha256(FILE_DATA).hexdigest()}
    require(put_file(source, file_request, base64.b64encode(FILE_DATA).decode(), marker) == marker,
            "source file marker differs from generated bytes")
    for key, fields, aliases in (
        ("other", {"text": "same alternate"}, SHARED_ALIAS),
        ("empty", {}, {"by_group": None}),
        ("removed", {"text": "deleted before export"}, SHARED_ALIAS),
    ):
        source.call("/api/mutation", mutation(HASH_SCHEMA, {"hash": key}, fields, "create",
                                               alternates=aliases))
    stale = conditioned(source, [mutation(HASH_SCHEMA, {"hash": "removed"}, {}, "delete")])[0]
    source.call("/api/mutation", stale)
    require(any(row["schema"] == HASH_SCHEMA and row["records_erased"] == 1
                for row in source.call("/api/db/delete-ledger")[0]["delete_ledger"]["entries"]),
            "source delete was absent from its audit ledger")
    now = int(time.time())
    old, fresh = f"{now - TTL_SECONDS - 5:020d}-old", f"{now:020d}-fresh"
    retention(source, RANGE_SCHEMA, "set", ttl_seconds=TTL_SECONDS)
    for key in (old, fresh):
        source.call("/api/mutation", mutation(RANGE_SCHEMA, {"hash": "clock", "range": key},
                                               {"text": key}, "create"))
    return stale, old, fresh


def verify_legacy_boundaries(source, target, stale):
    require(("removed", "") not in list_keys(target, HASH_SCHEMA),
            "copy revived a record deleted on the source")
    try:
        target.call("/api/mutation", stale)
    except RuntimeError as error:
        require(getattr(error, "status", None) == 409, "legacy condition refused for the wrong reason")
    else:
        raise RuntimeError("a legacy revision condition wrote to the new home")
    retention(source, HASH_SCHEMA, "set", ttl_seconds=TTL_SECONDS)
    try:
        capture(source, HASH_SCHEMA)
    except RuntimeError as error:
        require("format 12 has no complete age transfer" in str(error),
                "write-time TTL copy refused for the wrong reason")
    else:
        raise RuntimeError("copy admitted a schema whose original write times it cannot import")
    print("Deleted records stay absent, old revisions cannot write, and write-time TTL fails closed",
          flush=True)


def verify_range_age(node, old, fresh):
    deadline = time.monotonic() + 45
    while True:
        keys = list_keys(node, RANGE_SCHEMA)
        if ("clock", old) not in keys:
            require(keys == {("clock", fresh)}, "range TTL removed the fresh record")
            print("The target sweep preserved the source's range-key age across the copy", flush=True)
            return
        require(time.monotonic() < deadline, "import reset the old record's range-key age")
        time.sleep(0.25)


def owned_node(stack, binary, revision, output, root):
    output.mkdir(mode=0o700)
    node = Node(binary.resolve(strict=True), revision, output, temporary_directory=root, environment={
        "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_SELF_METRICS_SAMPLE_SECS": "1",
        "LASTDB_TTL_SWEEP": "0", "LASTDB_BYTE_BACKUP_V2_ENABLED": "false",
        "EXEMEM_API_URL": "http://127.0.0.1:9", "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
    })
    stack.callback(node.close)
    node.start()
    return node


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-daemon", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--target-daemon", type=Path, required=True)
    parser.add_argument("--target-revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product operation in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    output = args.output.resolve()
    require(output.is_relative_to(root) and not output.exists(), "output must be fresh under RUNNER_TEMP")
    output.mkdir(mode=0o700)
    with ExitStack() as stack:
        source = owned_node(stack, args.source_daemon, args.source_revision, output / "source", root)
        target = owned_node(stack, args.target_daemon, args.target_revision, output / "target", root)
        require(source.home_format() == 12 and target.home_format() == HOME_FORMAT == 13,
                "the two running daemons are not the expected format 12 and 13 pair")
        stale, old, fresh = seed(source)
        images = [capture(source, schema) for schema in (HASH_SCHEMA, RANGE_SCHEMA)]
        for image in images:
            copy_live_records(source, target, image)
        target.stop()
        target.start()
        for image in images:
            verify_copy(source, target, image)
        verify_legacy_boundaries(source, target, stale)
        target.stop()
        target.environment["LASTDB_TTL_SWEEP"] = "1"
        target.start()
        verify_range_age(target, old, fresh)


if __name__ == "__main__":
    main()
