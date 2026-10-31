#!/usr/bin/env python3
"""Exercise a format 7 (record model) home through a fresh CI daemon.

The daemon creates the home with LASTDB_RECORD_HOME=1. The script writes and
reads records with the selectors that the brain and LastSecrets apps use,
then restarts the daemon and reads again. It runs no test runner or
benchmark. The home contains generated data only and never reads or copies a
primary.
"""
import argparse
import os
from pathlib import Path
import sys
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node, RECORD_NODE_MARKER  # noqa: E402

FIELDS = ["id", "bucket", "value"]


def require(value, message):
    if not value:
        raise RuntimeError(message)


def install(node):
    for name, ranged in [("RecordById", False), ("RecordByBucket", True)]:
        definition = {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange" if ranged else "Hash",
            "key": {"hash_field": "bucket", "range_field": "id"} if ranged else {"hash_field": "id"},
            "fields": FIELDS,
            "field_types": {field: "String" for field in FIELDS},
            "field_descriptions": {field: "Record home example " + field for field in FIELDS},
            "field_versions": {field: 1 for field in FIELDS},
        }
        if not ranged:
            definition["alternate_keys"] = {"by_bucket": {"hash_field": "bucket", "range_field": "id"}}
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})


def mutate(node, schema, key, values, operation="update", expected=None):
    body = {
        "type": "mutation", "schema": schema, "key_value": key,
        "fields_and_values": values, "mutation_type": operation,
        "durability": "durable", "convergence": "sync",
    }
    if expected is not None:
        body["expected"] = expected
    return node.call("/api/mutation", body)[0]


def refused(call, words):
    try:
        call()
    except RuntimeError as error:
        require(words in str(error), "the call failed for another reason: " + str(error))
        return
    raise RuntimeError("the call did not fail: " + words)


def query(node, schema, selector, key_name=None):
    body = {"schema_name": schema, "fields": FIELDS, "filter": selector}
    if key_name is not None:
        body["key_name"] = key_name
    page = node.call("/api/query", body)[0]
    require(not page.get("has_more"), f"{schema} {selector} was truncated")
    return page["results"]


def ids(rows):
    return [row["fields"]["id"] for row in rows]


def one(node, key):
    rows = query(node, "RecordById", {"HashKey": key})
    require(len(rows) == 1, f"RecordById {key} returned {len(rows)} rows")
    return rows[0]["fields"]


def hash_records(node):
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": "v1"}, "create")
    require(one(node, "one") == {"id": "one", "bucket": "b1", "value": "v1"}, "the created record differs")
    mutate(node, "RecordById", {"hash": "one"}, {"value": "v2"})
    require(one(node, "one") == {"id": "one", "bucket": "b1", "value": "v2"},
            "an update of one field changed the other fields")
    for key in ["two", "three"]:
        mutate(node, "RecordById", {"hash": key}, {"id": key, "bucket": "b9", "value": key}, "create")
    rows = query(node, "RecordById", {"HashRangeKeys": [["one", ""], ["two", ""], ["missing", ""]]})
    require(sorted(ids(rows)) == ["one", "two"], "a batch read by key returned other records")
    print("Hash records: create, one-field update, batch read by key passed", flush=True)


def alternate_keys(node):
    def lookup(selector):
        return query(node, "RecordById", selector, key_name="by_bucket")
    require(ids(lookup({"HashRangeKey": {"hash": "b1", "range": "one"}})) == ["one"],
            "the alternate key did not find its record")
    mutate(node, "RecordById", {"hash": "one"}, {"bucket": "b2"})
    require(lookup({"HashRangeKey": {"hash": "b1", "range": "one"}}) == [],
            "the old alternate entry still resolves")
    rows = lookup({"HashRangeRange": {"hash": "b2", "start": "a", "end": "z"}})
    require(ids(rows) == ["one"] and rows[0]["fields"]["value"] == "v2",
            "the new alternate entry lost its record")
    require(sorted(ids(lookup({"HashKey": "b9"}))) == ["three", "two"],
            "the alternate hash read returned other records")
    print("Alternate keys: read, move on update, range read passed", flush=True)


def range_records(node):
    for key in ["c", "a", "e", "b", "d"]:
        mutate(node, "RecordByBucket", {"hash": "bk", "range": key},
               {"id": key, "bucket": "bk", "value": key.upper()}, "create")
    mutate(node, "RecordByBucket", {"hash": "other", "range": "a"},
           {"id": "a", "bucket": "other", "value": "OTHER"}, "create")
    require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "c", "d", "e"],
            "the records under one hash are missing or out of order")
    require(ids(query(node, "RecordByBucket", {"HashRangeRange": {"hash": "bk", "start": "b", "end": "d"}}))
            == ["b", "c"], "the range under one hash returned other records")
    require(ids(query(node, "RecordByBucket", {"HashRangePrefix": {"hash": "bk", "prefix": "c"}})) == ["c"],
            "the prefix under one hash returned other records")
    row = query(node, "RecordByBucket", {"HashRangeKey": {"hash": "other", "range": "a"}})
    require(len(row) == 1 and row[0]["fields"]["value"] == "OTHER", "a read by full key differs")
    print("HashRange records: hash, range, prefix and key reads passed", flush=True)


def preconditions(node):
    refused(lambda: mutate(node, "RecordById", {"hash": "one"}, {"value": "v3"},
                           expected={"type": "absent", "field": "value"}), "HTTP 4")
    require(one(node, "one")["value"] == "v2", "a refused CAS write changed the record")
    mutate(node, "RecordById", {"hash": "one"}, {"value": "v3"},
           expected={"type": "value", "field": "value", "value": "v2"})
    require(one(node, "one")["value"] == "v3", "a matching CAS write did not apply")
    mutate(node, "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b9", "value": "four"}, "create",
           expected={"type": "absent", "field": "value"})
    require(one(node, "four")["value"] == "four", "a create-if-absent write did not apply")
    refused(lambda: mutate(node, "RecordById", {"hash": "four"}, {}, "delete"), "format 7")
    require(one(node, "four")["value"] == "four", "a refused delete changed the record")
    print("CAS absent and value, and the delete refusal passed", flush=True)


def listing(node):
    keys, cursor = [], None
    for _ in range(10):
        target = "/api/list?schema=RecordById&limit=1"
        if cursor:
            target += "&cursor=" + urllib.parse.quote(cursor, safe="")
        page = node.call(target)[0]["list"]
        keys.extend(item["hash"] for item in page["keys"])
        cursor = page.get("next_cursor")
        if not page["has_more"]:
            break
    require(sorted(keys) == ["four", "one", "three", "two"], "the key list differs: " + repr(keys))
    print("Record key list with a cursor passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    output = args.output.resolve()
    require(output.is_relative_to(root) and not output.exists(), "output must be fresh under RUNNER_TEMP")
    output.mkdir(mode=0o700)
    node = Node(args.daemon.resolve(), args.revision, output, temporary_directory=root,
                environment={"LASTDB_RECORD_HOME": "1",
                             "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "0",
                             "LASTDB_BYTE_BACKUP_V2_ENABLED": "false",
                             "EXEMEM_API_URL": "http://127.0.0.1:9",
                             "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"})
    try:
        node.start()
        marker = (node.home / "data" / "laststore-pack-node-v1").read_text()
        require(marker == RECORD_NODE_MARKER, "the new home is not format 7: " + repr(marker))
        install(node)
        hash_records(node)
        alternate_keys(node)
        range_records(node)
        preconditions(node)
        listing(node)
        node.stop()
        node.start()
        require(one(node, "one") == {"id": "one", "bucket": "b2", "value": "v3"},
                "a record changed across the restart")
        require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "c", "d", "e"],
                "the records under one hash changed across the restart")
        print("Records survived a daemon restart", flush=True)
    finally:
        node.close()


if __name__ == "__main__":
    main()
