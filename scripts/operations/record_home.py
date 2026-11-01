#!/usr/bin/env python3
"""Exercise a format 7 (record model) home through a fresh CI daemon.

The daemon creates the home with LASTDB_RECORD_HOME=1. The script writes and
reads records with the selectors that the brain and LastSecrets apps use,
checks that repeated reads by key come from the record warm set, keeps field
history, lets the TTL sweep delete an expired record, reads the delete audit,
then restarts the daemon and reads again. It runs no test runner or
benchmark. The home contains generated data only and never reads or copies a
primary.
"""
import argparse
import os
from pathlib import Path
import sys
import time
import urllib.parse

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from node import Node, RECORD_NODE_MARKER  # noqa: E402

FIELDS = ["id", "bucket", "value"]
# Long enough that a record rewritten at three quarters of its age is still
# fresh some seconds after the sweep deletes its expired neighbor.
TTL_SECONDS = 20


def require(value, message):
    if not value:
        raise RuntimeError(message)


def install(node):
    for name, ranged in [("RecordById", False), ("RecordByBucket", True), ("RecordTtl", False)]:
        definition = {
            "source": "system_seed", "name": name, "descriptive_name": name,
            "schema_type": "HashRange" if ranged else "Hash",
            "key": {"hash_field": "bucket", "range_field": "id"} if ranged else {"hash_field": "id"},
            "fields": FIELDS,
            "field_types": {field: "String" for field in FIELDS},
            "field_descriptions": {field: "Record home example " + field for field in FIELDS},
            "field_versions": {field: 1 for field in FIELDS},
        }
        if name == "RecordById":
            definition["alternate_keys"] = {"by_bucket": {"hash_field": "bucket", "range_field": "id"}}
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})


def mutation(schema, key, values, operation="update", expected=None, must_exist=None):
    body = {
        "type": "mutation", "schema": schema, "key_value": key,
        "fields_and_values": values, "mutation_type": operation,
        "durability": "durable", "convergence": "sync",
    }
    if expected is not None:
        body["expected"] = expected
    if must_exist is not None:
        body["must_exist"] = must_exist
    return body


def mutate(node, schema, key, values, operation="update", **options):
    return node.call("/api/mutation", mutation(schema, key, values, operation, **options))[0]


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
    # A field that the record does not hold reads as absent or null.
    return {field: value for field, value in rows[0]["fields"].items() if value is not None}


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
    print("CAS absent and value passed", flush=True)


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


def absent(node, key):
    return query(node, "RecordById", {"HashKey": key}) == []


def deletes(node):
    mutate(node, "RecordById", {"hash": "four"}, {}, "delete")
    require(absent(node, "four"), "a deleted record still reads")
    require(sorted(ids(query(node, "RecordById", {"HashKey": "b9"}, key_name="by_bucket")))
            == ["three", "two"], "the alternate entry of a deleted record still resolves")
    mutate(node, "RecordById", {"hash": "four"}, {}, "delete")
    refused(lambda: mutate(node, "RecordById", {"hash": "four"}, {}, "delete", must_exist=True),
            "target not found")
    mutate(node, "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b5"}, "create")
    require(one(node, "four") == {"id": "four", "bucket": "b5"},
            "a write after a delete brought back a deleted value")
    refused(lambda: mutate(node, "RecordById", {"hash": "two"}, {}, "delete",
                           expected={"type": "value", "field": "value", "value": "other"}), "HTTP 4")
    require(one(node, "two")["value"] == "two", "a refused CAS delete changed the record")
    node.call("/api/mutations/batch", {"mutations": [
        mutation("RecordById", {"hash": "five"}, {"id": "five", "bucket": "b9", "value": "five"}, "create"),
        mutation("RecordById", {"hash": "two"}, {}, "delete",
                 expected={"type": "value", "field": "value", "value": "two"}),
        mutation("RecordByBucket", {"hash": "bk", "range": "c"}, {}, "delete"),
        mutation("RecordById", {"hash": "five"}, {}, "delete"),
    ]})
    require(absent(node, "two") and absent(node, "five"), "a delete in a batch did not apply")
    require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "d", "e"],
            "a HashRange delete left its record")
    keys = node.call("/api/list?schema=RecordById&limit=10")[0]["list"]["keys"]
    require(sorted(item["hash"] for item in keys) == ["four", "one", "three"],
            "the key list shows a deleted record: " + repr(keys))
    print("Delete, idempotent delete, must_exist miss, CAS delete, batch delete, "
          "write after delete passed", flush=True)


def native_status(node):
    return node.call("/api/db/native-status")[0]["native_disk"]


def warm_records(node):
    mutate(node, "RecordById", {"hash": "warm"}, {"id": "warm", "bucket": "bw", "value": "w1"}, "create")
    before = native_status(node)
    for _ in range(3):
        require(one(node, "warm")["value"] == "w1", "a repeated read returned another value")
    after = native_status(node)
    hits = after["core_warm_hits"] - before["core_warm_hits"]
    require(hits >= 2, f"repeated reads by key hit memory {hits} times")
    require(after["core_warm_records"] >= 1 and after["core_warm_bytes"]["record_bytes"] > 0,
            "the warm set holds or charges no record: " + repr(after["core_warm_bytes"]))
    mutate(node, "RecordById", {"hash": "warm"}, {"value": "w2"})
    require(one(node, "warm")["value"] == "w2", "a read after a write returned the old record")
    mutate(node, "RecordById", {"hash": "warm"}, {}, "delete")
    require(absent(node, "warm"), "a read after a delete returned the deleted record")
    print(f"Record warm set: {hits} memory hits for 3 reads by key, "
          f"{after['core_warm_records']} records, {after['core_warm_bytes']['record_bytes']} bytes; "
          "read after write and after delete passed", flush=True)


def ledger(node):
    return node.call("/api/db/delete-ledger")[0]["delete_ledger"]["entries"]


def history(node, action, key="hist"):
    return node.call("/api/atom-history", {
        "location": {"schema": "RecordById", "field": "value", "key": {"hash": key}},
        "action": action,
    })[0]


def field_history(node):
    reply = history(node, {"op": "policy", "policy": {"mode": "keep"}})
    require(reply["policy"] == {"mode": "keep"}, "the field history policy was not set")
    mutate(node, "RecordById", {"hash": "hist"}, {"id": "hist", "bucket": "bh", "value": "h1"}, "create")
    mutate(node, "RecordById", {"hash": "hist"}, {"value": "h1"})
    require(history(node, {"op": "list"})["entries"] == [],
            "a first write or a write of the same value added a history entry")
    mutate(node, "RecordById", {"hash": "hist"}, {"value": "h2"})
    page = history(node, {"op": "list"})
    entries = page["entries"]
    require(len(entries) == 1 and entries[0]["reason"] == "replace" and "atom" not in entries[0],
            "an overwrite did not add one copied entry: " + repr(entries))
    entry = entries[0]["id"]
    require(history(node, {"op": "get", "id": entry})["value"] == "h1", "the history entry holds another value")
    current = page["current"]["mutation"]
    history(node, {"op": "restore", "id": entry, "expected_mutation": current})
    require(one(node, "hist")["value"] == "h1", "the restore did not write the old value")
    refused(lambda: history(node, {"op": "restore", "id": entry, "expected_mutation": current}), "HTTP 4")
    reasons = sorted(item["reason"] for item in history(node, {"op": "list"})["entries"])
    require(reasons == ["replace", "replace"], "the restore did not keep the value it replaced: " + repr(reasons))
    mutate(node, "RecordById", {"hash": "kept"}, {"id": "kept", "bucket": "bh", "value": "k1"}, "create")
    mutate(node, "RecordById", {"hash": "kept"}, {}, "delete")
    deleted = history(node, {"op": "list"}, "kept")["entries"]
    require(len(deleted) == 1 and deleted[0]["reason"] == "delete",
            "a delete did not keep its value: " + repr(deleted))
    require(history(node, {"op": "get", "id": deleted[0]["id"]}, "kept")["value"] == "k1",
            "the delete entry holds another value")
    mutate(node, "RecordById", {"hash": "gone"}, {"id": "gone", "bucket": "bh", "value": "g1"}, "create")
    mutate(node, "RecordById", {"hash": "gone"}, {"value": "g2"})
    require(len(history(node, {"op": "list"}, "gone")["entries"]) == 1, "the purge target has no history")
    mutate(node, "RecordById", {"hash": "gone"}, {}, "purge")
    require(absent(node, "gone") and history(node, {"op": "list"}, "gone")["entries"] == [],
            "a record purge left the record or its history")
    rows = [row for row in ledger(node) if row.get("schema") == "RecordById" and row["verb"] == "purge"]
    require(len(rows) == 1 and rows[0]["committed"] and rows[0]["history_rows_deleted"] == 1
            and rows[0]["caller"] == "mutation-pipeline", "the purge audit row differs: " + repr(rows))
    print("Field history: copy on overwrite and delete, same value skip, get, guarded restore, "
          "release on record purge passed", flush=True)


def retention(node):
    reply = node.call("/api/db/schema-retention",
                      {"action": "set", "schema": "RecordTtl", "ttl_seconds": TTL_SECONDS})[0]
    require(reply["retention_policy"]["ttl_seconds"] == TTL_SECONDS, "the retention policy differs")
    for key in ["old", "kept"]:
        mutate(node, "RecordTtl", {"hash": key}, {"id": key, "bucket": "bt", "value": key}, "create")
    time.sleep(TTL_SECONDS * 3 / 4)
    mutate(node, "RecordTtl", {"hash": "kept"}, {"value": "kept again"})
    mutate(node, "RecordTtl", {"hash": "fresh"}, {"id": "fresh", "bucket": "bt", "value": "fresh"}, "create")
    kept_until = time.time() + TTL_SECONDS
    deadline = time.time() + TTL_SECONDS + 60
    while query(node, "RecordTtl", {"HashKey": "old"}):
        require(time.time() < deadline, "the TTL sweep did not delete the expired record")
        time.sleep(0.5)
    live = sorted(ids(query(node, "RecordTtl", {"HashRangeKeys": [["kept", ""], ["fresh", ""]]})))
    require(time.time() < kept_until, "the sweep was too slow to tell an old record from a rewritten one")
    require(live == ["fresh", "kept"], "the sweep deleted a record younger than the policy: " + repr(live))
    # The sweep erases through the schema that holds the policy.
    rows = [row for row in ledger(node) if row.get("schema") in {"RecordTtl", reply["anchor"]}]
    require(len(rows) == 1 and rows[0]["caller"] == "ttl-sweep-purge" and rows[0]["verb"] == "purge"
            and rows[0]["records_converged"] == 1 and rows[0]["committed"],
            "the TTL sweep audit row differs: " + repr(rows))
    print("Retention: the TTL sweep deleted the expired record, kept a rewritten and a fresh "
          "record, and wrote its audit row", flush=True)


def delete_audit(node):
    rows = [row for row in ledger(node) if row["caller"] == "mutation-pipeline" and row["verb"] == "delete"]
    schemas = {row.get("schema") for row in rows if row["committed"] and row["records_converged"] >= 1}
    require({"RecordById", "RecordByBucket"} <= schemas, "a record delete has no audit row: " + repr(rows))
    print(f"Delete audit: {len(rows)} delete rows for {sorted(schemas)} passed", flush=True)


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
                             "LASTDB_SELF_METRICS_TO_DB": "false", "LASTDB_TTL_SWEEP": "1",
                             # The TTL sweep runs from the sampler tick.
                             "LASTDB_SELF_METRICS_SAMPLE_SECS": "1",
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
        deletes(node)
        warm_records(node)
        delete_audit(node)
        field_history(node)
        retention(node)
        entries = history(node, {"op": "list"})["entries"]
        node.stop()
        node.start()
        require(one(node, "one") == {"id": "one", "bucket": "b2", "value": "v3"},
                "a record changed across the restart")
        require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "d", "e"],
                "the records under one hash changed across the restart")
        require(one(node, "four") == {"id": "four", "bucket": "b5"} and absent(node, "two"),
                "a delete or a write after a delete changed across the restart")
        require(history(node, {"op": "list"})["entries"] == entries, "field history changed across the restart")
        require(query(node, "RecordTtl", {"HashKey": "old"}) == [], "an expired record came back")
        released = history(node, {"op": "purge"})
        require(released["released"] == len(entries) and released["complete"]
                and history(node, {"op": "list"})["entries"] == [], "a history purge left entries")
        print("Records and field history survived a daemon restart; history purge passed", flush=True)
    finally:
        node.close()


if __name__ == "__main__":
    main()
