#!/usr/bin/env python3
"""Exercise a fresh CI record-model home across warm reads, TTL sweep, restarts, and refusals."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import time

_OPS = Path(__file__).resolve().parent
_SCRIPTS = _OPS.parent
sys.path[:0] = [str(_OPS), str(_SCRIPTS)]
from benchmarks.node import Node  # noqa: E402
from harness import HOME_FORMAT, marker_format, poll_until  # noqa: E402
import v1_records as v1  # noqa: E402
import record_app_api, record_inline_files, record_named_databases, record_runs  # noqa: E401,E402
import record_app_api_databases  # noqa: E402
from record_runs import (  # noqa: E402
    FIELDS, RANGED, definition, held, ids, ledger, listed_hashes, refused, require, system_seed,
)

# Long enough that a record rewritten at three quarters of its age is still
# fresh some seconds after the sweep deletes its expired neighbor.
TTL_SECONDS = 20


def install(node):
    for name, ranged in [("RecordById", False), ("RecordByBucket", True), ("RecordTtl", False)]:
        alternates = {"by_bucket": RANGED} if name == "RecordById" else {}
        system_seed(node, definition(name, "HashRange" if ranged else "Hash", FIELDS, alternates))


# RecordByBucket stores a range under one hash. A hash-only selector is that range, not one key.
HASH_RANGE_SCHEMAS = {"RecordByBucket"}


def mutate(node, schema, key, values, operation="replace", expected=None, alternates=None):
    """Return the revision /v1/write reported. Create does not read first."""
    item_key = v1.key_input(key)
    if alternates is None and schema == "RecordById" and values:
        alternates = {"by_bucket": {"hash": values["bucket"], "range": values["id"]}}
    revision = expected.get("revision") if isinstance(expected, dict) and expected.get("type") == "revision" else None
    if operation == "create":
        item = v1.create_item(schema, item_key, values, alternates=alternates)
        return v1.write_items(node, [item])["revisions"][0]
    if operation == "delete":
        item = {"schema": schema, "op": "delete", "key": item_key}
        if revision:
            item["revision"] = revision
        return v1.commit(node, [item])[0]
    item = {"schema": schema, "op": "replace", "key": item_key, "record": values}
    if alternates:
        item["alternates"] = alternates
    if revision:
        item["revision"] = revision
    return v1.commit(node, [item])[0]


def read_selector(schema, selector, key_name=None):
    options = {"via": key_name} if key_name else {}
    if "HashRangeKeys" in selector:
        items = []
        for pair in selector["HashRangeKeys"]:
            key = {"hash": pair[0]}
            if len(pair) > 1 and pair[1]:
                key["range"] = pair[1]
            items.append(v1.point(schema, key, **options))
        return items
    if "HashRangeKey" in selector:
        return v1.point(schema, selector["HashRangeKey"], **options)
    if "HashRangeRange" in selector:
        return v1.range_read(schema, selector["HashRangeRange"], **options)
    if "HashRangePrefix" in selector:
        return v1.range_read(schema, selector["HashRangePrefix"], **options)
    if key_name or schema in HASH_RANGE_SCHEMAS:
        return v1.range_read(schema, {"hash": selector["HashKey"]}, **options)
    return v1.point(schema, {"hash": selector["HashKey"]}, **options)


def query(node, schema, selector, key_name=None):
    selected = read_selector(schema, selector, key_name)
    pages = v1.read_all(node, selected) if isinstance(selected, list) else v1.read_items(node, [selected])
    for page in pages:
        require(page.get("next_cursor") is None, f"{schema} {selector} was truncated")
    return v1.flat_records(pages)


def one(node, key):
    rows = query(node, "RecordById", {"HashKey": key})
    require(len(rows) == 1, f"RecordById {key} returned {len(rows)} rows")
    # A field that the record does not hold reads as absent or null.
    return held(rows[0]["record"])


def node_key(node, cli):
    """Lowercase hex of this node's public key. The contact command reads the key file."""
    out = node.root / "node-contact.txt"
    result = subprocess.run(
        [str(cli), "--data-dir", str(node.home), "share", "contact", "--out", str(out)],
        capture_output=True, text=True, timeout=60, check=False,
        env={**os.environ, "LASTDB_HOME": str(node.home)},
    )
    require(result.returncode == 0, "lastdb share contact failed: " + result.stderr)
    key = json.loads(result.stdout)["recipient_public_key"]
    require(isinstance(key, str) and len(key) == 64 and set(key) <= set("0123456789abcdef"),
            "the node key is not lowercase hex")
    return key


def authorship(node, key, created_by, updated_by):
    row = query(node, "RecordById", {"HashKey": key})[0]
    require(row["created_by"] == created_by and row["updated_by"] == updated_by,
            f"{key} authorship differs: " + repr((row.get("created_by"), row.get("updated_by"))))


def hash_records(node, cli):
    author = node_key(node, cli)
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": "v1"}, "create")
    require(one(node, "one") == {"id": "one", "bucket": "b1", "value": "v1"}, "the created record differs")
    authorship(node, "one", author, author)
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": "v2"})
    require(one(node, "one") == {"id": "one", "bucket": "b1", "value": "v2"}, "the full replacement differs")
    authorship(node, "one", author, author)
    refused(lambda: mutate(node, "RecordById", {"hash": "one"},
                           {"id": "one", "bucket": "b1", "value": "v2", "created_by": author}),
            ("HTTP 400", "unknown_fields", "created_by"))
    require(one(node, "one")["value"] == "v2", "a refused created_by write changed the record")
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1"})
    require("value" not in one(node, "one"), "an omitted field survived replacement")
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": "v2"})
    for key in ["two", "three"]:
        mutate(node, "RecordById", {"hash": key}, {"id": key, "bucket": "b9", "value": key}, "create")
    rows = query(node, "RecordById", {"HashRangeKeys": [["one", ""], ["two", ""], ["missing", ""]]})
    require(sorted(ids(rows)) == ["one", "two"], "a batch read by key returned other records")
    external = {"by_bucket": {"hash": "external-alias", "range": "unrelated"}}
    mutate(node, "RecordById", {"hash": "external-key"}, {}, "create", alternates=external)
    row = query(node, "RecordById", {"HashKey": "external-key"})[0]
    require(held(row["record"]) == {}, "an empty record did not stay empty")
    mutate(node, "RecordById", {"hash": "external-key"}, {"value": "standalone"}, alternates=external)
    row = query(node, "RecordById", {"HashKey": "external-alias"}, key_name="by_bucket")[0]
    require(row["key"]["hash"] == "external-key" and held(row["record"]) == {"value": "standalone"},
            "independent primary/alternate addresses required payload key fields")
    external_boundaries(node, external)
    print("Hash records: create, full replacement, omitted-field removal, batch read by key, authorship passed", flush=True)


def external_boundaries(node, external):
    mutate(node, "RecordById", {"hash": "external-key-2"}, {}, "create", alternates=external)
    other = {"by_bucket": {"hash": "external-alias", "range": "unrelated\0tail"}}
    mutate(node, "RecordById", {"hash": "external-key-nul"}, {"value": "nul"}, "create", alternates=other)
    found, cursor = [], None
    for _ in range(4):
        read = v1.point("RecordById", {"hash": "external-alias", "range": "unrelated"},
                        via="by_bucket", limit=1, cursor=cursor)
        page = v1.read_items(node, [read])[0]
        found.extend(row["key"]["hash"] for row in page["records"])
        cursor = page.get("next_cursor")
        if cursor is None:
            break
    require(sorted(found) == ["external-key", "external-key-2"] and cursor is None,
            "duplicate alternate continuation included a different range or lost a primary")
    rows = query(node, "RecordById", {"HashRangeKey": other["by_bucket"]}, key_name="by_bucket")
    require(len(rows) == 1 and rows[0]["key"]["hash"] == "external-key-nul", "NUL range exact read differs")
    v1.commit(node, [{"schema": "RecordById", "op": "delete", "key": {"hash": name}}
                     for name in ("external-key", "external-key-2", "external-key-nul")])


def wide_empty_record_erasure(node):
    schema = "RecordWideEmpty"
    system_seed(node, definition(schema, "Hash", [], {"by_alias": {"kind": "hash"}}))
    key, aliases = {"hash": "k" * 65_000}, {"by_alias": {"hash": "wide-empty"}}
    created = mutate(node, schema, key, {}, "create", alternates=aliases)
    v1.write_items(node, [v1.delete_item(schema, key, created)])
    page = v1.read_items(node, [v1.point(schema, {"hash": "wide-empty"}, via="by_alias")])[0]
    require(page["records"] == [], "wide empty record retained its alternate after delete")
    print("Wide empty record: complete-address delete passed", flush=True)


def alternate_keys(node):
    lookup = lambda selector: query(node, "RecordById", selector, key_name="by_bucket")
    require(ids(lookup({"HashRangeKey": {"hash": "b1", "range": "one"}})) == ["one"], "the alternate key did not find its record")
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b2", "value": "v2"})
    require(lookup({"HashRangeKey": {"hash": "b1", "range": "one"}}) == [], "the old alternate entry still resolves")
    rows = lookup({"HashRangeRange": {"hash": "b2", "start": "a", "end": "z"}})
    require(ids(rows) == ["one"] and rows[0]["record"]["value"] == "v2", "the new alternate entry lost its record")
    require(sorted(ids(lookup({"HashKey": "b9"}))) == ["three", "two"], "the alternate hash read returned other records")
    print("Alternate keys: read, move on update, range read passed", flush=True)


def range_records(node):
    for key in ["c", "a", "e", "b", "d"]:
        mutate(node, "RecordByBucket", {"hash": "bk", "range": key}, {"id": key, "bucket": "bk", "value": key.upper()}, "create")
    mutate(node, "RecordByBucket", {"hash": "other", "range": "a"}, {"id": "a", "bucket": "other", "value": "OTHER"}, "create")
    require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "c", "d", "e"],
            "the records under one hash are missing or out of order")
    require(ids(query(node, "RecordByBucket", {"HashRangeRange": {"hash": "bk", "start": "b", "end": "d"}})) == ["b", "c"],
            "the range under one hash returned other records")
    require(ids(query(node, "RecordByBucket", {"HashRangePrefix": {"hash": "bk", "prefix": "c"}})) == ["c"],
            "the prefix under one hash returned other records")
    row = query(node, "RecordByBucket", {"HashRangeKey": {"hash": "other", "range": "a"}})
    require(len(row) == 1 and row[0]["record"]["value"] == "OTHER", "a read by full key differs")
    print("HashRange records: hash, range, prefix and key reads passed", flush=True)


def preconditions(node):
    fields = {"id": "one", "bucket": "b2", "value": "v3"}
    old = query(node, "RecordById", {"HashKey": "one"})[0]["revision"]
    refused(lambda: mutate(node, "RecordById", {"hash": "one"}, fields, "create", expected={"type": "absent"}), "HTTP 4")
    require(one(node, "one")["value"] == "v2", "a refused create changed the record")
    mutate(node, "RecordById", {"hash": "one"}, fields, expected={"type": "revision", "revision": old})
    refused(lambda: mutate(node, "RecordById", {"hash": "one"}, fields, expected={"type": "revision", "revision": old}), "HTTP 409")
    mutate(node, "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b9", "value": "four"}, "create")
    print("Expected-absent creation and whole-record revision refusal passed", flush=True)


def listing(node):
    keys = listed_hashes(node, "RecordById", limit=1)
    require(sorted(keys) == ["four", "one", "three", "two"], "the key list differs: " + repr(keys))
    print("Record key list with a cursor passed", flush=True)


def absent(node, key):
    return query(node, "RecordById", {"HashKey": key}) == []


def deletes(node):
    # Replay this exact body. A later read would see the recreated revision and hide the conflict.
    revision = query(node, "RecordById", {"HashKey": "four"})[0]["revision"]
    request = v1.delete_item("RecordById", {"hash": "four"}, revision)
    first = v1.write_items(node, [request])["revisions"][0]
    require(absent(node, "four"), "a deleted record still reads")
    require(sorted(ids(query(node, "RecordById", {"HashKey": "b9"}, key_name="by_bucket")))
            == ["three", "two"], "a deleted alternate entry still resolves")
    require(isinstance(first, str) and first, "a delete did not report a revision")
    refused(lambda: v1.write_items(node, [request]), "HTTP 409")
    mutate(node, "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b5"}, "create")
    require(one(node, "four") == {"id": "four", "bucket": "b5"}, "recreate brought back deleted content")
    refused(lambda: v1.write_items(node, [request]), "HTTP 409")
    fresh = v1.create_item(
        "RecordById", {"hash": "five"}, {"id": "five", "bucket": "b9", "value": "five"},
        alternates={"by_bucket": {"hash": "b9", "range": "five"}})
    refused(lambda: v1.write_items(node, [request, fresh]), "HTTP 409")
    require(absent(node, "five"), "a refused batch published its other record")
    accepted = v1.write_items(node, [fresh])
    require(accepted["written"] == 1 and accepted["revisions"][0] != first,
            "a create did not report its own revision")
    v1.commit(node, [
        {"schema": "RecordById", "op": "delete", "key": {"hash": "two"}},
        {"schema": "RecordByBucket", "op": "delete", "key": {"hash": "bk", "range": "c"}},
        {"schema": "RecordById", "op": "delete", "key": {"hash": "five"}},
    ])
    require(absent(node, "two") and absent(node, "five"), "a batch delete did not apply")
    require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "d", "e"],
            "a HashRange delete left its record")
    print("Conditional deletion, distinct revisions, stale replay, recreate and atomic batch passed", flush=True)


def native_status(node):
    reply = node.call("/api/db/native-status")[0]
    require("retry_cache" not in reply, "native status still exposes a retry cache")
    return reply["native_disk"]


def warm_records(node):
    mutate(node, "RecordById", {"hash": "warm"}, {"id": "warm", "bucket": "bw", "value": "w1"}, "create")
    before = native_status(node)
    for _ in range(3):
        require(one(node, "warm")["value"] == "w1", "a repeated read returned another value")
    after = native_status(node)
    hits = after["core_warm_hits"] - before["core_warm_hits"]
    require(hits >= 2, f"repeated reads by key hit memory {hits} times")
    require(after["warm_records"] >= 1 and after["core_warm_bytes"]["record_bytes"] > 0,
            "the warm set holds or charges no record: " + repr(after["core_warm_bytes"]))
    mutate(node, "RecordById", {"hash": "warm"}, {"id": "warm", "bucket": "bw", "value": "w2"})
    require(one(node, "warm")["value"] == "w2", "a read after a write returned the old record")
    mutate(node, "RecordById", {"hash": "warm"}, {}, "delete")
    require(absent(node, "warm"), "a read after a delete returned the deleted record")
    print(f"Record warm set: {hits} memory hits for 3 reads by key, "
          f"{after['warm_records']} records, {after['core_warm_bytes']['record_bytes']} bytes; "
          "read after write and after delete passed", flush=True)


def retention(node):
    reply = node.call("/api/db/schema-retention",
                      {"action": "set", "schema": "RecordTtl", "ttl_seconds": TTL_SECONDS})[0]
    require(reply["retention_policy"]["ttl_seconds"] == TTL_SECONDS, "the retention policy differs")
    for key in ["old", "kept"]:
        mutate(node, "RecordTtl", {"hash": key}, {"id": key, "bucket": "bt", "value": key}, "create")
    time.sleep(TTL_SECONDS * 3 / 4)
    mutate(node, "RecordTtl", {"hash": "kept"}, {"id": "kept", "bucket": "bt", "value": "kept again"})
    mutate(node, "RecordTtl", {"hash": "fresh"}, {"id": "fresh", "bucket": "bt", "value": "fresh"}, "create")
    kept_until = time.time() + TTL_SECONDS
    poll_until(TTL_SECONDS + 60, lambda: not query(node, "RecordTtl", {"HashKey": "old"}),
               interval=0.5, timeout_error="the TTL sweep did not delete the expired record")
    live = sorted(ids(query(node, "RecordTtl", {"HashRangeKeys": [["kept", ""], ["fresh", ""]]})))
    require(time.time() < kept_until, "the sweep was too slow to tell an old record from a rewritten one")
    require(live == ["fresh", "kept"], "the sweep deleted a record younger than the policy: " + repr(live))
    rows = [row for row in ledger(node) if row.get("schema") in {"RecordTtl", reply["anchor"]}]
    require(len(rows) == 1 and rows[0]["caller"] == "ttl-sweep-purge" and rows[0]["verb"] == "purge" and rows[0]["records_erased"] == 1,
            "the TTL sweep audit row differs: " + repr(rows))
    print("Retention: the TTL sweep deleted the expired record, kept a rewritten and a fresh record, and wrote its audit row", flush=True)


def delete_audit(node):
    rows = [row for row in ledger(node) if row["caller"] == "mutation-pipeline" and row["verb"] == "delete"]
    schemas = {row.get("schema") for row in rows if row["records_erased"] >= 1}
    require({"RecordById", "RecordByBucket"} <= schemas, "a record delete has no audit row: " + repr(rows))
    print(f"Delete audit: {len(rows)} delete rows for {sorted(schemas)} passed", flush=True)


def schema_count_refusal(node):
    """A schema list does not count records."""
    refused(lambda: node.call("/api/schemas?include_counts=true"), "HTTP 400")
    print("Schema list: include_counts=true is refused with HTTP 400", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for flag in ("--daemon", "--cli", "--output"):
        parser.add_argument(flag, type=Path, required=True)
    parser.add_argument("--revision", required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    output = args.output.resolve()
    require(output.is_relative_to(root) and not output.exists(), "output must be fresh under RUNNER_TEMP")
    output.mkdir(mode=0o700)
    env = {"LASTDB_TTL_SWEEP": "1", "LASTDB_TTL_SWEEP_INTERVAL_SECS": "1",
           "EXEMEM_API_URL": "http://127.0.0.1:9", "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"}
    node = Node(args.daemon.resolve(), args.revision, output, temporary_directory=root, environment=env)
    try:
        node.start()
        marker = (node.home / "data" / "laststore-pack-node-v1").read_text()
        require(marker_format(marker) == HOME_FORMAT, f"the new home is not format {HOME_FORMAT}: " + repr(marker))
        install(node)
        hash_records(node, args.cli.resolve(strict=True))
        wide_empty_record_erasure(node)
        alternate_keys(node)
        range_records(node)
        preconditions(node)
        listing(node)
        deletes(node)
        warm_records(node)
        delete_audit(node)
        retention(node)
        schema_count_refusal(node)
        record_runs.check(node)
        record_inline_files.exercise_drop_tracking(node)
        record_app_api.exercise(node)
        current = query(node, "RecordById", {"HashKey": "four"})[0]["revision"]
        retry = v1.replace_item(
            "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b5"}, current,
            alternates={"by_bucket": {"hash": "b5", "range": "four"}})
        acknowledged = v1.write_items(node, [retry])["revisions"][0]
        record_named_databases.prepare(node, args.cli.resolve(strict=True), output)
        record_app_api.named_database(node, "work")
        node.stop()
        record_inline_files.exercise_format_refusal(node)
        node.start()
        refused(lambda: v1.write_items(node, [retry]), "HTTP 409")
        require(query(node, "RecordById", {"HashKey": "four"})[0]["revision"] == acknowledged,
                "a stale replay changed the record after restart")
        record_inline_files.check_drop_tracking(node)
        require(one(node, "one") == {"id": "one", "bucket": "b2", "value": "v3"},
                "a record changed across the restart")
        require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "d", "e"],
                "the records under one hash changed across the restart")
        require(one(node, "four") == {"id": "four", "bucket": "b5"} and absent(node, "two"),
                "a delete or a write after a delete changed across the restart")
        require(query(node, "RecordTtl", {"HashKey": "old"}) == [], "an expired record came back")
        print("Records survived a daemon restart and expired records stayed absent", flush=True)
        record_named_databases.check(node, args.cli.resolve(strict=True), output)
        # The created database stays. The named-database list requires its own six rows.
        record_app_api_databases.exercise(node)
    finally:
        node.close()


if __name__ == "__main__":
    main()
