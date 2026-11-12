#!/usr/bin/env python3
"""Exercise a record model home through a fresh CI daemon.

Every new home uses the record model. The script writes and reads records with the
selectors that the brain and LastSecrets apps use, checks that repeated reads
by key come from the record warm set, lets the TTL sweep
delete an expired record, reads the delete audit, then restarts the daemon and
reads again. Before that restart it copies the stopped home and requires a
second daemon to refuse both a wrong node marker and a marker that claims an
older format. It also drains a schema of more than 10,000 records with
unfiltered cursor pages and records the time of each page, reads past a run
of more deleted records than one request examines, lets the TTL sweep read
past such a run, and drops a schema and installs it again (`record_runs.py`). Schema-owned
expiry tracking spans more than one native batch (`record_drop_tracking.py`). Blob and
inline files survive a restart (`record_inline_files.py`). Owned named
databases keep stable ids and isolated keys across that restart
(`record_named_databases.py`). LastDB Files imports
a file into a separate local publication home using the built CLI.
It runs no test runner or benchmark. The home contains generated data only and
never reads or copies a primary.
"""
import argparse
import json
import os
from pathlib import Path
import sys
import time
import urllib.parse
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "benchmarks"))
from home_format import HOME_FORMAT  # noqa: E402
from node import Node, marker_format, UnixConnection  # noqa: E402
import record_format_refusal  # noqa: E402
import record_runs  # noqa: E402
from record_requests import mutation as native_mutation, conditioned  # noqa: E402
import record_drop_tracking  # noqa: E402
import record_inline_files  # noqa: E402
import record_share_files  # noqa: E402
import record_named_databases  # noqa: E402

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
            "record_keys": {"primary": {"kind": "hash_range", "range_type": "String"} if ranged else {"kind": "hash"}, "alternates": {}},
            "fields": FIELDS,
            "field_types": {field: "String" for field in FIELDS},
            "field_descriptions": {field: "Record home example " + field for field in FIELDS},
            "field_versions": {field: 1 for field in FIELDS},
        }
        if name == "RecordById":
            definition["record_keys"]["alternates"] = {"by_bucket": {"kind": "hash_range", "range_type": "String"}}
        node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": definition})


def mutation(schema, key, values, operation="replace", expected=None, alternates=None):
    if alternates is None:
        alternates = ({"by_bucket": {"hash": values["bucket"], "range": values["id"]}}
                      if schema == "RecordById" and values else {})
    return native_mutation(schema, key, values, operation, alternates=alternates,
                           expected=expected or ({"type": "absent"} if operation == "create" else None))


def mutate(node, schema, key, values, operation="replace", **options):
    request = mutation(schema, key, values, operation, **options)
    return node.call("/api/mutation", conditioned(node, [request])[0])[0]


def refused(call, words):
    try:
        call()
    except RuntimeError as error:
        require(words in str(error), "the call failed for another reason: " + str(error))
        return
    raise RuntimeError("the call did not fail: " + words)


def query(node, schema, selector, key_name=None):
    body = {"schema_name": schema, "filter": selector}
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
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b1", "value": "v2"})
    require(one(node, "one") == {"id": "one", "bucket": "b1", "value": "v2"},
            "the full replacement differs")
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
    require(row["fields"] == {} and row["alternates"] == external, "empty record lost its explicit addresses")
    mutate(node, "RecordById", {"hash": "external-key"}, {"value": "standalone"}, alternates=external)
    row = query(node, "RecordById", {"HashKey": "external-alias"}, key_name="by_bucket")[0]
    require(row["key"]["hash"] == "external-key" and row["fields"] == {"value": "standalone"},
            "independent primary/alternate addresses required payload key fields")
    external_boundaries(node, external)
    print("Hash records: create, full replacement, omitted-field removal, batch read by key passed", flush=True)


def external_boundaries(node, external):
    mutate(node, "RecordById", {"hash": "external-key-2"}, {}, "create", alternates=external)
    other = {"by_bucket": {"hash": "external-alias", "range": "unrelated\0tail"}}
    mutate(node, "RecordById", {"hash": "external-key-nul"}, {"value": "nul"}, "create", alternates=other)
    base = {"schema_name": "RecordById", "key_name": "by_bucket",
            "filter": {"HashRangeKey": {"hash": "external-alias", "range": "unrelated"}}, "limit": 1}
    found, cursor = [], None
    for _ in range(4):
        page = node.call("/api/query", {**base, **({"cursor": cursor} if cursor else {})})[0]
        found.extend(row["key"]["hash"] for row in page["results"])
        cursor = page["next_cursor"]
        if not page["has_more"]:
            break
    require(sorted(found) == ["external-key", "external-key-2"] and cursor is None,
            "duplicate alternate continuation included a different range or lost a primary")
    rows = query(node, "RecordById", {"HashRangeKey": other["by_bucket"]}, key_name="by_bucket")
    require(len(rows) == 1 and rows[0]["key"]["hash"] == "external-key-nul", "NUL range exact read differs")
    requests = [mutation("RecordById", {"hash": name}, {}, "delete")
                for name in ("external-key", "external-key-2", "external-key-nul")]
    node.call("/api/mutations/batch", {"mutations": conditioned(node, requests)})


def wide_empty_record_erasure(node):
    schema = "RecordWideEmpty"
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": schema, "descriptive_name": schema,
        "schema_type": "Hash", "fields": [], "field_types": {},
        "record_keys": {"primary": {"kind": "hash"},
                        "alternates": {"by_alias": {"kind": "hash"}}},
    }})
    key, aliases = {"hash": "k" * 65_000}, {"by_alias": {"hash": "wide-empty"}}
    created = mutate(node, schema, key, {}, "create", alternates=aliases)
    request = mutation(schema, key, {}, "delete", alternates=aliases,
                       expected={"type": "revision", "revision": created["record_revision"]})
    deleted = node.call("/api/mutation", request)[0]
    request.update(mutation_type="purge",
                   expected={"type": "revision", "revision": deleted["record_revision"]})
    node.call("/api/mutation", request)
    reply = node.call("/api/query", {"schema_name": schema,
        "filter": {"HashKey": "wide-empty"}, "key_name": "by_alias"})[0]
    require(reply["results"] == [], "wide empty record retained its alternate after erasure")
    print("Wide empty record: complete-address delete and revision-guarded tombstone purge passed", flush=True)


def alternate_keys(node):
    def lookup(selector):
        return query(node, "RecordById", selector, key_name="by_bucket")
    require(ids(lookup({"HashRangeKey": {"hash": "b1", "range": "one"}})) == ["one"],
            "the alternate key did not find its record")
    mutate(node, "RecordById", {"hash": "one"}, {"id": "one", "bucket": "b2", "value": "v2"})
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
    fields = {"id": "one", "bucket": "b2", "value": "v3"}
    old = query(node, "RecordById", {"HashKey": "one"})[0]["revision"]
    refused(lambda: mutate(node, "RecordById", {"hash": "one"}, fields, "create",
                           expected={"type": "absent"}), "HTTP 4")
    require(one(node, "one")["value"] == "v2", "a refused create changed the record")
    mutate(node, "RecordById", {"hash": "one"}, fields,
           expected={"type": "revision", "revision": old})
    refused(lambda: mutate(node, "RecordById", {"hash": "one"}, fields,
                           expected={"type": "revision", "revision": old}), "HTTP 409")
    mutate(node, "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b9", "value": "four"}, "create")
    print("Expected-absent creation and whole-record revision refusal passed", flush=True)


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
    request = conditioned(node, [mutation("RecordById", {"hash": "four"}, {}, "delete")])[0]
    first = node.call("/api/mutation", request)[0]
    require(absent(node, "four"), "a deleted record still reads")
    require(sorted(ids(query(node, "RecordById", {"HashKey": "b9"}, key_name="by_bucket")))
            == ["three", "two"], "a deleted alternate entry still resolves")
    uuid.UUID(first["mutation_id"])
    refused(lambda: node.call("/api/mutation", request), "HTTP 409")
    mutate(node, "RecordById", {"hash": "four"}, {"id": "four", "bucket": "b5"}, "create")
    require(one(node, "four") == {"id": "four", "bucket": "b5"}, "recreate brought back deleted content")
    refused(lambda: node.call("/api/mutation", request), "HTTP 409")
    fresh = mutation("RecordById", {"hash": "five"}, {"id": "five", "bucket": "b9", "value": "five"}, "create")
    refused(lambda: node.call("/api/mutations/batch", {"mutations": [request, fresh]}), "HTTP 409")
    require(absent(node, "five"), "a refused batch published its other record")
    accepted = node.call("/api/mutation", fresh)[0]
    require(accepted["mutation_id"] != first["mutation_id"] and accepted["operations"] ==
            {"created": 1, "updated": 0, "deleted": 0, "total": 1}, "server identity or write counts differ")
    requests = [mutation("RecordById", {"hash": "two"}, {}, "delete"),
                mutation("RecordByBucket", {"hash": "bk", "range": "c"}, {}, "delete"),
                mutation("RecordById", {"hash": "five"}, {}, "delete")]
    node.call("/api/mutations/batch", {"mutations": conditioned(node, requests)})
    require(absent(node, "two") and absent(node, "five"), "a batch delete did not apply")
    require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "d", "e"],
            "a HashRange delete left its record")
    print("Conditional deletion, server identities, stale replay, recreate and atomic batch passed", flush=True)


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


def ledger(node):
    return node.call("/api/db/delete-ledger")[0]["delete_ledger"]["entries"]


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
            and rows[0]["records_erased"] == 1,
            "the TTL sweep audit row differs: " + repr(rows))
    print("Retention: the TTL sweep deleted the expired record, kept a rewritten and a fresh "
          "record, and wrote its audit row", flush=True)


def delete_audit(node):
    rows = [row for row in ledger(node) if row["caller"] == "mutation-pipeline" and row["verb"] == "delete"]
    schemas = {row.get("schema") for row in rows if row["records_erased"] >= 1}
    require({"RecordById", "RecordByBucket"} <= schemas, "a record delete has no audit row: " + repr(rows))
    print(f"Delete audit: {len(rows)} delete rows for {sorted(schemas)} passed", flush=True)


def query_refusals(node):
    """Unfiltered scans, full-scan headers, and post-key filters are refused with HTTP 400."""
    refused(lambda: node.call("/api/query", {"schema_name": "RecordById", "filter": None}),
            "full_schema_scan_not_allowed")
    headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-benchmark",
               "X-LastDB-Allow-Full-Scan": "true"}
    connection = UnixConnection(node.socket)
    try:
        connection.request("POST", "/api/query",
                           json.dumps({"schema_name": "RecordById", "filter": None}).encode(),
                           headers)
        response = connection.getresponse()
        raw = response.read()
    finally:
        connection.close()
    require(response.status == 400 and b"full_schema_scan_not_allowed" in raw,
            f"X-LastDB-Allow-Full-Scan header was not refused: {response.status} {raw[:300]!r}")
    for bad_body in [
        {"schema_name": "RecordById", "filter": {"HashKey": "one"},
         "where": [{"eq": {"field": "bucket", "value": "b2"}}]},
        {"schema_name": "RecordById", "filter": {"HashKey": "one"},
         "value_filters": [{"Equals": {"field": "id", "value": 1.0}}]},
        {"schema_name": "RecordById", "filter": {"HashKey": "one"},
         "order_by": {"field": "id"}},
        {"schema_name": "RecordById", "filter": {"SampleN": 3}},
    ]:
        refused(lambda body=bad_body: node.call("/api/query", body), "HTTP 400")
    refused(lambda: node.call("/api/schemas?include_counts=true"), "HTTP 400")
    print("Query refusals: unfiltered scan, X-LastDB-Allow-Full-Scan, where, value_filters, "
          "order_by, SampleN, and schemas?include_counts=true all refused with HTTP 400",
          flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--daemon", type=Path, required=True)
    parser.add_argument("--cli", type=Path, required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    output = args.output.resolve()
    require(output.is_relative_to(root) and not output.exists(), "output must be fresh under RUNNER_TEMP")
    output.mkdir(mode=0o700)
    node = Node(args.daemon.resolve(), args.revision, output, temporary_directory=root,
                environment={"LASTDB_TTL_SWEEP": "1",
                             "LASTDB_TTL_SWEEP_INTERVAL_SECS": "1",
                             "LASTDB_BYTE_BACKUP_V2_ENABLED": "false",
                             "EXEMEM_API_URL": "http://127.0.0.1:9",
                             "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9"})
    try:
        node.start()
        marker = (node.home / "data" / "laststore-pack-node-v1").read_text()
        require(marker_format(marker) == HOME_FORMAT, f"the new home is not format {HOME_FORMAT}: " + repr(marker))
        install(node)
        hash_records(node)
        wide_empty_record_erasure(node)
        alternate_keys(node)
        range_records(node)
        preconditions(node)
        listing(node)
        deletes(node)
        warm_records(node)
        delete_audit(node)
        retention(node)
        query_refusals(node)
        record_runs.check(node)
        record_drop_tracking.exercise(node)
        record_inline_files.exercise(node)
        retry = conditioned(node, [mutation("RecordById", {"hash": "four"},
            {"id": "four", "bucket": "b5"})])[0]
        acknowledged = node.call("/api/mutation", retry)[0]["record_revision"]
        record_named_databases.prepare(node, args.cli.resolve(strict=True), output)
        node.stop()
        record_format_refusal.exercise(node)
        node.start()
        refused(lambda: node.call("/api/mutation", retry), "HTTP 409")
        require(query(node, "RecordById", {"HashKey": "four"})[0]["revision"] == acknowledged,
                "a stale replay changed the record after restart")
        record_drop_tracking.check(node)
        record_inline_files.check(node)
        require(one(node, "one") == {"id": "one", "bucket": "b2", "value": "v3"},
                "a record changed across the restart")
        require(ids(query(node, "RecordByBucket", {"HashKey": "bk"})) == ["a", "b", "d", "e"],
                "the records under one hash changed across the restart")
        require(one(node, "four") == {"id": "four", "bucket": "b5"} and absent(node, "two"),
                "a delete or a write after a delete changed across the restart")
        require(query(node, "RecordTtl", {"HashKey": "old"}) == [], "an expired record came back")
        print("Records survived a daemon restart and expired records stayed absent", flush=True)
        record_named_databases.check(node, args.cli.resolve(strict=True), output)
        record_share_files.exercise(args.cli.resolve(strict=True), node)
    finally:
        node.close()


if __name__ == "__main__":
    main()
