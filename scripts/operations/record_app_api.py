"""The app API on a fresh generated home: POST /v1/read and POST /v1/write.

A write carries a condition. A create writes only when no record holds the key.
A replace and a delete name the revision the caller read, and write only when
the record is still at it. One list is all or nothing. A read names a key or a
range under one hash, and each read in a list gets its own result or its own
error.
"""
import json
import os
from pathlib import Path
import sys

_OPS = Path(__file__).resolve().parent
sys.path[:0] = [str(_OPS), str(_OPS.parent)]
from harness import send_unix_request  # noqa: E402
from record_runs import FIELDS, RANGED, definition, refused, require, system_seed  # noqa: E402

BY_ID, BY_BUCKET = "AppApiById", "AppApiByBucket"
MISSING_DATABASE = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"


def read(node, *reads):
    return node.call("/v1/read", {"reads": list(reads)})[0]["results"]


def write(node, *mutations):
    reply = node.call("/v1/write", {"mutations": list(mutations)})[0]
    require(reply["written"] == len(mutations) == len(reply["revisions"]),
            "a write did not report one revision for each mutation: " + repr(reply))
    return reply["revisions"]


def create(schema, key, record, alternates=None, database=None):
    item = {"schema": schema, "op": "create", "key": key, "record": record}
    if alternates is not None:
        item["alternates"] = alternates
    if database is not None:
        item["database"] = database
    return item


def by_id(name, bucket, value, schema=BY_ID, database=None):
    return create(schema, {"hash": name}, {"id": name, "bucket": bucket, "value": value},
                  {"by_bucket": {"hash": bucket, "range": name}}, database)


def replace(name, bucket, value, revision):
    return {**by_id(name, bucket, value), "op": "replace", "revision": revision}


def delete(schema, key, revision):
    return {"schema": schema, "op": "delete", "key": key, "revision": revision}


def held(record):
    return {field: value for field, value in record.items() if value is not None}


def stored(node, name, schema=BY_ID, database=None):
    item = {"schema": schema, "key": {"hash": name}}
    if database is not None:
        item["database"] = database
    records = read(node, item)[0]["records"]
    require(len(records) <= 1, f"{schema} {name} returned {len(records)} records")
    return records[0] if records else None


def value_of(node, name, **options):
    row = stored(node, name, **options)
    return held(row["record"]).get("value") if row else None


def error_body(node, path, body):
    status, raw, _, _, _ = send_unix_request(
        node.socket, "POST", path, body, {"Content-Type": "application/json"})
    return status, json.loads(raw)


def conflict(node, *mutations):
    status, reply = error_body(node, "/v1/write", {"mutations": list(mutations)})
    require(status == 409 and reply["error"]["code"] == "conflict",
            f"a write whose condition does not hold was not refused: {status} {reply!r}")


def writes(node):
    first = write(node, by_id("one", "b1", "v1"))[0]
    require(stored(node, "one")["revision"] == first, "a read returned another revision than the write")
    conflict(node, by_id("one", "b1", "again"))
    require(value_of(node, "one") == "v1", "a create over a record changed it")
    second = write(node, replace("one", "b1", "v2", first))[0]
    require(second != first and value_of(node, "one") == "v2", "a replace at the read revision was not written")
    conflict(node, replace("one", "b1", "stale", first))
    require(value_of(node, "one") == "v2", "a replace at a stale revision changed the record")
    require(write(node, replace("one", "b1", "v3", second), by_id("dup", "b1", "d")) and
            value_of(node, "one") == "v3" and value_of(node, "dup") == "d", "a list of two mutations was not written")
    conflict(node, by_id("two", "b1", "v"), by_id("one", "b1", "again"))
    require(value_of(node, "two") is None, "a refused list wrote one of its records")
    stray = create(BY_ID, {"hash": "three"}, {"nope": "x"}, {"by_bucket": {"hash": "b1", "range": "three"}})
    refused(lambda: write(node, by_id("two", "b1", "v"), stray), "HTTP 400")
    require(value_of(node, "two") is None, "a list with an invalid record wrote its other record")
    print("App API writes: create only when absent, replace only at the read revision, "
          "and a refused list writes nothing", flush=True)


def deletes(node):
    lookup = {"schema": BY_ID, "via": "by_bucket", "range": {"hash": "b1"}}
    found = sorted(row["key"]["hash"] for row in read(node, lookup)[0]["records"])
    require(found == ["dup", "one"], "the alternate key read differs: " + repr(found))
    revision = stored(node, "one")["revision"]
    conflict(node, delete(BY_ID, {"hash": "one"}, "0" * len(revision)))
    require(value_of(node, "one") == "v3", "a delete at another revision removed the record")
    write(node, delete(BY_ID, {"hash": "one"}, revision))
    require(stored(node, "one") is None, "a deleted record still reads")
    found = [row["key"]["hash"] for row in read(node, lookup)[0]["records"]]
    require(found == ["dup"], "a deleted alternate entry still resolves: " + repr(found))
    conflict(node, delete(BY_ID, {"hash": "one"}, revision))
    write(node, by_id("one", "b3", "back"))
    require(value_of(node, "one") == "back", "a create after a delete differs")
    print("App API deletes: delete at the read revision, refusal at another, and create after delete passed",
          flush=True)


def ranges(node):
    write(node, *[create(BY_BUCKET, {"hash": "bk", "range": key},
                         {"id": key, "bucket": "bk", "value": key.upper()})
                  for key in "caebd"])
    keys, cursor = [], None
    for _ in range(10):
        item = {"schema": BY_BUCKET, "range": {"hash": "bk"}, "limit": 2, "keys_only": True}
        if cursor is not None:
            item["cursor"] = cursor
        page = read(node, item)[0]
        require("records" not in page, "a keys-only read returned records")
        keys.extend(key["range"] for key in page["keys"])
        cursor = page["next_cursor"]
        if cursor is None:
            break
    require(keys == list("abcde") and cursor is None, "the keys under one hash differ: " + repr(keys))
    pages = read(node,
                 {"schema": BY_BUCKET, "range": {"hash": "bk", "prefix": "c"}},
                 {"schema": BY_BUCKET, "range": {"hash": "bk", "start": "b", "end": "d"}},
                 {"schema": BY_BUCKET, "key": {"hash": "bk", "range": "e"}},
                 {"schema": BY_BUCKET, "key": {"hash": "bk", "range": "missing"}})
    got = [[row["key"]["range"] for row in page["records"]] for page in pages]
    require(got == [["c"], ["b", "c"], ["e"], []], "the range reads differ: " + repr(got))
    require(held(pages[2]["records"][0]["record"]) == {"id": "e", "bucket": "bk", "value": "E"},
            "a read by full key returned another record")
    print("App API reads: key, range, prefix, bounds, keys only and cursor passed", flush=True)


def refusals(node):
    results = read(node,
                   {"schema": BY_ID, "key": {"hash": "dup"}},
                   {"schema": BY_ID},
                   {"schema": BY_ID, "key": {"hash": "dup"}, "database": MISSING_DATABASE})
    require(len(results[0]["records"]) == 1, "a good read beside a refused read returned nothing")
    require(results[1]["error"]["code"] == "invalid_request", "a read with no key was not refused")
    require(results[2]["error"]["code"] == "not_found", "a read of an absent database was not refused")
    for path, body in [("/v1/read", {"reads": []}), ("/v1/write", {"mutations": []}),
                       ("/v1/read", {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}, "where": []}]}),
                       ("/v1/write", {"mutations": [{"schema": BY_ID, "op": "create", "key": {"hash": "x"}}]}),
                       ("/v1/write", {"mutations": [{**by_id("x", "b1", "v"), "op": "put"}]}),
                       ("/v1/write", {"mutations": [{**by_id("x", "b1", "v"), "op": "replace"}]}),
                       ("/v1/write", {"mutations": [{"schema": BY_ID, "op": "delete", "key": {"hash": "dup"}}]}),
                       ("/v1/write", {"mutations": [{**by_id("x", "b1", "v"), "revision": "r"}]}),
                       ("/v1/write", {"mutations": [by_id("x", "b1", "v"),
                                                    by_id("y", "b1", "v", database=MISSING_DATABASE)]})]:
        status, reply = error_body(node, path, body)
        require(status == 400 and reply["error"]["code"] == "invalid_request" and reply["error"]["message"],
                f"{path} {body!r} was not refused in the one error shape: {status} {reply!r}")
    status, reply = error_body(node, "/v1/write", {"mutations": [by_id("x", "b1", "v", database=MISSING_DATABASE)]})
    require(status == 404 and reply["error"]["code"] == "not_found",
            f"a write to an absent database was not refused: {status} {reply!r}")
    require(value_of(node, "x") is None, "a refused write wrote a record")
    print("App API refusals: each read has its own error, and a refusal has one shape", flush=True)


def files(node):
    """A file is a path in and a path out. No bytes pass through the app API."""
    folder = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True) / f"app-api-files-{os.getpid()}"
    folder.mkdir(mode=0o700)
    original = folder / "whiteboard.txt"
    original.write_bytes(b"app api file by path\n" * 64)
    link = {"path": str(original), "name": "whiteboard.txt", "media_type": "text/plain"}
    with_path = lambda name: read(node, {"schema": BY_ID, "key": {"hash": name}, "file": True})[0]
    first = write(node, {**by_id("filed", "b8", "v1"), "file": link})[0]
    plain = stored(node, "filed")["file"]
    require(plain == {"name": "whiteboard.txt", "media_type": "text/plain", "size": original.stat().st_size},
            "a read did not report the linked file: " + repr(plain))
    linked = with_path("filed")["records"][0]["file"]
    require(linked == {**plain, "path": str(original)}, "a read did not return the file's path: " + repr(linked))
    second = write(node, replace("filed", "b8", "v2", first))[0]
    require(value_of(node, "filed") == "v2" and with_path("filed")["records"][0]["file"] == linked,
            "a replace without a file dropped the record's file")
    require(stored(node, "dup")["file"] is None and with_path("dup")["records"][0]["file"] is None,
            "a record without a file reported one")
    for item in [{**by_id("nofile", "b8", "v"), "file": {"path": str(folder / "absent.bin")}},
                 {**by_id("nofile", "b8", "v"), "file": {"path": str(node.home / "data" / "laststore-pack-node-v1")}},
                 {**by_id("nofile", "b8", "v"), "file": {"path": str(original), "bytes_b64": "QQ=="}},
                 {**delete(BY_ID, {"hash": "filed"}, second), "file": link}]:
        status, reply = error_body(node, "/v1/write", {"mutations": [item]})
        require(status == 400 and reply["error"]["code"] == "invalid_request",
                f"a file link that cannot be used was not refused: {status} {reply!r}")
    require(value_of(node, "nofile") is None, "a refused file write wrote its record")
    original.unlink()
    lost = with_path("filed")
    require(lost.get("error", {}).get("code") == "not_found", "a missing original was not reported: " + repr(lost))
    require(stored(node, "filed")["file"] == plain, "a read without a path failed for a missing original")
    write(node, delete(BY_ID, {"hash": "filed"}, second))
    folder.rmdir()
    print("App API files: a path on create, the same path on read, kept across a replace, "
          "refused inside the node home, and reported when the original is gone", flush=True)


def changed(node, **body):
    return node.call("/v1/x/changes", body)[0]


def changes(node, database=None):
    """The changes extension: the numbered list of changed record keys, read with a JSON body."""
    scope = {} if database is None else {"database": database}
    schema = BY_ID if database is None else "RecordById"
    start = changed(node, limit=0, **scope)
    require(start["changes"] == [] and isinstance(start["head"], int), "a head-only read returned entries")
    first = write(node, by_id("feed", "b7", "v1", schema=schema, database=database))[0]
    write(node, {**delete(schema, {"hash": "feed"}, first), **scope})
    page = changed(node, after=start["head"], schemas=[schema], **scope)
    got = [(entry["key"]["hash"], entry["deleted"]) for entry in page["changes"]]
    require(got == [("feed", False), ("feed", True)], "the changed keys differ: " + repr(page))
    require(page["id"] == start["id"] and page["gap"] is False
            and page["through"] >= page["changes"][-1]["seq"] > start["head"],
            "the changes page does not follow the head it was read after: " + repr(page))
    require(changed(node, after=page["through"], schemas=[schema], **scope)["changes"] == [],
            "a read after the saved position returned an entry again")
    if database is None:
        require(changed(node, after=start["head"], schemas=[BY_BUCKET])["changes"] == [],
                "a schema filter returned another schema's entries")
        require(changed(node, after=page["head"] + 1000)["gap"] is True,
                "a position past the head did not report a gap")
        for body in [{"after": 0, "where": []}, {"schemas": [""]}, {"after": -1}]:
            status, reply = error_body(node, "/v1/x/changes", body)
            require(status == 400 and reply["error"]["code"] == "invalid_request",
                    f"/v1/x/changes {body!r} was not refused in the one error shape: {status} {reply!r}")
    print("App API changes extension: head, entries with keys in commit order, schema filter, "
          f"position and gap passed ({database or 'personal'})", flush=True)


def extension_names(node):
    """GET /api/version lists the app API extensions this node serves."""
    extensions = node.call("/api/version")[0]["extensions"]
    require(extensions == ["changes"],
            "GET /api/version did not report extensions [changes]: " + repr(extensions))
    print("App API extensions: GET /api/version reports [changes]", flush=True)


def exercise(node):
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    extension_names(node)
    system_seed(node, definition(BY_ID, "Hash", FIELDS, {"by_bucket": RANGED}))
    system_seed(node, definition(BY_BUCKET, "HashRange", FIELDS))
    writes(node)
    deletes(node)
    ranges(node)
    refusals(node)
    files(node)
    changes(node)


def named_database(node, database):
    """A database identifier on an item reads and writes that database, not personal."""
    require(os.environ.get("GITHUB_ACTIONS") == "true", "run this product exercise in CI")
    write(node, by_id("app-api", "b1", "named", schema="RecordById", database=database))
    require(value_of(node, "app-api", schema="RecordById", database=database) == "named",
            "a named database did not return its record")
    require(value_of(node, "app-api", schema="RecordById") is None,
            "a write to a named database reached personal")
    changes(node, database)
    print("App API database identifier: a write and a read reached the named database only", flush=True)
