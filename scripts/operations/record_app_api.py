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
from record_runs import (  # noqa: E402
    FIELDS, MISSING_DATABASE, RANGED, definition, held, refused, require, system_seed,
)
import app_api_schema  # noqa: E402
import benchmarks.node as _bench_node  # noqa: E402
import record_app_api_all  # noqa: E402
import record_app_api_calls  # noqa: E402
import record_app_api_owner_file  # noqa: E402
import record_app_api_queue  # noqa: E402

app_api_schema.install(sys.modules[__name__], _bench_node, record_app_api_calls)

from record_app_api_calls import (  # noqa: E402
    BY_BUCKET, BY_ID, by_id, conflict, create, delete, error_body, read, replace, stored, value_of, write,
)


def writes(node):
    first = write(node, by_id("one", "b1", "v1"))[0]
    require(stored(node, "one")["revision"] == first, "a read returned another revision than the write")
    conflict(node, by_id("one", "b1", "again"), index=0, reason="exists")
    require(value_of(node, "one") == "v1", "a create over a record changed it")
    second = write(node, replace("one", "b1", "v2", first))[0]
    require(second != first and value_of(node, "one") == "v2", "a replace at the read revision was not written")
    conflict(node, replace("one", "b1", "stale", first), index=0, reason="revision")
    require(value_of(node, "one") == "v2", "a replace at a stale revision changed the record")
    require(write(node, replace("one", "b1", "v3", second), by_id("dup", "b1", "d")) and
            value_of(node, "one") == "v3" and value_of(node, "dup") == "d", "a list of two mutations was not written")
    conflict(node, by_id("two", "b1", "v"), by_id("one", "b1", "again"), index=1, reason="exists")
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
    conflict(node, delete(BY_ID, {"hash": "one"}, "0" * len(revision)), index=0, reason="revision")
    require(value_of(node, "one") == "v3", "a delete at another revision removed the record")
    write(node, delete(BY_ID, {"hash": "one"}, revision))
    require(stored(node, "one") is None, "a deleted record still reads")
    found = [row["key"]["hash"] for row in read(node, lookup)[0]["records"]]
    require(found == ["dup"], "a deleted alternate entry still resolves: " + repr(found))
    conflict(node, delete(BY_ID, {"hash": "one"}, revision), index=0, reason="absent")
    write(node, by_id("one", "b3", "back"))
    require(value_of(node, "one") == "back", "a create after a delete differs")
    same_delete_refusal(node)
    print("App API deletes: delete at the read revision, an absent key and a wrong "
          "revision each name why, and create after delete passed", flush=True)


def same_delete_refusal(node):
    """A delete of an absent key and a delete at a wrong revision name why."""
    revision = stored(node, "dup")["revision"]
    conflict(node, delete(BY_ID, {"hash": "no-such-record"}, revision), index=0, reason="absent")
    conflict(node, delete(BY_ID, {"hash": "dup"}, "0" * len(revision)), index=0, reason="revision")


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
    require(plain == {"name": "whiteboard.txt", "media_type": "text/plain",
                      "size": original.stat().st_size, "local": str(original)},
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
                 {**by_id("nofile", "b8", "v"), "file": {"path": str(node.home / "identity.key")}},
                 {**by_id("nofile", "b8", "v"), "file": {"path": str(original), "bytes_b64": "QQ=="}},
                 {**delete(BY_ID, {"hash": "filed"}, second), "file": link}]:
        status, reply = error_body(node, "/v1/write", {"mutations": [item]})
        require(status == 400 and reply["error"]["code"] == "invalid_request",
                f"a file link that cannot be used was not refused: {status} {reply!r}")
    require(value_of(node, "nofile") is None, "a refused file write wrote its record")
    original.unlink()
    lost = with_path("filed")
    require(lost.get("error", {}).get("code") == "not_found", "a missing original was not reported: " + repr(lost))
    gone = stored(node, "filed")["file"]
    require(gone == {**plain, "local": None}, "a read without a path failed for a missing original")
    place = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True) / f"app-api-place-{os.getpid()}"
    data = place / "exercise" / "data"
    data.mkdir(parents=True, mode=0o700)
    app_id = node.call("/api/apps/register", {"app": "exercise", "access": {"read": [BY_ID], "write": [], "extensions": []}, "data": str(data)})[0]["app"]
    headers = {"Content-Type": "application/json", "X-LastDB-App": app_id}
    status, raw, _, _, _ = send_unix_request(
        Path(node.socket).with_name("apps.sock"), "POST", "/v1/read",
        {"reads": [{"schema": BY_ID, "key": {"hash": "filed"}}]}, headers)
    placed = json.loads(raw)["results"][0]["records"][0]["file"]
    require(status == 200 and placed == gone and not (data / "files").exists(),
            "an app read wrote a file or reported another local path: " + repr(placed))
    data.rmdir()
    data.parent.rmdir()
    place.rmdir()
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


def app_socket(node):
    """The app socket serves only the app API, to a registered app, within what the app declared."""
    sock = Path(node.socket).with_name("apps.sock")

    def register_app(read, write, extensions):
        access = {"read": read, "write": write, "extensions": extensions}
        reply = node.call("/api/apps/register", {"app": "exercise", "access": access})[0]
        require(reply.get("app") == "exercise" and "expires_in_seconds" not in reply,
                "the registration reply is not the app id without an expiry: " + repr(reply))
        return reply["app"]

    def call(app_id, path, body=None):
        headers = {"Content-Type": "application/json"}
        if app_id is not None:
            headers["X-LastDB-App"] = app_id
        status, raw, _, _, _ = send_unix_request(sock, "GET" if body is None else "POST", path, body, headers)
        return status, json.loads(raw)

    def refused(app_id, path, body, status, code):
        got, reply = call(app_id, path, body)
        require(got == status and reply["error"]["code"] == code,
                f"the app socket did not refuse {path} with {status} {code}: {got} {reply!r}")

    plain = register_app([BY_ID], [BY_BUCKET], [])
    reads = {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}},
                       {"schema": BY_BUCKET, "key": {"hash": "bk", "range": "a"}}]}
    status, reply = call(plain, "/v1/read", reads)
    require(status == 200 and len(reply["results"][0]["records"]) == 1
            and reply["results"][1]["error"]["code"] == "permission_denied",
            f"the app socket did not hold a read to the declared schemas: {status} {reply!r}")
    status, reply = call(plain, "/v1/write", {"mutations": [
        create(BY_BUCKET, {"hash": "sock", "range": "1"}, {"id": "1", "bucket": "sock", "value": "v"})]})
    require(status == 200 and reply["written"] == 1, f"a declared write was refused: {status} {reply!r}")
    refused(plain, "/v1/write", {"mutations": [by_id("sock", "b1", "v")]}, 403, "permission_denied")
    require(value_of(node, "sock") is None, "an undeclared write wrote a record")
    refused(plain, "/v1/x/changes", {"limit": 0}, 403, "permission_denied")
    refused(plain, "/api/version", None, 404, "not_found")
    refused(plain, "/api/query", {"schema_name": BY_ID, "filter": {"HashKey": "dup"}}, 404, "not_found")
    refused(plain, "/api/apps/register", {"app": "exercise"}, 404, "not_found")
    refused(None, "/v1/read", reads, 403, "permission_denied")
    refused("0" * 64, "/v1/read", reads, 403, "permission_denied")

    follower = register_app([BY_ID], [], ["changes"])
    head = changed(node, limit=0)["head"]
    write(node, create(BY_BUCKET, {"hash": "sock", "range": "2"}, {"id": "2", "bucket": "sock", "value": "v"}),
          by_id("sock-follow", "b1", "v"))
    status, page = call(follower, "/v1/x/changes", {"after": head})
    got = [(entry["schema"], entry["key"]["hash"]) for entry in page.get("changes", [])]
    require(status == 200 and got == [(BY_ID, "sock-follow")],
            f"changes on the app socket was not limited to the declared schemas: {status} {page!r}")
    refused(follower, "/v1/x/changes", {"after": head, "schemas": [BY_BUCKET]}, 403, "permission_denied")
    loose = BY_ID.lower()
    page = read(node, {"schema": loose, "key": {"hash": "dup"}})[0]
    require(loose != BY_ID and len(page.get("records") or []) == 1,
            "no loose form of the seeded schema resolves on the owner socket: " + repr(page))
    folded = register_app([loose], [], [])
    status, reply = call(folded, "/v1/read", {"reads": [{"schema": loose, "key": {"hash": "dup"}}]})
    require(status == 200 and reply["results"][0].get("error", {}).get("code") == "not_found",
            f"a loose schema name on the app socket was not not_found: {status} {reply!r}")
    print("App socket: only the app API, only with a registered app, only the declared "
          "schemas and extensions, and only an exact schema name", flush=True)

    access = {"read": [BY_ID], "write": [], "extensions": []}
    other = node.call("/api/apps/register", {"app": "other", "access": access})[0]
    limited = node.call("/api/apps/register", {"app": "limited", "access": access})[0]
    require(other.get("app") == "other" and "expires_in_seconds" not in other,
            "the registration reply is not the app id without an expiry: " + repr(other))
    require(limited.get("app") == "limited",
            "a second app was not registered under its id: " + repr(limited))
    for _ in range(5):
        again = node.call("/api/apps/register", {"app": "limited", "access": access})[0]
        require(again.get("app") == "limited" and "expires_in_seconds" not in again,
                "registering an app again returned another value: " + repr(again))
    one = {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}}]}
    status, reply = call("limited", "/v1/read", one)
    require(status == 200 and len(reply["results"][0]["records"]) == 1,
            f"an app registered again did not read: {status} {reply!r}")
    status, reply = call("other", "/v1/read", one)
    require(status == 200 and len(reply["results"][0]["records"]) == 1,
            f"another app's grant did not stay: {status} {reply!r}")
    refused("unregistered", "/v1/read", one, 403, "permission_denied")
    print("App socket: registering an app again keeps that app and another app, "
          "and an unknown app id is refused", flush=True)

    # Larger than APP_MAX_BODY_LEN in lastdb_node/src/exec/uds_server.rs.
    oversized = b'{"reads":[]}' + b" " * (8 * 1024 * 1024)
    status, raw, _, _, _ = send_unix_request(
        sock, "POST", "/v1/read", oversized, {"Content-Type": "application/json"})
    reply = json.loads(raw)
    require(status == 413 and reply["error"]["code"] == "too_large",
            f"a body over 8 MiB on the app socket was not refused: {status} {reply!r}")
    owner = node.call("/v1/read", {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}}]})[0]
    require(len(owner["results"][0]["records"]) == 1,
            "the owner socket did not answer after the app socket refused a large body")
    print("App socket: a body over 8 MiB is refused, and the owner socket still answers",
          flush=True)

    first = {"read": [BY_ID], "write": [], "extensions": []}
    second = {"read": [BY_BUCKET], "write": [BY_ID], "extensions": ["changes"]}
    node.call("/api/apps/register", {"app": "exercise", "access": first})
    status, reply = call("exercise", "/v1/read", {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}}]})
    require(status == 200 and len(reply["results"][0]["records"]) == 1,
            f"the first registration did not read: {status} {reply!r}")
    node.call("/api/apps/register", {"app": "exercise", "access": second})
    status, reply = call("exercise", "/v1/read", {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}}]})
    require(status == 200 and reply["results"][0].get("error", {}).get("code") == "permission_denied",
            f"the latest registration still served the previous read list: {status} {reply!r}")
    status, reply = call(
        "exercise", "/v1/read",
        {"reads": [{"schema": BY_BUCKET, "key": {"hash": "bk", "range": "a"}}]})
    require(status == 200 and len(reply["results"][0].get("records") or []) == 1,
            f"the latest read list was not served: {status} {reply!r}")
    status, reply = call("exercise", "/v1/write", {"mutations": [by_id("latest", "b1", "v")]})
    require(status == 200 and reply.get("written") == 1,
            f"the latest write list was not served: {status} {reply!r}")
    status, reply = call("exercise", "/v1/x/changes", {"limit": 0})
    require(status == 200 and reply.get("changes") == [],
            f"the latest extensions list was not served: {status} {reply!r}")
    print("App socket: the same app registered twice is served under the latest lists",
          flush=True)
    record_app_api_all.all_schemas(node)


def exercise(node):
    extension_names(node)
    system_seed(node, definition(BY_ID, "Hash", FIELDS, {"by_bucket": RANGED}))
    system_seed(node, definition(BY_BUCKET, "HashRange", FIELDS))
    writes(node)
    deletes(node)
    ranges(node)
    refusals(node)
    files(node)
    changes(node)
    app_socket(node)
    record_app_api_queue.parallel_reads(node)
    record_app_api_owner_file.exercise(node)


def named_database(node, database):
    """A database identifier on an item reads and writes that database, not personal."""
    write(node, by_id("app-api", "b1", "named", schema="RecordById", database=database))
    require(value_of(node, "app-api", schema="RecordById", database=database) == "named",
            "a named database did not return its record")
    require(value_of(node, "app-api", schema="RecordById") is None,
            "a write to a named database reached personal")
    changes(node, database)
    print("App API database identifier: a write and a read reached the named database only", flush=True)
