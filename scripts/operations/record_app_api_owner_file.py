"""Owner `file_dir` on POST /v1/read, and the unknown-database message.

A received database that already holds records cannot be stood up here.
Records enter a receiver store only when log sync replays a published log,
and that sync is what writes the durability marker. This operation points
ExeMem at a closed port. Registering a share on this node would also add a
database row, and the named-database check requires exactly six rows. There
is no command that unregisters a share.

A file is a path. `file_dir` is checked on the owner socket. A replace that
sends JSON null for `file` drops the record's file.
"""
import json
import os
from pathlib import Path

from harness import send_unix_request
import record_app_api_calls
import v1_records as v1
from record_app_api_calls import BY_ID, by_id, error_body, read
from record_runs import MISSING_DATABASE, definition, require, system_seed

NOT_REGISTERED = "database is not registered on this node"
SCHEMA = "RecordFile"
PHOTO = "photo"
PAYLOAD = b"owner file by path\n"
REMOVED = (
    ("POST", "/api/query"),
    ("POST", "/api/queries/batch"),
    ("POST", "/api/mutation"),
    ("POST", "/api/mutations/batch"),
    ("GET", "/api/list"),
    ("POST", "/api/db/put-blob"),
    ("POST", "/api/db/fetch-blob"),
    ("POST", "/api/db/delete-blob"),
    ("POST", "/api/schemas/declare"),
    ("GET", "/api/changes"),
)


def exercise(node):
    unknown_database(node)
    owner_file(node)
    removed_routes(node)


def unknown_database(node):
    page = read(node, {"schema": BY_ID, "key": {"hash": "dup"}, "database": MISSING_DATABASE})[0]
    require(page["error"]["code"] == "not_found" and page["error"]["message"] == NOT_REGISTERED,
            "an unknown database on a read used another message: " + repr(page))
    status, reply = error_body(node, "/v1/write", {"mutations": [by_id("x", "b1", "v", database=MISSING_DATABASE)]})
    require(status == 404 and reply["error"]["code"] == "not_found"
            and reply["error"]["message"] == NOT_REGISTERED,
            f"a write to an unknown database used another message: {status} {reply!r}")
    status, reply = error_body(node, "/v1/x/changes", {"database": MISSING_DATABASE, "limit": 0})
    require(status == 404 and reply["error"]["code"] == "not_found"
            and reply["error"]["message"] == NOT_REGISTERED,
            f"changes for an unknown database used another message: {status} {reply!r}")
    print("App API: an unknown database says it is not registered on this node", flush=True)


def owner_file(node):
    root = Path(os.environ["RUNNER_TEMP"]).resolve(strict=True)
    folder = root / f"app-api-file-dir-{os.getpid()}"
    folder.mkdir(mode=0o700)
    folder.chmod(0o700)
    folder = folder.resolve(strict=True)
    source = seed_file(node, root)
    plain = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}, "file": True})[0]["records"][0]["file"]
    require(plain["path"] == str(source) and plain["local"] == str(source)
            and plain["size"] == len(PAYLOAD) and source.read_bytes() == PAYLOAD,
            "an owner read did not return the linked file: " + repr(plain))
    placed = one(node, str(folder))
    require(placed["path"] == str(source) and placed["local"] == str(source)
            and Path(placed["path"]).read_bytes() == PAYLOAD,
            "file_dir did not return the linked file: " + repr(placed))
    again = one(node, str(folder))
    require(again["path"] == placed["path"], "a second read returned another path: " + repr(again))
    refuse(node, "files", "absolute path")
    refuse(node, str(folder / "missing"), "existing directory")
    note = folder / "not-a-dir"
    note.write_text("x")
    refuse(node, str(note), "directory")
    link = root / f"app-api-file-dir-link-{os.getpid()}"
    link.symlink_to(folder, target_is_directory=True)
    refuse(node, str(link), "not a symlink")
    wide = root / f"app-api-file-dir-wide-{os.getpid()}"
    wide.mkdir(mode=0o700)
    wide.chmod(0o775)
    refuse(node, str(wide), "group or world writable")
    bare = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}, "file_dir": str(folder)})[0]
    require(bare["error"]["code"] == "invalid_request" and bare["error"]["message"] == "file_dir requires file",
            "file_dir without file was not refused: " + repr(bare))
    # A key list that sets file or file_dir matches no ReadItem form. The schema
    # checker rejects a 200 whose request matches none, so this call stays raw.
    body = {"reads": [{"schema": BY_ID, "keys_only": True, "file": True, "file_dir": str(folder)}]}
    status, raw, _, _, _ = send_unix_request(
        node.socket, "POST", "/v1/read", body, {"Content-Type": "application/json"})
    listed = json.loads(raw)["results"][0]
    require(status == 200 and listed["error"]["code"] == "invalid_request"
            and "file_dir" in listed["error"]["message"],
            "a key list with file_dir was not refused: " + repr(listed))
    app_refuses(node, folder)
    clear_file(node)
    source.unlink()
    source.parent.rmdir()
    note.unlink()
    link.unlink()
    folder.rmdir()
    wide.rmdir()
    print("App API files: a path on the write, the same path on a file_dir read, "
          "and file null clears it", flush=True)


def one(node, directory):
    page = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}, "file": True, "file_dir": directory})[0]
    require("records" in page, "a file_dir read did not return the record: " + repr(page))
    return page["records"][0]["file"]


def refuse(node, directory, words):
    page = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}, "file": True, "file_dir": directory})[0]
    message = page.get("error", {}).get("message", "")
    require(page.get("error", {}).get("code") == "invalid_request" and words in message,
            f"file_dir {directory!r} was not refused ({words}): {page!r}")


def app_refuses(node, folder):
    place = folder.parent / f"app-api-file-dir-app-{os.getpid()}"
    data = place / "filedir" / "data"
    data.mkdir(parents=True, mode=0o700)
    registered = node.call("/api/apps/register", {
        "app": "filedir",
        "access": {"read": [BY_ID], "write": [], "extensions": []},
        "data": str(data),
    })[0]
    require(registered.get("app") == "filedir", "the file_dir app was not registered: " + repr(registered))
    body = {"reads": [{"schema": BY_ID, "key": {"hash": "dup"}, "file": True, "file_dir": str(folder)}]}
    headers = {"Content-Type": "application/json", "X-LastDB-App": "filedir"}
    status, raw, _, _, _ = record_app_api_calls.send_unix_request(
        Path(node.socket).with_name("apps.sock"), "POST", "/v1/read", body, headers)
    reply = json.loads(raw)
    error = reply["results"][0]["error"]
    require(status == 200 and error["code"] == "invalid_request" and "app socket" in error["message"],
            f"the app socket did not refuse file_dir: {status} {reply!r}")
    data.rmdir()
    data.parent.rmdir()
    place.rmdir()


def seed_file(node, root):
    system_seed(node, definition(SCHEMA, "Hash", ["id", "value"]))
    directory = root / f"app-api-file-src-{os.getpid()}"
    directory.mkdir(mode=0o700)
    source = directory / "photo.bin"
    source.write_bytes(PAYLOAD)
    v1.commit(node, [v1.create_item(
        SCHEMA, {"hash": PHOTO}, {"id": PHOTO, "value": "v1"},
        file={"path": str(source), "name": "photo.bin", "media_type": "application/octet-stream"},
    )])
    return source.resolve(strict=True)


def clear_file(node):
    """A replace with JSON null for file drops the file and keeps the record."""
    current = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}})[0]["records"][0]["revision"]
    written = node.call("/v1/write", {"mutations": [{
        "schema": SCHEMA, "op": "replace", "key": {"hash": PHOTO},
        "record": {"id": PHOTO, "value": "v1"}, "revision": current, "file": None,
    }]})[0]
    require(written.get("written") == 1, "clearing a file did not write: " + repr(written))
    after = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}, "file": True})[0]["records"][0]
    require(after["file"] is None and after["record"]["value"] == "v1",
            "a replace with file null kept the file: " + repr(after))


def removed_routes(node):
    """Each deleted record route is an unknown path: HTTP 404, body Not Found."""
    headers = {"Content-Type": "application/json"}
    paths = list(REMOVED)
    for name in ("query", "queries/batch", "mutation", "mutations/batch", "list"):
        method = "GET" if name == "list" else "POST"
        paths.append((method, f"/api/databases/gone/{name}"))
    for method, path in paths:
        status, raw, _, _, _ = send_unix_request(
            node.socket, method, path, {} if method == "POST" else None, headers)
        require(status == 404 and raw == b"Not Found",
                f"{method} {path} was not a plain 404: {status} {raw[:80]!r}")
    print("Owner socket: the old record routes answer 404", flush=True)
