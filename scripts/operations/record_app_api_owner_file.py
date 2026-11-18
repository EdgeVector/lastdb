"""Owner `file_dir` on POST /v1/read, and the unknown-database message.

A received database that already holds records cannot be stood up here.
Records enter a receiver store only when log sync replays a published log,
and that sync is what writes the durability marker. This operation points
ExeMem at a closed port. Registering a share on this node would also add a
database row, and the named-database check requires exactly six rows. There
is no command that unregisters a share.
"""
import base64
import hashlib
import json
import os
from pathlib import Path
import stat

from harness import send_unix_request
import record_app_api_calls
from record_app_api_calls import BY_ID, by_id, error_body, read
from record_runs import MISSING_DATABASE, require

NOT_REGISTERED = "database is not registered on this node"
SCHEMA = "RecordFile"
PHOTO = "photo"


def exercise(node):
    unknown_database(node)
    owner_file(node)


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
    held = blob(node)
    plain = read(node, {"schema": SCHEMA, "key": {"hash": PHOTO}, "file": True})[0]["records"][0]["file"]
    require(plain.get("path") is None and plain["size"] == len(held),
            "an owner read without file_dir returned a path: " + repr(plain))
    placed = one(node, str(folder))
    path = Path(placed["path"])
    require(path.parent == folder and path.name == hashlib.sha256(held).hexdigest()
            and path.read_bytes() == held and stat.S_IMODE(path.stat().st_mode) == 0o600
            and placed["local"] == placed["path"],
            "file_dir did not write the held file: " + repr(placed))
    inode = path.stat().st_ino
    again = one(node, str(folder))
    require(again["path"] == placed["path"] and Path(again["path"]).stat().st_ino == inode,
            "a second read did not reuse the file: " + repr(again))
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
    path.unlink()
    note.unlink()
    link.unlink()
    folder.rmdir()
    wide.rmdir()
    print("App API files: the owner names a folder and the held file is written there", flush=True)


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


def blob(node):
    body = {"schema": SCHEMA, "key": {"hash": PHOTO, "range": None}, "placement": "blob"}
    encoded = node.call("/api/db/fetch-blob", body)[0]["file"]["bytes_b64"]
    return base64.b64decode(encoded)
