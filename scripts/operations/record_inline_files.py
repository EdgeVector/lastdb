"""Inline files, schema-drop partition cleanup, and home format refusals on a fresh CI home."""

from __future__ import annotations

import base64
import hashlib
import http.client
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

_OPS = Path(__file__).resolve().parent
_SCRIPTS = _OPS.parent
for _entry in (str(_OPS), str(_SCRIPTS), str(_SCRIPTS / "benchmarks")):
    if _entry not in sys.path:
        sys.path.insert(0, _entry)

from harness import conditioned, mutation, poll_until, send_unix_request, stop_process  # noqa: E402
from home_format import FORMAT, HOME_FORMAT, NODE_MARKER as PRIVATE_MARKER, source_constant  # noqa: E402
from record_runs import FIELDS as DROP_FIELDS, TTL_SECONDS, definition, mutation as run_mutation, seed, write  # noqa: E402

SCHEMA = "RecordFile"
FIELDS = ["id", "value"]
MAX_INLINE = 64 * 1024
# A thumbnail this large would not fit in a 64 KiB record, proving bytes live outside the record.
THUMBNAIL = os.urandom(60_000)
ORIGINAL = os.urandom(200_000)
ICON = os.urandom(MAX_INLINE)
TRACKED_HASHES = 40_050

MARKER_FILE = source_constant(FORMAT, "NODE_MARKER")
BOOT_ERROR = source_constant("lastdb_node/src/host/recovery.rs", "LAST_BOOT_ERROR_FILE")
RETIRED_MARKER = source_constant(FORMAT, "RETIRED_SHARED_NODE_BYTES")
RETIRED_DIR = source_constant(FORMAT, "RETIRED_SHARED_DIR")
REFUSAL = "node range admission: the node range format is unsupported"
RETIRED = "retired format: the dedicated shared home format is retired"
PANIC_MARKER = "LastDB panic"
DEADLINE_SECONDS = 120


def require(value, message):
    if not value:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def encoded(data):
    return base64.b64encode(data).decode()


def install(node):
    schema = {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA,
        "schema_type": "Hash", "record_keys": {"primary": {"kind": "hash"}, "alternates": {}},
        "fields": FIELDS, "field_types": {field: "String" for field in FIELDS},
        "field_descriptions": {field: "Inline file example " + field for field in FIELDS},
        "field_versions": {field: 1 for field in FIELDS},
    }
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": schema})


def replacement(node, key, values):
    return conditioned(node, [mutation(SCHEMA, {"hash": key}, values)])[0]


def row(node, key):
    rows = node.call("/api/query", {"schema_name": SCHEMA, "filter": {"HashKey": key}})[0]["results"]
    require(len(rows) == 1, f"{SCHEMA} {key} returned {len(rows)} rows")
    return rows[0]


def inline_marker(record):
    return (record.get("metadata") or {}).get("inline_file")


def fetch(node, key, placement):
    body = {"schema": SCHEMA, "key": {"hash": key, "range": None}, "placement": placement}
    return base64.b64decode(node.call("/api/db/fetch-blob", body)[0]["file"]["bytes_b64"])


def refused(call, words):
    try:
        call()
    except RuntimeError as error:
        require(words in str(error), "the call failed for another reason: " + str(error))
        return
    raise RuntimeError("the call did not fail: " + words)


def put(node, key, values, data, **options):
    body = {"mutation": replacement(node, key, values), "bytes_b64": encoded(data), **options}
    return node.call("/api/db/put-blob", body)[0]


def exercise(node):
    install(node)
    values = {"id": "photo", "value": "v1"}
    node.call("/api/mutation", mutation(SCHEMA, {"hash": "photo"}, values, "create"))
    reply = put(node, "photo", values, ORIGINAL, name="photo.bin",
                media_type="application/octet-stream",
                thumbnail={"bytes_b64": encoded(THUMBNAIL), "name": "thumb.bin"})
    require(reply["file"]["sha256"] == digest(ORIGINAL)
            and reply["thumbnail"]["sha256"] == digest(THUMBNAIL),
            "put-blob did not report the blob and thumbnail markers")

    plain = inline_marker(row(node, "photo"))
    require(plain is not None and plain["sha256"] == digest(THUMBNAIL)
            and plain["size_bytes"] == len(THUMBNAIL) and "bytes_b64" not in plain,
            "a query did not return the inline marker alone")
    require(fetch(node, "photo", "inline") == THUMBNAIL, "fetch-blob inline returned other bytes")
    require(fetch(node, "photo", "blob") == ORIGINAL, "fetch-blob returned another blob")

    values = {"id": "photo", "value": "v2"}
    node.call("/api/mutation", replacement(node, "photo", values))
    record = row(node, "photo")
    require(inline_marker(record)["sha256"] == digest(THUMBNAIL)
            and record["metadata"]["file"]["sha256"] == digest(ORIGINAL),
            "a field replacement dropped a file")

    icon = {"id": "icon", "value": "i1"}
    node.call("/api/mutation", mutation(SCHEMA, {"hash": "icon"}, icon, "create"))
    reply = put(node, "icon", icon, ICON, placement="inline", name="icon.bin")
    require(reply["placement"] == "inline" and reply["file"]["sha256"] == digest(ICON),
            "an inline put did not report its marker")
    require(row(node, "icon").get("metadata", {}).get("file") is None, "an inline put created a blob file")
    refused(lambda: put(node, "icon", icon, ICON + b"x", placement="inline"), "HTTP 413")

    node.call("/api/db/delete-blob", {"mutation": replacement(node, "photo", values), "placement": "inline"})
    record = row(node, "photo")
    require(inline_marker(record) is None and record["metadata"]["file"]["sha256"] == digest(ORIGINAL),
            "delete-blob inline removed the wrong file")
    put(node, "photo", values, THUMBNAIL, placement="inline", name="thumb.bin")

    gone = {"id": "gone", "value": "g1"}
    node.call("/api/mutation", mutation(SCHEMA, {"hash": "gone"}, gone, "create"))
    put(node, "gone", gone, THUMBNAIL, placement="inline", name="gone.bin")
    node.call("/api/mutation", conditioned(node, [mutation(SCHEMA, {"hash": "gone"}, {}, "delete")])[0])
    node.call("/api/mutation", mutation(SCHEMA, {"hash": "gone"}, gone, "create"))
    require(inline_marker(row(node, "gone")) is None, "a new record kept a deleted inline file")
    refused(lambda: fetch(node, "gone", "inline"), "no inline file")
    print("Inline files: put, fetch, keep, cap, delete and record delete passed", flush=True)


def check(node):
    require(fetch(node, "photo", "inline") == THUMBNAIL
            and fetch(node, "photo", "blob") == ORIGINAL
            and fetch(node, "icon", "inline") == ICON,
            "an inline or blob file changed across the restart")
    print("Inline files survived a daemon restart", flush=True)


def _retention_state(node, schema):
    current = node.call("/api/db/schema-retention", {"action": "get", "schema": schema})[0]
    hashes, cursor = [], None
    for _ in range(1_000):
        request = {"action": "partitions", "schema": schema, "limit": 256}
        if cursor:
            request["cursor"] = cursor
        page = node.call("/api/db/schema-retention", request)[0]
        hashes.extend(page["observed_hash_partitions"])
        following = page["next_cursor"]
        if following is None:
            current["observed_hash_partitions"] = sorted(hashes)
            return current
        require(following != cursor, "the partition page cursor did not advance")
        cursor = following
    raise RuntimeError("partition registry did not end in 1,000 pages")


def check_drop_tracking(node):
    for schema, expected in [("RecordAged", []), ("RecordDropKeep", ["aged"])]:
        current = _retention_state(node, schema)
        require(current["observed_hash_partitions"] == expected,
                f"{schema} partition tracking differs: {current}")
        require((current["retention_policy"] is None) == (schema == "RecordAged"),
                f"{schema} policy changed across the drop: {current}")
    rows = node.call("/api/query", {"schema_name": "RecordDropKeep",
                                    "filter": {"HashKey": "aged"}})[0]["results"]
    require(len(rows) == 1 and rows[0]["fields"]["value"] == "kept", "the drop erased another schema")


def exercise_drop_tracking(node):
    key = f"{int(time.time()):020d}-kept"
    kept = definition("RecordDropKeep", "HashRange", DROP_FIELDS)
    seed(node, kept)
    node.call("/api/db/schema-retention",
              {"action": "set", "schema": kept["name"], "ttl_seconds": TTL_SECONDS})
    write(node, [run_mutation(kept["name"], {"hash": "aged", "range": key},
                              {"id": key, "bucket": "aged", "value": "kept"}, "create")])
    hashes = [f"drop-{number:05d}" for number in range(TRACKED_HASHES)]
    write(node, [run_mutation("RecordAged", {"hash": hash_key, "range": key},
                              {"id": key, "bucket": hash_key, "value": "drop"}, "create")
                 for hash_key in hashes])
    before = _retention_state(node, "RecordAged")
    require(before["observed_hash_partitions"] == ["aged"] + hashes,
            "the drop target does not hold the complete generated partition registry")
    node.call("/api/schemas/drop", {"schema": "RecordAged", "must_exist": True})
    again = node.call("/api/schemas/drop", {"schema": "RecordAged"})[0]
    require(not again["existed"], "the repeated schema drop was not an idempotent miss")
    seed(node, definition("RecordAged", "HashRange", DROP_FIELDS))
    check_drop_tracking(node)
    print(f"Schema drop: {TRACKED_HASHES + 1} tracked hashes removed in bounded batches; "
          "repeated drop, reinstall and other schema passed", flush=True)


def _wrong_bytes(marker):
    require(len(marker) > 2 and marker.endswith(b"\n"), "the private marker has no trailing line")
    changed = bytearray(marker)
    changed[-2] ^= 1
    return bytes(changed)


def _older_format(marker):
    prefix = f"format={HOME_FORMAT}\n".encode()
    require(marker.startswith(prefix), "the private marker does not start with the current format")
    claimed = HOME_FORMAT - 1
    require(claimed > 0, "there is no older format to claim")
    return f"format={claimed}\n".encode() + marker[len(prefix):]


def _serving(socket):
    if not socket.exists():
        return False
    try:
        send_unix_request(socket, "GET", "/health", timeout=2)
        return True
    except (OSError, TimeoutError, http.client.HTTPException):
        return False


def _expect_refusal(node, directory, label, refusal, prepare_home, expected_marker=None):
    directory.mkdir(mode=0o700)
    home = directory / "home"
    shutil.copytree(node.home, home, symlinks=True)
    marker_path = home / "data" / MARKER_FILE
    require(marker_path.read_bytes() == PRIVATE_MARKER, "the copy changed the node marker")
    prepare_home(home, marker_path)
    error_file = home / BOOT_ERROR
    error_file.unlink(missing_ok=True)
    log_path, socket = directory / "daemon.log", node.root / "r.sock"
    socket.unlink(missing_ok=True)
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [str(node.binary), "--data-dir", str(home), "--socket-path", str(socket), *node.arguments],
            env=node.daemon_env(home), stdin=subprocess.DEVNULL, stdout=log, stderr=log)
        try:
            outcome = poll_until(
                DEADLINE_SECONDS,
                lambda: "exited" if process.poll() is not None else ("opened" if _serving(socket) else None),
                interval=0.05, timeout_error=f"the {label} home neither exited nor opened")
            code = process.returncode
        finally:
            stop_process(process, timeout=10, crash=True)
            socket.unlink(missing_ok=True)
    log_text = log_path.read_text(errors="replace")
    require(outcome != "opened", f"the {label} home opened")
    require(PANIC_MARKER not in log_text, f"the {label} home panicked instead of refusing the format")
    require(outcome == "exited" and code is not None and code > 0,
            f"the {label} home did not exit with a format refusal")
    boot = error_file.read_text(errors="replace") if error_file.is_file() else ""
    require(refusal in boot, f"the {label} home failed without {refusal!r}: {boot[-500:]}")
    require("lastdbd: daemon_failed" in log_text, f"the {label} daemon did not report a controlled failure")
    if expected_marker is not None:
        observed = marker_path.read_bytes() if marker_path.is_file() else b""
        require(observed == expected_marker, f"startup rewrote the {label} node marker")
    shutil.rmtree(directory)
    print(f"Format refusal: {label} exited with {refusal}", flush=True)


def exercise_format_refusal(node):
    require(node.process is None, "stop the daemon before copying its home")
    marker = (node.home / "data" / MARKER_FILE).read_bytes()
    require(marker == PRIVATE_MARKER, "the generated home marker is not the current private marker")
    changed, previous = _wrong_bytes(marker), _older_format(marker)
    require(changed != previous, "the two refused markers are the same")
    require(changed.startswith(f"format={HOME_FORMAT}\n".encode()),
            "the altered marker no longer claims the current format")
    require(previous.startswith(f"format={HOME_FORMAT - 1}\n".encode()),
            "the older marker does not claim the previous format")
    root = node.root / "format-refusal"
    root.mkdir(mode=0o700)
    for name, bad, label, refusal in (
        ("wrong-bytes", changed, "altered marker bytes", REFUSAL),
        ("older-format", previous, f"format {HOME_FORMAT - 1} marker", REFUSAL),
        ("retired-shared", RETIRED_MARKER, "retired shared marker", RETIRED),
    ):
        require(bad != PRIVATE_MARKER, f"{label} is still an admitted marker")
        _expect_refusal(node, root / name, label, refusal, lambda _h, p, b=bad: p.write_bytes(b), bad)
    _expect_refusal(node, root / "retired-directory", "disk-private directory", RETIRED,
                    lambda h, _p: (h / "data" / RETIRED_DIR).mkdir(mode=0o700))
    print("Format refusal: altered bytes and an older format failed open as unsupported; "
          "the retired shared marker and disk-private directory failed open as retired", flush=True)
