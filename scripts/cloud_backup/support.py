#!/usr/bin/env python3
"""Private, bounded I/O for the DEV restore operation."""

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import traceback
import uuid

_SCRIPTS = Path(__file__).resolve().parents[1]
for _entry in (str(_SCRIPTS), str(_SCRIPTS / "benchmarks")):
    if _entry not in sys.path:
        sys.path.append(_entry)

from harness.ci import summary_line, write_private  # noqa: E402,F401
from home_format import BACKUP_DIR, CREDENTIALS_FILE  # noqa: E402,F401


ACCOUNT = "2a81c95623f2445854a794b5b48471d6"
CHUNK = 1024 * 1024


class OperationError(Exception):
    """An error code that contains no product output or credential."""

    def __init__(self, code, service_field=None, *, cli_refusal=None, socket_refusal=None):
        super().__init__(code)
        self.service_field = service_field
        self.cli_refusal = cli_refusal
        self.socket_refusal = socket_refusal


def unexpected(error, nodes):
    """Where an unclassified error left the recipe: its class, frames, and daemon state."""
    frames = traceback.extract_tb(error.__traceback__)[-8:]
    return {
        "unexpected_error": type(error).__name__,
        "unexpected_frames": [f"{Path(frame.filename).name}:{frame.lineno}:{frame.name}" for frame in frames],
        "daemons": [node.exit_report() for node in nodes],
    }


def failure(error):
    """The public failure fields of one error; the code stays the compared value."""
    value = {"failure": str(error)}
    if error.service_field is not None:
        value["service_field"] = error.service_field
    if error.cli_refusal is not None:
        value["cli_refusal"] = error.cli_refusal
    if error.socket_refusal is not None:
        value["socket_refusal"] = error.socket_refusal
    return value


def require(condition, code):
    if not condition:
        raise OperationError(code)


def pairs(items):
    result = {}
    for key, value in items:
        require(key not in result, "duplicate_json_key")
        result[key] = value
    return result


def decode_json(raw):
    def reject(_value):
        raise OperationError("nonfinite_json")

    try:
        return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject)
    except (ValueError, UnicodeError, RecursionError) as error:
        raise OperationError("invalid_json") from error


def stable(before, after):
    return (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) == (
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns
    )


def private_directory(path):
    metadata = path.lstat()
    require(
        stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.getuid() and metadata.st_mode & 0o077 == 0,
        "private_directory",
    )
    return metadata


def private_file(path, hardlinks=False):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        require(
            stat.S_ISREG(metadata.st_mode)
            and metadata.st_uid == os.getuid()
            and metadata.st_mode & 0o077 == 0
            and (metadata.st_nlink >= 1 if hardlinks else metadata.st_nlink == 1),
            "private_file",
        )
        return os.fdopen(descriptor, "rb"), metadata
    except BaseException:
        os.close(descriptor)
        raise


def read_json(path, max_bytes=262144):
    stream, before = private_file(Path(path))
    with stream:
        require(before.st_size <= max_bytes, "json_size")
        raw = stream.read(max_bytes + 1)
        require(len(raw) <= max_bytes and stable(before, os.fstat(stream.fileno())), "json_changed")
    return decode_json(raw)


def hash_file(path, expected_bytes=None):
    stream, before = private_file(Path(path), hardlinks=True)
    value = hashlib.sha256()
    with stream:
        require(expected_bytes is None or before.st_size == expected_bytes, "file_size")
        for block in iter(lambda: stream.read(CHUNK), b""):
            value.update(block)
        require(stable(before, os.fstat(stream.fileno())), "file_changed")
    return value.hexdigest()


def save_json(path, value):
    raw = (json.dumps(value, sort_keys=True, indent=2, allow_nan=False) + "\n").encode()
    require(len(raw) <= 65536, "summary_size")
    write_private(path, raw)
    sync_directory(Path(path).parent)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def canonical_uuid(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value and uuid.UUID(value).int != 0, "baseline_uuid")
    return value


def dev_endpoint():
    path = Path(__file__).resolve().parents[2] / "lastdb_node/environments.json"
    require(path.is_file() and path.stat().st_size <= 65536, "compiled_profile")
    value = decode_json(path.read_bytes())
    endpoint = value["environments"]["dev"]["exemem_api"]
    require(
        isinstance(endpoint, str)
        and re.fullmatch(r"https://[a-z0-9]+\.execute-api\.us-west-2\.amazonaws\.com", endpoint),
        "compiled_dev_endpoint",
    )
    return endpoint


def child_environment(root):
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "LANG": "C",
        "TZ": "UTC",
        "HOME": str(root / "os-home"),
        "TMPDIR": str(root / "tmp"),
        "ENVIRONMENT": "dev",
        "EXEMEM_ENV": "dev",
        "LASTDB_SUPPRESS_RECOVERY_PHRASE": "1",
        "RUST_MIN_STACK": "16777216",
        "LASTDB_SELF_METRICS_TO_DB": "false",
        "RUST_LOG": "warn",
        "OBS_SENTRY_DSN": "",
        "POSTHOG_API_KEY": "",
        "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
    }
