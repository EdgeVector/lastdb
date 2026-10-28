#!/usr/bin/env python3
"""Private, bounded I/O for the DEV restore operation."""

import hashlib
import json
import os
from pathlib import Path
import re
import stat
import uuid


ACCOUNT = "2a81c95623f2445854a794b5b48471d6"
CHUNK = 1024 * 1024
FILE_BYTES = 8 * CHUNK


class OperationError(Exception):
    """An error code that contains no product output or credential."""


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
        after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)


def private_directory(path):
    metadata = path.lstat()
    require(stat.S_ISDIR(metadata.st_mode) and metadata.st_uid == os.getuid()
            and metadata.st_mode & 0o077 == 0, "private_directory")
    return metadata


def private_file(path, hardlinks=False):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        require(stat.S_ISREG(metadata.st_mode) and metadata.st_uid == os.getuid()
                and metadata.st_mode & 0o077 == 0
                and (metadata.st_nlink >= 1 if hardlinks else metadata.st_nlink == 1),
                "private_file")
        return os.fdopen(descriptor, "rb"), metadata
    except BaseException:
        os.close(descriptor)
        raise


def read_json(path, max_bytes=262144):
    stream, before = private_file(Path(path))
    with stream:
        require(before.st_size <= max_bytes, "json_size")
        raw = stream.read(max_bytes + 1)
        require(len(raw) <= max_bytes and stable(before, os.fstat(stream.fileno())),
                "json_changed")
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
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    sync_directory(Path(path).parent)


def sync_directory(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def canonical_uuid(value):
    require(isinstance(value, str) and str(uuid.UUID(value)) == value
            and uuid.UUID(value).int != 0, "baseline_uuid")
    return value


def baseline_from_environment():
    raw = os.environ.pop("LASTDB_CLOUD_DEV_BASELINE", "")
    require(0 < len(raw.encode()) <= 65536, "baseline_missing_or_large")
    value = decode_json(raw)
    expected = {"account", "stream_id", "account_generation", "commit_id", "commit_sha256",
                "schema", "records", "file_bytes", "file_sha256"}
    require(isinstance(value, dict) and set(value) == expected, "baseline_fields")
    require(value["account"] == ACCOUNT, "baseline_dev_account")
    canonical_uuid(value["stream_id"])
    canonical_uuid(value["commit_id"])
    generation = str(value["account_generation"])
    require(re.fullmatch(r"0|[1-9][0-9]{0,19}", generation) is not None
            and int(generation) <= 2**64 - 1, "baseline_generation")
    value["account_generation"] = generation
    require(all(isinstance(value[key], str) and re.fullmatch(r"[0-9a-f]{64}", value[key])
                for key in ("commit_sha256", "file_sha256")), "baseline_digest")
    require(isinstance(value["schema"], str) and 0 < len(value["schema"]) <= 512,
            "baseline_schema")
    records = value["records"]
    require(isinstance(records, list) and len(records) == 16, "baseline_record_count")
    ids = set()
    for record in records:
        require(isinstance(record, dict) and set(record) == {"id", "note"}, "baseline_record")
        require(all(isinstance(record[key], str) and 0 < len(record[key].encode()) <= limit
                    for key, limit in (("id", 256), ("note", 2048))), "baseline_record_value")
        require(record["id"] not in ids, "baseline_duplicate_key")
        ids.add(record["id"])
    require(type(value["file_bytes"]) is int and value["file_bytes"] == FILE_BYTES,
            "baseline_file_size")
    return value


def dev_endpoint():
    path = Path(__file__).resolve().parents[2] / "folddb_profile/environments.json"
    require(path.is_file() and path.stat().st_size <= 65536, "compiled_profile")
    value = decode_json(path.read_bytes())
    endpoint = value["environments"]["dev"]["exemem_api"]
    require(isinstance(endpoint, str) and re.fullmatch(
        r"https://[a-z0-9]+\.execute-api\.us-west-2\.amazonaws\.com", endpoint),
        "compiled_dev_endpoint")
    return endpoint


def child_environment(root):
    # The allowlist prevents inherited CI, AWS, GitHub and product credentials.
    return {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C", "TZ": "UTC",
        "HOME": str(root / "os-home"), "TMPDIR": str(root / "tmp"),
        "ENVIRONMENT": "dev", "EXEMEM_ENV": "dev",
        "LASTDB_BYTE_BACKUP_V2_ENABLED": "false",
        "LASTDB_SUPPRESS_RECOVERY_PHRASE": "1", "RUST_MIN_STACK": "16777216",
        "LASTDB_SELF_METRICS_TO_DB": "false",
        "RUST_LOG": "warn", "OBS_SENTRY_DSN": "", "POSTHOG_API_KEY": "",
        "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
        "EXEMEM_API_URL": "http://127.0.0.1:9",
    }
