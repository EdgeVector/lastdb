#!/usr/bin/env python3
"""Private, bounded I/O and BIP39 seed decoding for DEV cloud backup operations."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import time
import uuid

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import CREDENTIALS_FILE, summary_line, write_private  # noqa: E402,F401


ACCOUNT = "2a81c95623f2445854a794b5b48471d6"
WORDS_SHA256 = "2f5eed53a4727b4bf8880d8f3f199efc90e58503646d9ff8eff3a2ed3b24dbda"
DECIMAL = re.compile(r"0|[1-9][0-9]{0,19}")
CHUNK = 1024 * 1024


class BaselineDataError(Exception):
    """A fixed failure code. It never carries a secret value."""


def _check_baseline(condition, code):
    if not condition:
        raise BaselineDataError(code)


def words():
    path = Path(__file__).resolve().with_name("bip39_english.txt")
    raw = path.read_bytes()
    _check_baseline(hashlib.sha256(raw).hexdigest() == WORDS_SHA256, "baseline_word_list_digest")
    values = raw.decode("ascii").split("\n")[:-1]
    _check_baseline(len(values) == 2048 and len(set(values)) == 2048, "baseline_word_list_shape")
    return {word: index for index, word in enumerate(values)}


def seed(phrase):
    """Return the 32-byte Ed25519 seed of a 24-word phrase."""
    _check_baseline(isinstance(phrase, bytes) and 0 < len(phrase) <= 4096, "baseline_phrase_size")
    try:
        items = phrase.decode("ascii").split()
    except UnicodeDecodeError:
        raise BaselineDataError("baseline_phrase_text") from None
    index = words()
    _check_baseline(len(items) == 24 and all(item in index for item in items), "baseline_phrase_words")
    bits = 0
    for item in items:
        bits = (bits << 11) | index[item]
    entropy = (bits >> 8).to_bytes(32, "big")
    _check_baseline(bits & 0xFF == hashlib.sha256(entropy).digest()[0], "baseline_phrase_checksum")
    return entropy


class OperationError(Exception):
    """An error code that contains no product output or credential."""

    def __init__(self, code, service_field=None, *, cli_refusal=None, socket_refusal=None):
        super().__init__(code)
        self.service_field = service_field
        self.cli_refusal = cli_refusal
        self.socket_refusal = socket_refusal


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


def private_file(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        require(
            stat.S_ISREG(metadata.st_mode)
            and metadata.st_uid == os.getuid()
            and metadata.st_mode & 0o077 == 0
            and metadata.st_nlink == 1,
            "private_file",
        )
        return os.fdopen(descriptor, "rb"), metadata
    except BaseException:
        os.close(descriptor)
        raise


def load_dev_owner(expected_account, *, pop_env=False, exact_baseline_keys=False):
    """Decode and verify the enrolled DEV owner key, API key, and account generation."""
    mut = str(Path(__file__).resolve().parents[1] / "mutation_log_load")
    if mut not in sys.path:
        sys.path.insert(0, mut)
    from node_auth import NodeKey, account_id

    read_env = os.environ.pop if pop_env else os.environ.get
    phrase = read_env("LASTDB_CLOUD_DEV_RECOVERY_PHRASE", "").strip().encode("utf-8", errors="surrogatepass")
    api_key = read_env("LASTDB_CLOUD_DEV_API_KEY", "")
    raw = read_env("LASTDB_CLOUD_DEV_BASELINE", "")
    require(len(api_key.encode("utf-8", errors="surrogatepass")) <= 4096, "dev_api_key")
    api_key = api_key.strip()
    require(api_key and api_key.isascii() and api_key.isprintable(), "dev_api_key")
    require(0 < len(raw) <= 65_536, "snapshot_baseline")
    try:
        baseline = json.loads(raw)
    except json.JSONDecodeError:
        raise OperationError("snapshot_baseline") from None
    require(
        isinstance(baseline, dict)
        and (not exact_baseline_keys or set(baseline) == {"account", "account_generation"}),
        "snapshot_baseline",
    )
    generation = baseline.get("account_generation")
    try:
        key = NodeKey.from_seed(seed(phrase))
    except BaselineDataError:
        raise OperationError("snapshot_phrase") from None
    require(
        baseline.get("account") == account_id(key.public) == expected_account
        and isinstance(generation, str)
        and DECIMAL.fullmatch(generation) is not None
        and int(generation) <= 2**64 - 1,
        "snapshot_baseline_scope",
    )
    return key, api_key, generation, phrase


def read_json(path, max_bytes=262144):
    stream, before = private_file(Path(path))
    with stream:
        require(before.st_size <= max_bytes, "json_size")
        raw = stream.read(max_bytes + 1)
        require(len(raw) <= max_bytes and stable(before, os.fstat(stream.fileno())), "json_changed")
    return decode_json(raw)


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


@contextmanager
def running(node, *, remove_saved=False):
    try:
        node.start()
        yield
        node.stop(remove_saved=remove_saved)
    finally:
        node.cleanup_stop()


def retry(stage, command, complete):
    deadline = time.monotonic() + 150
    last_error = None
    while time.monotonic() < deadline:
        try:
            result = command()
        except OperationError as error:
            last_error = error
        else:
            if complete(result):
                return result
        time.sleep(2)
    raise OperationError(stage, cli_refusal=last_error.cli_refusal if last_error else None)


def summary(message):
    summary_line(message, echo=False)
