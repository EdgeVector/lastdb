"""Shared stdlib harness helpers for LastDB CI operations, benchmarks, and release probes."""

from __future__ import annotations

import argparse
import ast
from collections import defaultdict
import copy
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import time
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
FORMAT = "fold_db/crates/core/src/storage/format.rs"
CHUNK_BYTES = 1024 * 1024


def source_constant(path, name):
    source = (ROOT / path).read_text(encoding="utf-8")
    pattern = r'\bconst\s+' + re.escape(name) + r'\s*:[^=]+?=\s*(b?"(?:\\.|[^"\\])*"|[0-9]+)\s*;'
    matches = re.findall(pattern, source)
    if len(matches) != 1:
        raise ValueError(f"expected one authoritative {name} in {path}")
    return ast.literal_eval(matches[0])


HOME_FORMAT = source_constant(FORMAT, "HOME_FORMAT")
NODE_MARKER = source_constant(FORMAT, "RECORD_NODE_BYTES")
CREDENTIALS_FILE = source_constant("lastdb_node/src/cloud.rs", "CLOUD_CREDENTIALS_FILE")
if not NODE_MARKER.startswith(f"format={HOME_FORMAT}\n".encode()):
    raise ValueError("the authoritative home version and node marker disagree")


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Refuse HTTP redirects via HTTPError (default) or a caller-provided exception."""

    def __init__(self, error=None):
        self.error = error

    def redirect_request(self, *_args, **_kwargs):
        if self.error is None:
            return None
        raise self.error() if callable(self.error) else self.error
SOCKET_OPERATIONS = {
    "/health": "health", "/api/status": "status", "/api/system/boot-identity": "boot_identity",
    "/api/db/native-status": "native_status", "/api/queries/batch": "query_batch",
    "/api/mutations/batch": "mutation_batch",
}
SOCKET_REFUSALS = frozenset({
    "cas_conflict", "compaction_behind", "field_value_too_large", "persist_queue_full",
    "read_resource_limit", "storage_full", "write_resource_limit",
})


class UnixConnection(http.client.HTTPConnection):
    """HTTP/1.1 client connection over an AF_UNIX stream socket."""

    def __init__(self, path, timeout=30):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


def send_unix_request(socket_path, method, path, body=None, headers=None, *, timeout=30, max_bytes=None):
    payload = body
    if payload is not None and not isinstance(payload, (bytes, bytearray)):
        payload = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    connection = UnixConnection(socket_path, timeout=timeout)
    started = time.perf_counter_ns()
    try:
        connection.request(method, path, payload, headers or {})
        response = connection.getresponse()
        raw = response.read() if max_bytes is None else response.read(max_bytes + 1)
        elapsed_ms = (time.perf_counter_ns() - started) / 1e6
        return response.status, raw, {k.lower(): v for k, v in response.getheaders()}, elapsed_ms, len(payload or b"")
    finally:
        connection.close()


def classify_socket_refusal(path, response, *, decode_json_fn=json.loads, ignored_decode_errors=(ValueError,)):
    refusal = {"operation": SOCKET_OPERATIONS.get(path, "other"), "status": response.status, "reason": "unrecognized"}
    try:
        raw = response.read(4097)
    except (OSError, http.client.HTTPException):
        return refusal
    if len(raw) > 4096:
        return refusal
    if raw.startswith(b"node is busy: too many concurrent reads; retry after "):
        refusal["reason"] = "read_busy"
        return refusal
    try:
        body = decode_json_fn(raw)
    except ignored_decode_errors:
        return refusal
    code = body.get("error") if isinstance(body, dict) else None
    if isinstance(code, str) and code in SOCKET_REFUSALS:
        refusal["reason"] = code
    return refusal


def marker_format(marker):
    if marker is None:
        return None
    first = marker.split("\n", 1)[0]
    return int(first[len("format=") :]) if first.startswith("format=") else None


def read_shutdown_receipt(home):
    receipt_path = home / ".shutdown_flush_ready"
    return json.loads(receipt_path.read_text(encoding="utf-8")) if receipt_path.is_file() else None


def verify_shutdown_receipt(receipt, pid, start_ts=None):
    if not isinstance(receipt, dict) or receipt.get("pid") != pid or receipt.get("flush_ok") is not True:
        return False
    if "version" in receipt and receipt.get("version") != 1:
        return False
    return start_ts is None or receipt.get("start_ts") == start_ts


def stop_process(process, *, timeout=45, kill_timeout=10, crash=False):
    forced = False
    if process.poll() is None:
        process.send_signal(signal.SIGKILL if crash else signal.SIGTERM)
        try:
            process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            forced = True
            process.kill()
            process.wait(timeout=kill_timeout)
    return forced


def poll_until(timeout_seconds, step, *, interval=0.05, timeout_error="timed out"):
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        result = step()
        if result:
            return result
        time.sleep(interval)
    if callable(timeout_error):
        raise timeout_error()
    raise timeout_error if isinstance(timeout_error, BaseException) else RuntimeError(timeout_error)


def key(hash_value):
    return {"hash": hash_value, "range": None}


def coordinate(schema, key_value):
    return schema, key_value.get("hash"), key_value.get("range")


def create(schema, key_value, record):
    return {
        "type": "mutation", "schema": schema, "key_value": key_value,
        "record": copy.deepcopy(record), "alternates": {},
        "mutation_type": "create", "expected": {"type": "absent"}, "durability": "durable",
    }


def replace_row(schema, row, record):
    request = {
        "type": "mutation", "schema": schema, "key_value": copy.deepcopy(row["key"]),
        "record": copy.deepcopy(record), "alternates": copy.deepcopy(row["alternates"]),
        "mutation_type": "replace", "durability": "durable",
        "expected": {"type": "revision", "revision": row["revision"]},
    }
    metadata = row.get("metadata") or {}
    for name in ("source_file_name", "metadata", "file"):
        if metadata.get(name) is not None:
            request[name] = copy.deepcopy(metadata[name])
    return request


def mutation(schema, key_value, record, operation="replace", *, alternates=None, expected=None):
    return {
        "type": "mutation", "schema": schema, "key_value": key_value, "record": record,
        "alternates": alternates or {}, "mutation_type": operation,
        "expected": expected if expected is not None else {"type": "absent"} if operation == "create" else None,
        "durability": "durable", "convergence": "sync",
    }


def read_records(node, requests):
    grouped = defaultdict(list)
    for request in requests:
        grouped[request["schema"]].append(request["key_value"])
    queries = [{"schema_name": schema, "limit": len(keys),
                "filter": {"HashRangeKeys": [[item["hash"], item.get("range") or ""] for item in keys]}}
               for schema, keys in grouped.items()]
    if not queries:
        return {}
    reply = node.call("/api/queries/batch", {"queries": queries})
    reply = reply[0] if isinstance(reply, tuple) else reply
    found = {}
    for schema, item in zip(grouped, reply["results"]):
        if item["status"] != 200:
            raise RuntimeError(f"complete record preparation: HTTP {item['status']}")
        for row in item["response"].get("data", item["response"])["results"]:
            found[coordinate(schema, row["key"])] = row
    return found


def conditioned(node, requests):
    current = read_records(node, [req for req in requests if req["mutation_type"] != "create"])
    for request in requests:
        if request["mutation_type"] == "create":
            request["expected"] = request["expected"] or {"type": "absent"}
            continue
        row = current.get(coordinate(request["schema"], request["key_value"]))
        if request["expected"] is None:
            if row is None:
                raise RuntimeError("complete record target is absent")
            request["expected"] = {"type": "revision", "revision": row["revision"]}
        if request["mutation_type"] in ("delete", "purge") and row is not None:
            request["alternates"] = row["alternates"]
    return requests


def append_step_summary(text):
    path = os.environ.get("GITHUB_STEP_SUMMARY")
    if path:
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(text)


def summary_line(line, *, echo=True):
    if echo:
        print(line, flush=True)
    append_step_summary(line + "\n")


def file_sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(CHUNK_BYTES), b""):
            digest.update(chunk)
    return digest.hexdigest()


def save_json(path, value):
    Path(path).write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def write_private(path, payload):
    descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        stream.write(payload)
        stream.flush()
        os.fsync(stream.fileno())


def clean_local_cargo_packages(
    target,
    *,
    profile,
    source=None,
    environment=None,
    exact_target=None,
    verify_binaries=(),
):
    """Remove local workspace and vendored path-package outputs from a CI Cargo target."""
    if os.environ.get("GITHUB_ACTIONS") != "true":
        raise ValueError("CI Cargo dependency preparation runs only in CI")
    target = Path(target)
    runner_temp = Path(os.environ["RUNNER_TEMP"]).resolve()
    if exact_target is not None:
        if target.is_symlink() or target.resolve() != Path(exact_target).resolve():
            raise ValueError("CodeQL Cargo output must use its isolated runner directory")
    elif target.is_symlink() or target.resolve().parent != runner_temp:
        raise ValueError("The dependency target must stay in this runner's temporary directory")
    if not target.exists():
        return {"target_preexisted": False, "cleaned_path_packages": [], "clean_seconds": 0}
    started = time.monotonic()
    metadata = json.loads(
        subprocess.check_output(
            ["cargo", "metadata", "--locked", "--format-version", "1"],
            cwd=source,
            env=environment,
            text=True,
            timeout=120,
        )
    )
    packages = [pkg for pkg in metadata["packages"] if pkg["source"] is None]
    if not any(pkg["name"] == "lastdb_node" for pkg in packages):
        raise ValueError("The dependency target has no identified LastDB package to rebuild")
    # Include vendored path dependencies, even when they are not workspace members.
    # cargo clean ignores a package-id url and version. The package name is the clean key.
    command = ["cargo", "clean", "--profile", profile, "--locked"]
    seen = set()
    for pkg in packages:
        name = pkg["name"]
        if name not in seen:
            seen.add(name)
            command.extend(("--package", name))
    subprocess.run(command, cwd=source, env=environment, check=True, timeout=120)
    for name in verify_binaries:
        binary = target / profile / name
        if binary.exists() or binary.is_symlink():
            raise ValueError("A previous workspace executable survived dependency preparation")
    return {
        "target_preexisted": True,
        "cleaned_path_packages": sorted(pkg["name"] for pkg in packages),
        "clean_seconds": round(time.monotonic() - started, 3),
    }


def _main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("clean-local-packages", help="Clean local Cargo package outputs for CodeQL")
    parser.parse_args()
    target = Path(os.environ["CARGO_TARGET_DIR"])
    exact = Path(os.environ["RUNNER_TEMP"]).resolve() / "codeql-rust-target"
    result = clean_local_cargo_packages(target, profile="dev", exact_target=exact)
    if not result["target_preexisted"]:
        print("No previous CodeQL Cargo target; extraction prepares fresh dependencies")
    else:
        print(f"Removed outputs for {len(set(result['cleaned_path_packages']))} local packages; external dependencies remain")


__all__ = [
    "CREDENTIALS_FILE", "FORMAT", "HOME_FORMAT", "NODE_MARKER", "NoRedirect",
    "UnixConnection", "append_step_summary", "classify_socket_refusal",
    "clean_local_cargo_packages", "conditioned", "create", "file_sha256", "key",
    "marker_format", "mutation", "poll_until", "read_shutdown_receipt",
    "replace_row", "save_json", "send_unix_request", "source_constant",
    "stop_process", "summary_line", "verify_shutdown_receipt", "write_private",
]

if __name__ == "__main__":
    _main()
