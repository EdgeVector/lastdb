#!/usr/bin/env python3
"""Run the packaged CLI and daemon on one owned, generated, offline home."""

from __future__ import annotations

import argparse
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import tempfile

from nightly_package import (
    MAX_FILE, TARGETS, digest, open_executable_pair, validate_architecture,
    validate_pair, write_json,
)

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import (  # noqa: E402
    NODE_MARKER, poll_until, read_shutdown_receipt,
    send_unix_request, stop_process, verify_shutdown_receipt,
)
import v1_records as v1  # noqa: E402

SCHEMA, HASH = "NightlyReleaseRecords", "nightly"
FIELDS = ["bucket", "id", "body"]


class OwnedNode:
    def __init__(self, directory, output, version):
        self.root = directory.resolve()
        self.uid, self.inode = os.getuid(), self.root.stat().st_ino
        self.home, self.binaries = self.root / "home", self.root / "bin"
        self.socket = self.home / "data" / "folddb.sock"
        self.output, self.version = output, version
        self.process, self.log = None, None
        try:
            for path in (self.home, self.binaries, self.root / "os-home", self.root / "tmp"):
                path.mkdir(mode=0o700)
        except BaseException:
            self.close()
            raise
        self.environment = {
            "PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C",
            "HOME": str(self.root / "os-home"), "TMPDIR": str(self.root / "tmp"),
            "LASTDB_HOME": str(self.home), "LASTDB_ISOLATED_COPY": "1",
            "LASTDB_SUPPRESS_RECOVERY_PHRASE": "1", "ENVIRONMENT": "dev", "EXEMEM_ENV": "dev",
            "RUST_MIN_STACK": "16777216",
            "EXEMEM_API_URL": "http://127.0.0.1:9",
        }

    def call(self, path, body=None):
        status, raw, _, _, _ = send_unix_request(
            self.socket, "GET" if body is None else "POST", path, body,
            {"Content-Type": "application/json", "X-LastDB-Client": "nightly-release"},
            timeout=30, max_bytes=1024 * 1024)
        if status != 200 or len(raw) > 1024 * 1024:
            raise RuntimeError(f"Release operation {path} failed: HTTP {status}: {raw[:600]!r}")
        value = json.loads(raw)
        if not isinstance(value, dict) or value.get("ok") is False:
            raise RuntimeError(f"Release operation {path} returned an error: {raw[:600]!r}")
        return value.get("data", value)

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned daemon is already active")
        self.log = (self.output / "node.log").open("a")
        argv = [str(self.binaries / "lastdbd"), "--data-dir", str(self.home), "--socket-path", str(self.socket)]
        self.process = subprocess.Popen(argv, env=self.environment, stdin=subprocess.DEVNULL,
                                        stdout=self.log, stderr=self.log)

        def check_boot():
            if self.process.poll() is not None:
                raise RuntimeError("The packaged daemon exited before health")
            try:
                if self.socket.exists() and self.call("/health").get("status") == "ok":
                    version = self.call("/api/version")
                    if version.get("build") != self.version:
                        raise RuntimeError("The socket daemon reports a different version")
                    return version
            except (OSError, http.client.HTTPException):
                pass
            return None

        return poll_until(90, check_boot, interval=0.05,
                          timeout_error="The packaged daemon missed its health deadline")

    def stop(self, crash=False):
        if self.process is None:
            return
        process = self.process
        if crash and process.poll() is not None:
            raise RuntimeError("The owned daemon exited before the deliberate crash")
        try:
            forced = stop_process(process, timeout=45, kill_timeout=10, crash=crash)
        finally:
            if process.poll() is not None:
                self.process = None
                self.log.close()
                self.log = None
        if forced:
            raise RuntimeError("The owned daemon required a forced stop")
        if crash and process.returncode != -signal.SIGKILL:
            raise RuntimeError("The owned daemon did not exit from the deliberate SIGKILL")
        if not crash and (process.returncode != 0
                          or not verify_shutdown_receipt(read_shutdown_receipt(self.home), process.pid)):
            raise RuntimeError("The packaged daemon lacks a successful shutdown receipt")

    def close(self):
        try:
            self.stop()
        finally:
            if self.process is not None:
                raise RuntimeError("A live daemon still owns the generated home")
            if self.log is not None:
                self.log.close()
                self.log = None
            if (self.root.is_symlink() or self.root.stat().st_uid != self.uid
                    or self.root.stat().st_ino != self.inode
                    or self.root.parent != Path("/tmp").resolve()
                    or not self.root.name.startswith("ldb-rel-")):
                raise RuntimeError("The generated home identity changed; refuse cleanup")
            shutil.rmtree(self.root)


def unpack(bundle, manifest, directory, expected_version, source):
    if (manifest.get("schema") != "lastdb.mini.bundle.v1" or manifest.get("source_git_oid") != source
            or manifest.get("version") != expected_version or manifest.get("target") not in TARGETS):
        raise ValueError("The package manifest has a different source, version, or target")
    artifact = manifest["artifact"]
    if (not 0 < bundle.stat().st_size <= MAX_FILE or artifact["file"] != bundle.name
            or artifact["sha256"] != digest(bundle) or artifact["size"] != bundle.stat().st_size):
        raise ValueError("The package bytes differ from the bundle manifest")
    with open_executable_pair(bundle) as (archive, members):
        for member in members:
            with archive.extractfile(member) as content, (directory / member.name).open("xb") as output:
                shutil.copyfileobj(content, output, length=1024 * 1024)
            (directory / member.name).chmod(0o755)
    if validate_pair(directory, expected_version) != manifest["binaries"]:
        raise ValueError("The unpacked executable digests differ from the manifest")
    validate_architecture(directory, manifest["target"])


def _record(number, updated):
    key = f"record-{number:02d}"
    return key, {"bucket": HASH, "id": key, "body": f"nightly-record-{number}-{'updated' if updated else 'created'}"}


def _revisions_for(node, numbers):
    keys = [f"record-{number:02d}" for number in numbers]
    pages = v1.read_items(node, [v1.point(SCHEMA, {"hash": HASH, "range": key}) for key in keys])
    found = {}
    for page in pages:
        for row in page["records"]:
            if row["key"].get("hash") == HASH and isinstance(row.get("revision"), str) and row["revision"]:
                found[row["key"]["range"]] = row["revision"]
    if any(key not in found for key in keys):
        raise RuntimeError("The packaged daemon did not return the records to replace")
    return found


def write(node, numbers, create=False):
    numbers = list(numbers)
    revisions = {} if create else _revisions_for(node, numbers)
    mutations = []
    for number in numbers:
        key, fields = _record(number, not create)
        address = {"hash": HASH, "range": key}
        if create:
            mutations.append(v1.create_item(SCHEMA, address, fields))
        else:
            mutations.append(v1.replace_item(SCHEMA, address, fields, revisions[key]))
    v1.write_items(node, mutations)


def read(node, updated, ranged=False):
    if ranged:
        records = v1.read_items(node, [v1.range_read(
            SCHEMA, {"hash": HASH, "start": "record-00", "end": "record-08"}, limit=8)])[0]["records"]
    else:
        records = v1.flat_records(v1.read_items(
            node, [v1.point(SCHEMA, {"hash": HASH, "range": f"record-{n:02d}"}) for n in range(8)]))
    if len(records) != 8:
        raise RuntimeError("The packaged daemon returned an incomplete query")
    found = []
    for item in records:
        key = item.get("key", {})
        number = int(key.get("range", "").removeprefix("record-"))
        _, expected = _record(number, number in updated)
        if key != {"hash": HASH, "range": expected["id"]} or item.get("record") != expected:
            raise RuntimeError("The packaged daemon returned different keys or values")
        found.append(number)
    if sorted(found) != list(range(8)) or (ranged and found != list(range(8))):
        raise RuntimeError("The packaged daemon returned duplicate keys or a different range order")


def operations(node):
    version = node.start()
    status = subprocess.run([str(node.binaries / "lastdb"), "--data-dir", str(node.home), "status", "--json"],
                            env=node.environment, capture_output=True, text=True, check=True, timeout=35)
    if not isinstance(json.loads(status.stdout), dict):
        raise RuntimeError("The packaged CLI returned an invalid status response")
    if (node.home / "data" / "laststore-pack-node-v1").read_bytes() != NODE_MARKER:
        raise RuntimeError("The packaged daemon did not initialize a home in the current format")
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "HashRange",
        "record_keys": {"primary": {"kind": "hash_range", "range_type": "String"}, "alternates": {}},
        "fields": FIELDS, "field_types": {"bucket": "String", "id": "String"},
        "field_descriptions": {field: f"generated nightly release {field}" for field in FIELDS},
    }})
    write(node, range(8), create=True)
    read(node, set())
    read(node, set(), ranged=True)
    write(node, range(4))
    read(node, set(range(4)))
    node.stop()
    node.start()
    read(node, set(range(4)), ranged=True)
    write(node, range(4, 8))
    node.stop(crash=True)
    node.start()
    read(node, set(range(8)))
    read(node, set(range(8)), ranged=True)
    return version


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("bundle", "manifest", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--source", required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / "probe.json").exists() or args.manifest.stat().st_size > 1024 * 1024:
        raise ValueError("Use a fresh proof directory and a bounded bundle manifest")
    manifest = json.loads(args.manifest.read_text())
    node = OwnedNode(Path(tempfile.mkdtemp(prefix="ldb-rel-", dir="/tmp")), args.output, args.version)
    try:
        unpack(args.bundle, manifest, node.binaries, args.version, args.source)
        version = operations(node)
    finally:
        node.close()
    if type(version.get("api_version")) is not int or not isinstance(version.get("capabilities"), dict):
        raise ValueError("The packaged daemon lacks its compatibility contract")
    checks = ("keyed_read", "range_read", "update", "reopen", "cli_status", "crash_recovery")
    write_json(args.output / "probe.json", {
        "schema": "lastdb.nightly.probe.v1", "complete": True, "target": manifest["target"],
        "source_git_oid": args.source, "version": args.version, "bundle_sha256": digest(args.bundle),
        "api_version": version["api_version"], "capabilities": version["capabilities"],
        "min_supported_api_version": version.get("min_supported_api_version"),
        "current_outbox_format_version": version.get("current_outbox_format_version"),
        "records": 8, "daemon_stopped": True, "home_removed": True,
        "checks": {name: True for name in checks},
    })
    print("Packaged CLI, durable writes, keyed reads, ranges, reopen, and crash recovery passed")


if __name__ == "__main__":
    main()
