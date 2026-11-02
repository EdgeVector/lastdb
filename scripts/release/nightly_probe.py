#!/usr/bin/env python3
"""Run the packaged CLI and daemon on one owned, generated, offline home."""

import argparse
import http.client
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tarfile
import tempfile
import time

from nightly_package import MAX_BINARY_BYTES, TARGETS, digest, validate_architecture, validate_pair, write_json


SCHEMA, HASH = "NightlyReleaseRecords", "nightly"
FIELDS = ["bucket", "id", "body"]
# Every new home is format 8 (the record model).
MARKER = "format=8\nhome=disk-run\ncas=disk-cas\n"


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=30)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


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
        # No developer credentials, service-home config, or host profile enters the node.
        # The setup route below is local. Unexpected catalog requests cannot reach a service.
        self.environment = {"PATH": os.environ.get("PATH", "/usr/bin:/bin"), "LANG": "C",
                            "HOME": str(self.root / "os-home"), "TMPDIR": str(self.root / "tmp"),
                            "LASTDB_HOME": str(self.home), "FOLDDB_HOME": str(self.home),
                            "LASTDB_ISOLATED_COPY": "1", "LASTDB_SUPPRESS_RECOVERY_PHRASE": "1",
                            "ENVIRONMENT": "dev", "EXEMEM_ENV": "dev", "RUST_MIN_STACK": "16777216",
                            "FOLD_SCHEMA_SERVICE_URL": "http://127.0.0.1:9",
                            "EXEMEM_API_URL": "http://127.0.0.1:9"}

    def call(self, path, body=None):
        connection = UnixConnection(self.socket)
        try:
            content = None if body is None else json.dumps(body, separators=(",", ":"))
            connection.request("GET" if body is None else "POST", path, content,
                               {"Content-Type": "application/json", "X-LastDB-Client": "nightly-release"})
            response = connection.getresponse()
            raw = response.read(1024 * 1024 + 1)
            if response.status != 200 or len(raw) > 1024 * 1024:
                raise RuntimeError(f"Release operation {path} failed: HTTP {response.status}")
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get("ok") is False:
                raise RuntimeError(f"Release operation {path} returned an error")
            return value.get("data", value)
        finally:
            connection.close()

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned daemon is already active")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen(
            [str(self.binaries / "lastdbd"), "--data-dir", str(self.home),
             "--socket-path", str(self.socket), "--full-socket-path", str(self.home / "data" / "full.sock")],
            env=self.environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
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
            time.sleep(0.05)
        raise RuntimeError("The packaged daemon missed its health deadline")

    def stop(self, crash=False):
        if self.process is None:
            return
        process = self.process
        if crash and process.poll() is not None:
            raise RuntimeError("The owned daemon exited before the deliberate crash")
        if process.poll() is None:
            process.kill() if crash else process.terminate()
        try:
            process.wait(timeout=45)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
            raise RuntimeError("The owned daemon required a forced stop")
        finally:
            if process.poll() is not None:
                self.process = None
                self.log.close()
                self.log = None
        if crash and process.returncode != -signal.SIGKILL:
            raise RuntimeError("The owned daemon did not exit from the deliberate SIGKILL")
        if not crash:
            receipt = self.home / ".shutdown_flush_ready"
            value = json.loads(receipt.read_text()) if receipt.is_file() else {}
            if process.returncode != 0 or value.get("pid") != process.pid or value.get("flush_ok") is not True:
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
                    or self.root.stat().st_ino != self.inode or self.root.parent != Path("/tmp").resolve()
                    or not self.root.name.startswith("ldb-rel-")):
                raise RuntimeError("The generated home identity changed; refuse cleanup")
            shutil.rmtree(self.root)


def unpack(bundle, manifest, directory, expected_version, source):
    if (manifest.get("schema") != "lastdb.mini.bundle.v1" or manifest.get("source_git_oid") != source
            or manifest.get("version") != expected_version or manifest.get("target") not in TARGETS):
        raise ValueError("The package manifest has a different source, version, or target")
    artifact = manifest["artifact"]
    if (not 0 < bundle.stat().st_size <= MAX_BINARY_BYTES
            or artifact["file"] != bundle.name or artifact["sha256"] != digest(bundle)
            or artifact["size"] != bundle.stat().st_size):
        raise ValueError("The package bytes differ from the bundle manifest")
    with tarfile.open(bundle, "r:gz") as archive:
        members = []
        for member in archive:
            if (len(members) == 2 or member.name not in {"lastdb", "lastdbd"}
                    or not member.isfile() or not 0 < member.size <= MAX_BINARY_BYTES):
                raise ValueError("The archive exceeds the bounded executable pair")
            members.append(member)
        if len(members) != 2 or {member.name for member in members} != {"lastdb", "lastdbd"}:
            raise ValueError("The archive must contain only the matched executable pair")
        for member in members:
            if not member.isfile() or not 0 < member.size <= MAX_BINARY_BYTES:
                raise ValueError("A release archive member has an invalid type or size")
            with archive.extractfile(member) as content, (directory / member.name).open("xb") as output:
                shutil.copyfileobj(content, output, length=1024 * 1024)
            (directory / member.name).chmod(0o755)
    if validate_pair(directory, expected_version) != manifest["binaries"]:
        raise ValueError("The unpacked executable digests differ from the manifest")
    validate_architecture(directory, manifest["target"])


def mutation(number, updated, create):
    key = f"record-{number:02d}"
    fields = {"body": f"nightly-record-{number}-{'updated' if updated else 'created'}"}
    if create:
        fields.update(bucket=HASH, id=key)
    return {"type": "mutation", "schema": SCHEMA, "mutation_type": "create" if create else "update",
            "durability": "durable", "key_value": {"hash": HASH, "range": key}, "fields_and_values": fields}


def write(node, numbers, create=False):
    numbers = list(numbers)
    result = node.call("/api/mutations/batch", {"mutations": [mutation(n, not create, create) for n in numbers]})
    identities = result.get("mutation_ids")
    if (result.get("count") != len(numbers) or result.get("durability") != "durable" or result.get("errors")
            or not isinstance(identities, list) or len(identities) != len(numbers)
            or any(not isinstance(value, str) or not value for value in identities)):
        raise RuntimeError("The packaged daemon did not acknowledge the full durable batch")


def read(node, updated, ranged=False):
    condition = ({"HashRangeRange": {"hash": HASH, "start": "record-00", "end": "record-08"}}
                 if ranged else {"HashRangeKeys": [[HASH, f"record-{n:02d}"] for n in range(8)]})
    result = node.call("/api/query", {"schema_name": SCHEMA, "fields": FIELDS, "limit": 8, "filter": condition})
    records = result.get("results", [])
    if result.get("returned_count") != 8 or len(records) != 8:
        raise RuntimeError("The packaged daemon returned an incomplete query")
    found = []
    for record in records:
        key = record.get("key", {})
        number = int(key.get("range", "").removeprefix("record-"))
        expected = {"bucket": HASH, "id": f"record-{number:02d}",
                    "body": f"nightly-record-{number}-{'updated' if number in updated else 'created'}"}
        if key != {"hash": HASH, "range": expected["id"]} or record.get("fields") != expected:
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
    marker = node.home / "data" / "laststore-pack-node-v1"
    if marker.read_text() != MARKER:
        raise RuntimeError("The packaged daemon did not initialize a format 8 home")
    node.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
        "source": "system_seed", "name": SCHEMA, "descriptive_name": SCHEMA, "schema_type": "HashRange",
        "key": {"hash_field": "bucket", "range_field": "id"}, "fields": FIELDS,
        "field_types": {"bucket": "String", "id": "String"},
        "field_descriptions": {field: f"generated nightly release {field}" for field in FIELDS}}})
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
    write_json(args.output / "probe.json", {
        "schema": "lastdb.nightly.probe.v1", "complete": True, "target": manifest["target"],
        "source_git_oid": args.source, "version": args.version, "bundle_sha256": digest(args.bundle),
        "api_version": version["api_version"], "capabilities": version["capabilities"], "disk_format": 2,
        "min_supported_api_version": version.get("min_supported_api_version"),
        "current_outbox_format_version": version.get("current_outbox_format_version"),
        "records": 8, "daemon_stopped": True, "home_removed": True,
        "checks": {name: True for name in ("keyed_read", "range_read", "update", "reopen",
                                          "cli_status", "crash_recovery")}})
    print("Packaged CLI, durable writes, keyed reads, ranges, reopen, and crash recovery passed")


if __name__ == "__main__":
    main()
