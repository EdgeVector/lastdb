"""Owned generated homes and actual HTTP requests over the daemon socket."""

import hashlib
import http.client
import json
import os
from pathlib import Path
import shutil
import socket
import stat
import subprocess
import tempfile
import time


# The node marker of a disk format v2 home. Every new home is v2.
RUN_NODE_MARKER = "format=3\nhome=disk-run\ncas=disk-cas\n"


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=60)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


class Node:
    def __init__(self, binary, revision, output, native_timing=False, workers=0, environment=None,
                 start_seconds=120):
        self.binary, self.revision, self.output = binary, revision, output
        self.native_timing = native_timing
        self.workers = workers
        self.environment = environment or {}
        # The first health reply can wait for the new home creation.
        self.start_seconds = start_seconds
        # This short private path also fits the macOS Unix-socket path limit.
        self.root = Path(tempfile.mkdtemp(prefix="ldb-bench-", dir="/tmp")).resolve()
        self.home, self.socket = self.root / "home", self.root / "q.sock"
        self.home.mkdir(mode=0o700)
        self.owner = {"pid": os.getpid(), "uid": os.getuid(), "root": str(self.root),
                      "inode": self.root.stat().st_ino, "generated_data_only": True}
        save(self.root / "owner.json", self.owner)
        save(output / "ownership.json", self.owner)
        self.process, self.log = None, None

    def call(self, path, body=None, receipt=False, request_id=None):
        if body is not None and not isinstance(body, bytes):
            body = json.dumps(body, separators=(",", ":")).encode()
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-benchmark"}
        if request_id is not None:
            headers["X-LastDB-Request-Id"] = request_id
        if receipt:
            headers["x-lastdb-read-receipt"] = "true"
        connection = UnixConnection(self.socket)
        started = time.perf_counter_ns()
        try:
            connection.request("GET" if body is None else "POST", path, body, headers)
            response = connection.getresponse()
            raw = response.read()
            elapsed = (time.perf_counter_ns() - started) / 1e6
            cost = response.getheader("x-lastdb-read-receipt")
            if response.status != 200:
                raise RuntimeError(f"{path}: HTTP {response.status}: {raw[:600]!r}")
            try:
                result = json.loads(raw)
            except json.JSONDecodeError as error:
                raise RuntimeError(f"{path}: HTTP {response.status} returned non-JSON: {raw[:600]!r}") from error
            if result.get("ok") is False:
                raise RuntimeError(f"{path}: HTTP {response.status}: {raw[:600]!r}")
            return result.get("data", result), elapsed, json.loads(cost) if cost else None
        finally:
            connection.close()

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned daemon is already active")
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           RUST_MIN_STACK="16777216")
        environment.update(self.environment)
        if self.native_timing:
            environment["LASTDB_SNAPSHOT_TIMING"] = "1"
        if self.workers:
            environment["LASTDB_UDS_WORKERS"] = str(self.workers)
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen(
            [str(self.binary), "--data-dir", str(self.home), "--socket-path", str(self.socket),
             "--full-socket-path", str(self.root / "f.sock")],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        deadline = time.monotonic() + self.start_seconds
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned daemon exited before health; inspect node.log")
            if self.socket.exists():
                try:
                    health, _, _ = self.call("/health")
                    if health.get("ok") is True or health.get("status") == "ok":
                        version, _, _ = self.call("/api/version")
                        save(self.output / "version.json", version)
                        if f"bench-{self.revision}" not in json.dumps(version):
                            raise RuntimeError("The actual daemon version differs from the selected revision")
                        if self.workers:
                            status, _, _ = self.call("/api/status")
                            observed = status["status"]["uds"]["workers"]
                            save(self.output / "worker-config.json",
                                 {"requested": self.workers, "observed": observed})
                            if observed != self.workers:
                                raise RuntimeError("The daemon worker count differs from the selected count")
                        return
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(0.05)
        raise RuntimeError("The owned daemon missed its health deadline")

    def stop(self):
        if self.process is None:
            return
        process, forced = self.process, False
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                forced = True
                process.kill()
                process.wait(timeout=5)
        self.process = None
        self.log.close()
        self.log = None
        receipt_path = self.home / ".shutdown_flush_ready"
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        save(self.output / "shutdown.json", {"exit_code": process.returncode, "forced": forced,
                                             "receipt": receipt})
        if (forced or process.returncode != 0 or not receipt
                or receipt.get("pid") != process.pid or receipt.get("flush_ok") is not True):
            raise RuntimeError("The owned daemon lacks a successful shutdown receipt")

    def close(self):
        try:
            self.stop()
        finally:
            if self.process is None:
                if (self.root.is_symlink() or self.root.stat().st_uid != os.getuid()
                        or self.root.stat().st_ino != self.owner["inode"]
                        or json.loads((self.root / "owner.json").read_text()) != self.owner):
                    raise RuntimeError("The owned home identity changed; refuse cleanup")
                shutil.rmtree(self.root)
                save(self.output / "cleanup.json", {"removed": str(self.root), "daemon_stopped": True})

    def memory_bytes(self):
        if self.process is None:
            return None
        status = Path(f"/proc/{self.process.pid}/status")
        if not status.exists():
            return None
        for line in status.read_text().splitlines():
            if line.startswith("VmRSS:"):
                return int(line.split()[1]) * 1024
        return None

    def disk_format(self):
        """2 for a v2 run home, 1 for any other marker; the marker exists after the first start."""
        marker = (self.home / "data" / "laststore-pack-node-v1").read_text()
        return 2 if marker == RUN_NODE_MARKER else 1

    def disk_bytes(self):
        return self.disk_usage()["logical_bytes"]

    def disk_usage(self):
        # Bound the generated-home walk by depth, entry count, and elapsed time.
        stack, entries, logical, allocated = [(self.home, 0)], 0, 0, 0
        deadline = time.monotonic() + 15
        while stack:
            parent, depth = stack.pop()
            if depth > 12 or entries > 1_000_000 or time.monotonic() > deadline:
                raise RuntimeError("The owned-home size check exceeded its bound")
            try:
                children = list(parent.iterdir())
            except FileNotFoundError:
                # Maintenance can remove a queued retired-generation directory.
                continue
            for path in children:
                entries += 1
                if entries > 1_000_000 or time.monotonic() > deadline:
                    raise RuntimeError("The owned-home size check exceeded its bound")
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    # The product maintenance worker can reclaim a file.
                    continue
                if stat.S_ISLNK(info.st_mode):
                    raise RuntimeError("A generated home contains an unexpected symlink")
                if stat.S_ISDIR(info.st_mode):
                    stack.append((path, depth + 1))
                else:
                    if not hasattr(info, "st_blocks"):
                        raise RuntimeError("The generated-home filesystem lacks allocated block counts")
                    logical += info.st_size
                    allocated += info.st_blocks * 512
        return {"logical_bytes": logical, "allocated_bytes": allocated}
