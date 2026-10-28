"""A finite generated home and its owned daemon, used only on GitHub Actions."""

import gzip
import hashlib
import http.client
import json
import os
from pathlib import Path
import signal
import socket
import stat
import subprocess
import tempfile
import time

from process_counters import read_counters


def save(path, value):
    path.write_text(json.dumps(value, sort_keys=True, indent=2) + "\n")


def file_hash(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 ** 2), b""):
            value.update(chunk)
    return value.hexdigest()


class Connection(http.client.HTTPConnection):
    def __init__(self, path, timeout):
        super().__init__("localhost", timeout=timeout)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX)
        self.sock.settimeout(self.timeout)
        self.sock.connect(str(self.path))


class Node:
    def __init__(self, binary, revision, mode, output):
        self.binary, self.revision, self.mode, self.output = binary, revision, mode, output
        self.root = Path(tempfile.mkdtemp(prefix="range-", dir=os.environ["RUNNER_TEMP"])).resolve()
        self.home, self.socket = self.root / "home", self.root / "q.sock"
        self.home.mkdir(mode=0o700)
        info = self.root.stat()
        self.owner = {"root": str(self.root), "uid": os.getuid(), "device": info.st_dev,
                      "inode": info.st_ino, "generated_data_only": True}
        save(self.root / "owner.json", self.owner)
        save(output / "ownership.json", self.owner)
        self.process, self.log, self.sequence = None, None, 0
        self.capture_process = False
        self.deadline = time.monotonic() + 1200
        self.raw_bytes = 0
        self.raw = gzip.open(output / "requests.jsonl.gz", "xb")
        self.settings = {"LASTDB_AUTO_MAINTENANCE": "0", "RUST_LOG": "warn"}

    def remember(self, value):
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode() + b"\n"
        self.raw_bytes += len(raw)
        if len(raw) > 8 * 1024 ** 2 or self.raw_bytes > 1024 ** 3 or self.sequence > 10000:
            raise RuntimeError("The generated operation exceeds its finite receipt bound")
        self.raw.write(raw)
        if self.raw.fileobj.tell() > 512 * 1024 ** 2:
            raise RuntimeError("The compressed generated receipts exceed512MiB")

    def call(self, path, request=None, expected=(200,)):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("The generated operation exceeds twenty minutes")
        body = None if request is None else json.dumps(request, separators=(",", ":")).encode()
        self.sequence += 1
        request_id = "range-" + str(os.getpid()) + "-" + str(self.sequence)
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "range-continuation-ci",
                   "X-LastDB-Request-Id": request_id, "x-lastdb-read-receipt": "true"}
        connection = Connection(self.socket, min(60, remaining))
        process_before = read_counters(self.process.pid) if self.capture_process and path == "/api/query" else None
        started = time.perf_counter_ns()
        try:
            connection.request("GET" if request is None else "POST", path, body, headers)
            response = connection.getresponse()
            raw = response.read(4 * 1024 ** 2 + 1)
            ended = time.perf_counter_ns()
            process_after = read_counters(self.process.pid) if process_before is not None else None
            if len(raw) > 4 * 1024 ** 2:
                raise RuntimeError("A generated response exceeds fourMiB")
            result = json.loads(raw)
            receipt = response.getheader("x-lastdb-read-receipt")
            row = {"request_id": request_id, "path": path, "request": request, "response": result,
                "http_status": response.status, "started_ns": started, "ended_ns": ended,
                "elapsed_ms": (ended - started) / 1e6,
                "request_sha256": hashlib.sha256(body or b"").hexdigest(),
                "response_sha256": hashlib.sha256(raw).hexdigest(), "response_bytes": len(raw),
                "storage_read_receipt": json.loads(receipt) if receipt else None,
                "storage_read_receipt_raw": receipt}
            if process_before is not None:
                row["process_counters"] = {"before": process_before, "after": process_after}
            self.remember(row)
            if response.status not in expected:
                raise RuntimeError(path + " returned HTTP" + str(response.status) + "; inspect its raw receipt")
            return result.get("data", result), row
        finally:
            connection.close()

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned daemon already runs")
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))
                       and name not in ("GH_TOKEN", "GITHUB_TOKEN")}
        environment.update(self.settings, LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
            LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home), RUST_MIN_STACK="16777216")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.socket), "--full-socket-path", str(self.root / "f.sock")],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember({"operation": "start", "pid": self.process.pid, "settings": self.settings})
        deadline = min(self.deadline, time.monotonic() + 120)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned daemon exited before health")
            if self.socket.exists():
                try:
                    health, _ = self.call("/health")
                    if health.get("ok") is True or health.get("status") == "ok":
                        version, _ = self.call("/api/version")
                        if "range-" + self.revision not in json.dumps(version):
                            raise RuntimeError("The actual daemon version differs from its exact source")
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
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=45)
            except subprocess.TimeoutExpired:
                forced = True
                process.kill()
                process.wait(timeout=10)
        self.process = None
        self.log.close()
        receipt_path = self.home / ".shutdown_flush_ready"
        receipt = json.loads(receipt_path.read_text()) if receipt_path.exists() else None
        self.remember({"operation": "stop", "pid": process.pid, "exit_code": process.returncode,
                       "forced": forced, "shutdown_receipt": receipt})
        if (forced or process.returncode != 0 or not receipt
                or receipt.get("version") != 1 or receipt.get("pid") != process.pid
                or receipt.get("start_ts", 0) <= 0 or receipt.get("flush_ok") is not True):
            raise RuntimeError("The owned daemon lacks a successful shutdown receipt")

    def status(self, label):
        result, _ = self.call("/api/db/native-status")
        if result["native_disk"].get("writer_failed") is not False:
            raise RuntimeError("The generated native writer reports failure")
        memory = {}
        for line in Path(f"/proc/{self.process.pid}/status").read_text().splitlines():
            if line.startswith(("VmRSS:", "VmHWM:")):
                memory[line.split(":")[0]] = int(line.split()[1]) * 1024
        row = {"operation": "status", "label": label, "pid": self.process.pid,
               "native": result, "process_memory_bytes": memory}
        self.remember(row)
        return row

    def close(self):
        error = None
        try:
            self.stop()
        except BaseException as failure:
            error = failure
        finally:
            self.raw.close()
            if self.process is None:
                cleanup(self.root, self.owner, self.output)
        if error:
            raise error


def cleanup(root, owner, output):
    info = root.lstat()
    if (root.is_symlink() or (info.st_dev, info.st_ino, info.st_uid)
            != (owner["device"], owner["inode"], owner["uid"])
            or root.parent != Path(os.environ["RUNNER_TEMP"]).resolve()
            or stat.S_IMODE(info.st_mode) != 0o700
            or json.loads((root / "owner.json").read_text()) != owner):
        raise RuntimeError("The exact owned home identity differs; refuse cleanup")
    for path in (root / "q.sock", root / "f.sock"):
        probe = socket.socket(socket.AF_UNIX)
        probe.settimeout(0.25)
        try:
            probe.connect(str(path))
        except (FileNotFoundError, ConnectionRefusedError):
            pass
        else:
            raise RuntimeError("An exact owned socket still accepts connections; refuse cleanup")
        finally:
            probe.close()
    pending, files, size = [(root, 0)], [], 0
    deadline = time.monotonic() + 30
    while pending:
        parent, depth = pending.pop()
        if depth > 12 or time.monotonic() > deadline:
            raise RuntimeError("The exact owned cleanup exceeds its finite depth/time bound")
        for path in parent.iterdir():
            item = path.lstat()
            if (item.st_uid != owner["uid"] or item.st_dev != owner["device"]
                    or not (stat.S_ISDIR(item.st_mode) or stat.S_ISREG(item.st_mode)
                            or (stat.S_ISSOCK(item.st_mode) and path in (root / "q.sock", root / "f.sock")))):
                raise RuntimeError("The exact owned home contains a linked or foreign object")
            files.append((path, item))
            size += item.st_size if stat.S_ISREG(item.st_mode) else 0
            if len(files) > 50000 or size > 4 * 1024 ** 3:
                raise RuntimeError("The exact owned home exceeds50000 entries or fourGiB")
            if stat.S_ISDIR(item.st_mode):
                pending.append((path, depth + 1))
    for path, item in sorted(files, key=lambda row: len(row[0].parts), reverse=True):
        current = path.lstat()
        if (current.st_dev, current.st_ino, current.st_uid, current.st_mode) != (
                item.st_dev, item.st_ino, item.st_uid, item.st_mode):
            raise RuntimeError("An owned object changed identity before cleanup")
        if stat.S_ISDIR(item.st_mode):
            path.rmdir()
        else:
            path.unlink()
    root.rmdir()
    save(output / "cleanup.json", {"removed": not root.exists(), "root": str(root),
                                  "generated_data_only": True, "entries": len(files), "file_bytes": size})
