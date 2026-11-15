"""Owned generated homes and actual HTTP requests over the daemon socket."""

import http.client
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from disk_usage import usage  # noqa: E402
from harness.ci import file_sha256 as file_hash, save_json as save  # noqa: E402,F401
from harness.daemon import (  # noqa: E402,F401
    marker_format,
    poll_until,
    read_shutdown_receipt,
    stop_process,
    verify_shutdown_receipt,
)
from harness.uds import UnixConnection, send_unix_request  # noqa: E402,F401
from record_protocol import RecordProtocol  # noqa: E402


def home_format_line(output, manifest):
    """One summary line with the home format of each label, from home-format.json of each run."""
    found = {"baseline": set(), "candidate": set()}
    for run in manifest["runs"]:
        path = output / run["directory"] / "home-format.json"
        found[run["label"]].add(str(json.loads(path.read_text())["format"]) if path.exists() else "unreported")
    shown = {label: ", ".join(sorted(values)) or "unreported" for label, values in found.items()}
    return f"Home format: base **{shown['baseline']}**, candidate **{shown['candidate']}**."


class Node:
    def __init__(self, binary, revision, output, native_timing=False, workers=0, environment=None,
                 start_seconds=120, arguments=(), temporary_directory=None, record_protocol=False):
        self.binary, self.revision, self.output = binary, revision, output
        self.api_version = 0
        self.record_protocol = RecordProtocol() if record_protocol else None
        self.native_timing = native_timing
        self.workers = workers
        self.environment = environment or {}
        self.arguments = tuple(arguments)
        self.start_seconds = start_seconds
        self.root = Path(tempfile.mkdtemp(prefix="ldb-bench-", dir=temporary_directory or "/tmp")).resolve()
        self.home, self.socket = self.root / "home", self.root / "q.sock"
        self.home.mkdir(mode=0o700)
        self.owner = {
            "pid": os.getpid(),
            "uid": os.getuid(),
            "root": str(self.root),
            "inode": self.root.stat().st_ino,
            "generated_data_only": True,
        }
        save(self.root / "owner.json", self.owner)
        save(output / "ownership.json", self.owner)
        self.process, self.log = None, None

    def call(self, path, body=None, receipt=False, request_id=None, transport=None,
             complete_records=False, timeout=60):
        if self.record_protocol is not None:
            return self.record_protocol.call(
                self._exchange,
                path,
                body,
                receipt=receipt,
                request_id=request_id,
                transport=transport,
                complete_records=complete_records,
                timeout=timeout,
            )
        return self._exchange(path, body, receipt=receipt, request_id=request_id, transport=transport, timeout=timeout)

    def _exchange(self, path, body=None, receipt=False, request_id=None, transport=None, timeout=60):
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-benchmark"}
        if request_id is not None:
            headers["X-LastDB-Request-Id"] = request_id
        if receipt:
            headers["x-lastdb-read-receipt"] = "true"
        status, raw, resp_headers, elapsed, req_bytes = send_unix_request(
            self.socket,
            "GET" if body is None else "POST",
            path,
            body,
            headers,
            timeout=timeout,
        )
        if transport is not None:
            transport.update(
                request_body_bytes=req_bytes,
                response_body_bytes=len(raw),
                rpc_count=1,
                response_status=status,
                latency_ms=elapsed,
            )
        cost = resp_headers.get("x-lastdb-read-receipt")
        if status != 200:
            error = RuntimeError(f"{path}: HTTP {status}: {raw[:600]!r}")
            error.status = status
            raise error
        try:
            result = json.loads(raw)
        except json.JSONDecodeError as error:
            raise RuntimeError(f"{path}: HTTP {status} returned non-JSON: {raw[:600]!r}") from error
        if result.get("ok") is False:
            raise RuntimeError(f"{path}: HTTP {status}: {raw[:600]!r}")
        return result.get("data", result), elapsed, json.loads(cost) if cost else None

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned daemon is already active")
        environment = {
            name: value
            for name, value in os.environ.items()
            if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))
        }
        environment.update(
            LASTDB_ISOLATED_COPY="1",
            LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
            LASTDB_HOME=str(self.home),
            RUST_MIN_STACK="16777216",
        )
        environment.update(self.environment)
        if self.native_timing:
            environment["LASTDB_SNAPSHOT_TIMING"] = "1"
        if self.workers:
            environment["LASTDB_UDS_WORKERS"] = str(self.workers)
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen(
            [str(self.binary), "--data-dir", str(self.home), "--socket-path", str(self.socket), *self.arguments],
            env=environment,
            stdin=subprocess.DEVNULL,
            stdout=self.log,
            stderr=self.log,
        )

        def check_boot():
            if self.process.poll() is not None:
                raise RuntimeError("The owned daemon exited before health; inspect node.log")
            if not self.socket.exists():
                return False
            try:
                health, _, _ = self.call("/health")
                if health.get("ok") is not True and health.get("status") != "ok":
                    return False
                version, _, _ = self.call("/api/version")
            except (OSError, http.client.HTTPException):
                return False
            self.api_version = version["api_version"]
            save(self.output / "version.json", version)
            if f"bench-{self.revision}" not in json.dumps(version):
                raise RuntimeError("The actual daemon version differs from the selected revision")
            if self.workers:
                status, _, _ = self.call("/api/status")
                observed = status["status"]["uds"]["workers"]
                save(self.output / "worker-config.json", {"requested": self.workers, "observed": observed})
                if observed != self.workers:
                    raise RuntimeError("The daemon worker count differs from the selected count")
            return True

        poll_until(self.start_seconds, check_boot, interval=0.05, timeout_error="The owned daemon missed its health deadline")

    def stop(self):
        if self.process is None:
            return
        process = self.process
        forced = stop_process(process, timeout=45, kill_timeout=5)
        self.process = None
        if self.record_protocol is not None:
            save(self.output / "client-protocol.json", {"api_version": self.api_version, **self.record_protocol.status()})
        self.log.close()
        self.log = None
        receipt = read_shutdown_receipt(self.home)
        save(self.output / "shutdown.json", {"exit_code": process.returncode, "forced": forced, "receipt": receipt})
        if forced or process.returncode != 0 or not verify_shutdown_receipt(receipt, process.pid):
            raise RuntimeError("The owned daemon lacks a successful shutdown receipt")

    def close(self):
        try:
            self.stop()
        finally:
            if self.process is None:
                if (
                    self.root.is_symlink()
                    or self.root.stat().st_uid != os.getuid()
                    or self.root.stat().st_ino != self.owner["inode"]
                    or json.loads((self.root / "owner.json").read_text()) != self.owner
                ):
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

    def marker(self):
        path = self.home / "data" / "laststore-pack-node-v1"
        return path.read_text() if path.exists() else None

    def home_format(self):
        return marker_format(self.marker())

    def disk_bytes(self):
        return self.disk_usage()["logical_bytes"]

    def disk_usage(self):
        return self.disk_breakdown()["whole_home"]

    def disk_breakdown(self):
        return usage(self.home)
