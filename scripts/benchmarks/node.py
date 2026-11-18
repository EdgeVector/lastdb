"""Owned generated homes, bounded disk breakdown walks, and daemon socket RPCs."""

from __future__ import annotations

from collections import Counter
import copy
import http.client
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time

_SCRIPTS = str(Path(__file__).resolve().parents[1])
if _SCRIPTS not in sys.path:
    sys.path.insert(0, _SCRIPTS)

from harness import (  # noqa: E402
    append_step_summary, file_sha256 as file_hash, marker_format,
    poll_until, read_shutdown_receipt, save_json as save, send_unix_request,
    stop_process, verify_shutdown_receipt,
)
from v1_records import benchmark_batch, benchmark_write  # noqa: E402

CATEGORIES = ("database", "other_home")
SIZES = ("logical_bytes", "allocated_bytes")


def declaration(body):
    body = copy.deepcopy(body)
    schema = body["schema"]
    if "record_keys" in schema:
        return body
    primary = schema.pop("key", {}) or {}
    ranged = primary.get("range_field")
    definition = {"kind": "hash_range" if ranged else "hash"}
    if ranged:
        definition["range_type"] = schema.get("field_types", {}).get(ranged, "String")
    schema["record_keys"] = {"primary": definition, "alternates": {}}
    return body


class RecordProtocol:
    """Explicit benchmark-only client for current record operations."""

    def __init__(self):
        self.lock, self.totals = threading.Lock(), Counter()

    def status(self):
        with self.lock:
            return {"client": "benchmark /v1 write adapter", "totals": dict(self.totals),
                    "payload_cache": False, "retry_limit": 8,
                    "mutation_timing_includes_all_client_work": True}

    def call(self, exchange, path, body, *, transport=None, **options):
        started, physical = time.perf_counter_ns(), []

        def send(route, value, **overrides):
            metrics = {}
            settings = {**options, **overrides, "transport": metrics}
            # Distinct IDs retain every physical request in phase correlation.
            request_id = options.get("request_id")
            settings["request_id"] = f"{request_id}/rpc-{len(physical)}" if request_id else None
            try:
                return exchange(route, value, **settings)
            finally:
                physical.append({"path": route, "request_id": settings["request_id"], **metrics})

        try:
            decoded = json.loads(body) if isinstance(body, (bytes, bytearray)) else body
            if path == "/api/schemas/seed-system":
                result = send(path, declaration(decoded))
            elif path == "/v1/write" and benchmark_batch(decoded):
                # Field patches become one /v1/write. Revision reads stay inside this elapsed time.
                def on_conflict():
                    with self.lock:
                        self.totals.update(conflicts=1, retries=1)
                result = benchmark_write(send, decoded["mutations"], on_conflict)
            else:
                return send(path, body)
            return result[0], (time.perf_counter_ns() - started) / 1e6, result[2]
        finally:
            total = {name: sum(item.get(name, 0) for item in physical)
                     for name in ("request_body_bytes", "response_body_bytes")}
            total.update(rpc_count=len(physical), physical_requests=physical,
                         retries=sum(item.get("response_status") == 409 for item in physical))
            if transport is not None:
                transport.update(total)
            with self.lock:
                self.totals.update(logical_calls=1, physical_rpcs=len(physical), **{
                    name: total[name] for name in ("request_body_bytes", "response_body_bytes")})


def validate_breakdown(result):
    if set(result) != {*CATEGORIES, "whole_home"}:
        raise ValueError("The disk breakdown has incomplete categories")
    for category in (*CATEGORIES, "whole_home"):
        if (set(result[category]) != set(SIZES)
                or any(type(result[category][name]) is not int or result[category][name] < 0 for name in SIZES)):
            raise ValueError("The disk breakdown has invalid sizes")
    if any(result["whole_home"][name] != sum(result[category][name] for category in CATEGORIES) for name in SIZES):
        raise ValueError("The disk categories do not add to the whole home")


def usage(home):
    result = {name: dict.fromkeys(SIZES, 0) for name in (*CATEGORIES, "whole_home")}
    stack, entries, deadline = [(home, 0)], 0, time.monotonic() + 15
    while stack:
        parent, depth = stack.pop()
        if depth > 12 or time.monotonic() > deadline:
            raise RuntimeError("The owned-home size check exceeded its bound")
        try:
            for path in parent.iterdir():
                entries += 1
                if entries > 1_000_000 or time.monotonic() > deadline:
                    raise RuntimeError("The owned-home size check exceeded its bound")
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    continue
                if stat.S_ISLNK(info.st_mode):
                    raise RuntimeError("A generated home contains an unexpected symlink")
                if stat.S_ISDIR(info.st_mode):
                    stack.append((path, depth + 1))
                    continue
                if (not (stat.S_ISREG(info.st_mode) or stat.S_ISSOCK(info.st_mode))
                        or not hasattr(info, "st_blocks")):
                    raise RuntimeError("A generated home contains an unsupported disk entry")
                parts = path.relative_to(home).parts
                category = "database" if parts[0] == "data" else "other_home"
                result[category]["logical_bytes"] += info.st_size
                result[category]["allocated_bytes"] += info.st_blocks * 512
        except FileNotFoundError:
            continue
    for name in SIZES:
        result["whole_home"][name] = sum(result[category][name] for category in CATEGORIES)
    validate_breakdown(result)
    return result


def home_format_line(output, manifest):
    """One summary line with the home format of each label, from home-format.json of each run."""
    found = {"baseline": set(), "candidate": set()}
    for run in manifest["runs"]:
        path = output / run["directory"] / "home-format.json"
        found[run["label"]].add(str(json.loads(path.read_text())["format"]) if path.exists() else "unreported")
    shown = {label: ", ".join(sorted(values)) or "unreported" for label, values in found.items()}
    return f"Home format: base **{shown['baseline']}**, candidate **{shown['candidate']}**."


def publish_summary(output, report, filename="summary.md"):
    output.mkdir(parents=True, exist_ok=True)
    (output / filename).write_text(report)
    append_step_summary(report)
    print(report)


class Node:
    def __init__(self, binary, revision, output, native_timing=False, workers=0, environment=None,
                 start_seconds=120, arguments=(), temporary_directory=None, record_protocol=False):
        self.binary, self.revision, self.output = binary, revision, output
        self.api_version = 0
        self.record_protocol = RecordProtocol() if record_protocol else None
        self.native_timing, self.workers = native_timing, workers
        self.environment = environment or {}
        self.arguments, self.start_seconds = tuple(arguments), start_seconds
        self.root = Path(tempfile.mkdtemp(prefix="ldb-bench-", dir=temporary_directory or "/tmp")).resolve()
        self.home, self.socket = self.root / "home", self.root / "q.sock"
        self.home.mkdir(mode=0o700)
        self.owner = {"pid": os.getpid(), "uid": os.getuid(), "root": str(self.root),
                      "inode": self.root.stat().st_ino, "generated_data_only": True}
        save(self.root / "owner.json", self.owner)
        save(output / "ownership.json", self.owner)
        self.process, self.log = None, None

    def call(self, path, body=None, receipt=False, request_id=None, transport=None, timeout=60):
        if self.record_protocol is not None:
            return self.record_protocol.call(
                self._exchange, path, body, receipt=receipt, request_id=request_id,
                transport=transport, timeout=timeout)
        return self._exchange(path, body, receipt=receipt, request_id=request_id, transport=transport, timeout=timeout)

    def _exchange(self, path, body=None, receipt=False, request_id=None, transport=None, timeout=60):
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "github-benchmark"}
        if request_id is not None:
            headers["X-LastDB-Request-Id"] = request_id
        if receipt:
            headers["x-lastdb-read-receipt"] = "true"
        status, raw, resp_headers, elapsed, req_bytes = send_unix_request(
            self.socket, "GET" if body is None else "POST", path, body, headers, timeout=timeout)
        if transport is not None:
            transport.update(request_body_bytes=req_bytes, response_body_bytes=len(raw),
                             rpc_count=1, response_status=status, latency_ms=elapsed)
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

    def daemon_env(self, home=None):
        environment = {k: v for k, v in os.environ.items()
                       if not k.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_", "OTEL_"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           LASTDB_HOME=str(self.home if home is None else home),
                           RUST_MIN_STACK="16777216", **self.environment)
        if self.native_timing:
            environment["LASTDB_SNAPSHOT_TIMING"] = "1"
        if self.workers:
            environment["LASTDB_UDS_WORKERS"] = str(self.workers)
        return environment

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned daemon is already active")
        environment = self.daemon_env()
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen(
            [str(self.binary), "--data-dir", str(self.home), "--socket-path", str(self.socket), *self.arguments],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)

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
                observed = self.call("/api/status")[0]["status"]["uds"]["workers"]
                save(self.output / "worker-config.json", {"requested": self.workers, "observed": observed})
                if observed != self.workers:
                    raise RuntimeError("The daemon worker count differs from the selected count")
            return True

        poll_until(self.start_seconds, check_boot, interval=0.05,
                   timeout_error="The owned daemon missed its health deadline")

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
                if (self.root.is_symlink() or self.root.stat().st_uid != os.getuid()
                        or self.root.stat().st_ino != self.owner["inode"]
                        or json.loads((self.root / "owner.json").read_text()) != self.owner):
                    raise RuntimeError("The owned home identity changed; refuse cleanup")
                shutil.rmtree(self.root)
                save(self.output / "cleanup.json", {"removed": str(self.root), "daemon_stopped": True})

    def memory_bytes(self):
        status = Path(f"/proc/{self.process.pid}/status") if self.process is not None else None
        if status is None or not status.exists():
            return None
        return next((int(line.split()[1]) * 1024 for line in status.read_text().splitlines()
                     if line.startswith("VmRSS:")), None)

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
