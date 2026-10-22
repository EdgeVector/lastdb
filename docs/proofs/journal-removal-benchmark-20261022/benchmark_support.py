"""Owned product process, buffered receipts, and bounded benchmark checkpoints."""

from contextlib import contextmanager
import hashlib
import http.client
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
from types import SimpleNamespace

sys.dont_write_bytecode = True
PROOFS = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROOFS / "snapshot-commits-20261022"))
sys.path.insert(0, str(PROOFS / "write-overhead-20261021"))
from capture import Capture, UnixConnection, file_hash, save
from io_probe import sample_process
import measure
from measure import inventory, growth, public_inventory, stats, mutations, body_for, SCHEMA


FIELDS = ["bucket", "id", "title", "body"]
SETTINGS = {"LASTDB_AUTO_MAINTENANCE": "0", "LASTDB_RESIDENT_PERSIST_MS": "500",
            "LASTDB_UDS_WORKERS": "4", "RUST_MIN_STACK": "16777216", "RUST_LOG": "warn"}


@contextmanager
def guarded_entries(path, guard):
    with os.scandir(path) as entries:
        def checked():
            for entry in entries:
                guard["entries"] += 1
                if guard["entries"] > 40000 or time.monotonic() > guard["deadline"]:
                    raise RuntimeError("The native inventory exceeds its entry or time bound")
                if entry.is_file(follow_symlinks=False):
                    guard["files"] += 1
                    info = entry.stat(follow_symlinks=False)
                    guard["bytes"] += info.st_size
                    guard["allocated"] += info.st_blocks * 512
                    if (guard["files"] > 20000 or guard["bytes"] > 1024 ** 3
                            or guard["allocated"] > 1024 ** 3):
                        raise RuntimeError("The native home exceeds 20000 files or 1 GiB")
                yield entry
        yield checked()


def bounded_inventory(home):
    # Use the existing metadata-only helper with stricter per-entry bounds.
    # This module has its own imported measure namespace; os itself is unchanged.
    guard = {"entries": 0, "files": 0, "bytes": 0, "allocated": 0,
             "deadline": time.monotonic() + 20}
    original = measure.os
    measure.os = SimpleNamespace(scandir=lambda path: guarded_entries(path, guard))
    try:
        return inventory(home)
    finally:
        measure.os = original


def process_delta(before, after):
    identity = ("pid", "ri_proc_start_abstime", "executable_uuid")
    if any(before[key] != after[key] for key in identity):
        raise RuntimeError("The process identity changed inside a measured phase")
    counters = ("ri_diskio_bytesread", "ri_diskio_byteswritten", "ri_logical_writes",
                "ri_user_time", "ri_system_time")
    return {"cumulative_counter_delta": {key: after[key] - before[key] for key in counters},
            "memory_before_bytes": {key: before[key] for key in
                                    ("ri_resident_size", "ri_phys_footprint")},
            "memory_after_bytes": {key: after[key] for key in
                                   ("ri_resident_size", "ri_phys_footprint")},
            "process_lifetime_max_memory_bytes": after["ri_lifetime_max_phys_footprint"]}


def phase_cost(before, after):
    first, last = before["native"], after["native"]
    return {"growth": growth(before["inventory"], after["inventory"]),
            "inventory_before": public_inventory(before["inventory"]),
            "inventory_after": public_inventory(after["inventory"]),
            "process": process_delta(before["process"], after["process"]),
            "generation_delta": last["generation"] - first["generation"],
            "frontier_delta": last["frontier"] - first["frontier"],
            "accepted_sequence_delta": last["accepted_sequence"] - first["accepted_sequence"],
            "completed_sequence_delta": last["completed_sequence"] - first["completed_sequence"]}


class Benchmark(Capture):
    def __init__(self, args):
        self.buffer, self.receipt_lock = [], threading.Lock()
        self.phase = "initialization"
        self.deadline = time.monotonic() + 300
        super().__init__(args)
        self.expected = {}
        self.checkpoints = []
        paths = [Path(__file__), Path(__file__).with_name("benchmark.py"),
                 PROOFS / "snapshot-commits-20261022" / "capture.py",
                 PROOFS / "write-overhead-20261021" / "measure.py",
                 PROOFS / "write-overhead-20261021" / "io_probe.py"]
        save(self.output / "inputs.json", {"source_revision": args.source_revision,
             "label": args.label, "binary": str(self.binary), "binary_sha256": file_hash(self.binary),
             "helpers_sha256": {str(path): file_hash(path) for path in paths},
             "settings": SETTINGS, "automatic_maintenance": False,
             "records": 512, "schema_fields": FIELDS, "body_bytes": 1024,
             "seed": {"requests": 16, "width": 32, "durability": "durable"},
             "limits": {"run_seconds": 300, "request_seconds": 120, "native_bytes": 1024 ** 3,
                        "native_files": 20000, "inventory_entries": 40000,
                        "inventory_depth": 8, "inventory_seconds": 20},
             "claim_limit": "Controlled auto-maintenance-off comparison; filesystem cache uncontrolled"})

    def remember(self, operation, **value):
        with self.receipt_lock:
            self.buffer.append({"operation": operation, "phase": self.phase,
                                "at_unix_ns": time.time_ns(), **value})

    def save_receipts(self):
        with self.receipt_lock:
            pending, self.buffer = self.buffer, []
        with (self.output / "receipts.jsonl").open("a") as stream:
            stream.writelines(json.dumps(row, sort_keys=True) + "\n" for row in pending)

    def remaining(self):
        remaining = self.deadline - time.monotonic()
        if remaining <= 0:
            raise RuntimeError("The owned benchmark exceeds its five-minute bound")
        return remaining

    def call_with_receipt(self, path, value=None, cost=False):
        with self.receipt_lock:
            self.sequence += 1
            request_id = f"benchmark-{self.identity}-{self.sequence}"
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "journal-removal-benchmark",
                   "X-LastDB-Request-Id": request_id}
        if cost:
            headers["x-lastdb-read-receipt"] = "true"
        body = None if value is None else json.dumps(value, separators=(",", ":")).encode()
        connection = UnixConnection(self.socket)
        connection.timeout = min(120, self.remaining())
        started_unix, started = time.time_ns(), time.monotonic_ns()
        try:
            connection.request("GET" if value is None else "POST", path, body, headers)
            response = connection.getresponse()
            raw = response.read()
            ended, ended_unix = time.monotonic_ns(), time.time_ns()
            result = json.loads(raw)
            receipt = {"request_id": request_id, "path": path, "request": value,
                       "request_bytes": len(body or b""), "response_bytes": len(raw),
                       "http_status": response.status, "response": result,
                       "started_unix_ns": started_unix, "ended_unix_ns": ended_unix,
                       "started_monotonic_ns": started, "ended_monotonic_ns": ended,
                       "elapsed_seconds": (ended - started) / 1e9}
            self.remember("request", **receipt)
            if response.status != 200:
                raise RuntimeError(f"{path}: HTTP {response.status}; inspect the raw receipt")
            return result.get("data", result), receipt
        finally:
            connection.close()

    def call(self, path, value=None, cost=False):
        return self.call_with_receipt(path, value, cost)[0]

    def start(self):
        if self.process is not None:
            raise RuntimeError("The owned node is already active")
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY", "OBS_SENTRY"))}
        environment.update(SETTINGS, LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home))
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.socket), "--full-socket-path", str(self.full_socket)],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember("owned-node-start", pid=self.process.pid, home=str(self.home), settings=SETTINGS)
        deadline = min(self.deadline, time.monotonic() + 120)
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned node exited before health; inspect node.log")
            if self.socket.exists():
                try:
                    health = self.call("/health")
                    if health.get("ok") is True or health.get("status") == "ok":
                        return
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(0.05)
        raise RuntimeError("The owned node missed its health deadline")

    def install(self):
        self.call("/api/schemas/seed-system", {"isolated_copy": True, "schema": {
            "source": "system_seed", "name": SCHEMA, "descriptive_name": "Generated benchmark records",
            "schema_type": "HashRange", "key": {"hash_field": "bucket", "range_field": "id"},
            "fields": FIELDS, "field_descriptions": {
                field: f"generated journal removal benchmark {field}" for field in FIELDS}}})

    def flush(self, phase):
        result, receipt = self.call_with_receipt("/api/db/flush", {})
        native = result.get("native_flush", {})
        if (result.get("durable") is not True or
                native.get("completed_sequence", -1) < native.get("target_sequence", 0)):
            raise RuntimeError("The explicit flush lacks complete durable coverage")
        self.remember("durable-boundary", boundary=phase, receipt=native,
                      elapsed_seconds=receipt["elapsed_seconds"])
        return result, receipt

    def checkpoint(self, label):
        self.flush(label)
        stable, previous = 0, None
        deadline = min(self.deadline, time.monotonic() + 30)
        while stable < 3:
            self.remaining()
            native = self.call("/api/db/native-status")["native_disk"]
            if native.get("writer_failed") is not False:
                raise RuntimeError("The native writer reports failure")
            marker = tuple(native[name] for name in
                           ("generation", "frontier", "accepted_sequence", "completed_sequence"))
            drained = (not native["accepted_batches"] and not native["core_warm_dirty"]
                       and native["accepted_sequence"] == native["completed_sequence"])
            stable = stable + 1 if drained and marker == previous else 0
            previous = marker
            if time.monotonic() > deadline:
                raise RuntimeError("The product queue does not reach a stable durable boundary")
            time.sleep(0.1)
        listing = bounded_inventory(self.home)
        result = {"label": label, "native": native, "inventory": listing,
                  "process": sample_process(self.process.pid), "at_unix_ns": time.time_ns()}
        self.checkpoints.append(result)
        self.remember("checkpoint", label=label, native=native,
                      inventory=public_inventory(listing), process=result["process"])
        return result

    def write(self, numbers, revision, mode="durable", create=False):
        request = mutations(numbers, revision, mode, create)
        result, receipt = self.call_with_receipt("/api/mutations/batch", request)
        if result.get("count") != len(request["mutations"]):
            raise RuntimeError("The mutation reply lacks its complete accepted count")
        if result.get("durability") not in ("queued", "durable"):
            raise RuntimeError("The mutation reply lacks its acknowledgement contract")
        if mode == "durable" and result["durability"] != "durable":
            raise RuntimeError("The durable request returns an incomplete acknowledgement")
        for number in numbers:
            self.expected[f"key-{number:06d}"] = {
                "bucket": "measure", "id": f"key-{number:06d}",
                "title": f"title-{number}-revision-{revision}", "body": body_for(number, revision)}
        return result, receipt

    def readback(self, label):
        request = {"schema_name": SCHEMA, "fields": FIELDS,
                   "filter": {"HashRangeKeys": [["measure", key] for key in sorted(self.expected)]},
                   "limit": 512}
        result = self.call("/api/query", request)
        if self.rows(result) != self.expected or result.get("has_more") is not False:
            raise RuntimeError(f"The complete exact-key read differs after {label}")
        actual, order, cursor = {}, [], None
        for _ in range(16):
            request = {"schema_name": SCHEMA, "fields": FIELDS, "limit": 37,
                       "filter": {"HashRangeRange": {"hash": "measure", "start": "key-000000",
                                                    "end": "key-000512"}}}
            if cursor is not None:
                request["cursor"] = cursor
            result = self.call("/api/query", request)
            current = self.rows(result)
            if len(current) > 37 or set(current).intersection(actual):
                raise RuntimeError("A bounded range page repeats keys or exceeds 37 records")
            actual.update(current)
            order.extend(row["key"]["range"] for row in result["results"])
            if not result.get("has_more"):
                break
            next_cursor = result.get("next_cursor")
            if not next_cursor or next_cursor == cursor:
                raise RuntimeError("The range cursor cannot advance")
            cursor = next_cursor
        else:
            raise RuntimeError("The known-hash range exceeds sixteen pages")
        if actual != self.expected or order != sorted(self.expected) or len(actual) != 512:
            raise RuntimeError(f"The complete range values or order differ after {label}")
        self.remember("known-key-readback", label=label, records=512, range_page_size=37,
                      expected_sha256=hashlib.sha256(json.dumps(self.expected, sort_keys=True).encode()).hexdigest())
