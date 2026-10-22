#!/usr/bin/env python3
"""Capture concurrent product use on one fresh, owned home; preserve compact receipts."""
import argparse
import concurrent.futures
import hashlib
import http.client
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import threading
import time
from urllib.parse import urlencode

SUPPORT = Path(__file__).resolve().parents[1] / "snapshot-commits-20261022" / "capture.py"
spec = importlib.util.spec_from_file_location("snapshot_capture", SUPPORT)
base = importlib.util.module_from_spec(spec)
spec.loader.exec_module(base)


class ProductRun(base.Capture):
    def __init__(self, args):
        super().__init__(args)
        self.records = args.records
        self.receipt_lock = threading.Lock()
        self.timings = []
        self.compactions = []
        self.long_keys = []
        base.save(self.output / "recipe.json", {
            "records": self.records, "batch": 64, "body_bytes": 1024,
            "writer_threads": 2, "rounds_per_writer": 12,
            "source_revision": args.source_revision,
            "driver_sha256": base.file_hash(Path(__file__)),
            "scope": "Actual local node; generated data; no primary or cloud access",
        })

    def remember(self, operation, **values):
        lock = getattr(self, "receipt_lock", None)
        if lock is None:
            return super().remember(operation, **values)
        with lock:
            super().remember(operation, **values)

    def call(self, path, value=None, cost=False):
        headers = {"Content-Type": "application/json", "X-LastDB-Client": "atom-lifecycle-product"}
        if cost:
            headers["x-lastdb-read-receipt"] = "true"
        connection = base.UnixConnection(self.socket)
        connection.timeout = 180
        body = None if value is None else json.dumps(value, separators=(",", ":"))
        started = time.perf_counter_ns()
        try:
            connection.request("GET" if value is None else "POST", path, body, headers)
            response = connection.getresponse()
            raw = response.read()
            elapsed = time.perf_counter_ns() - started
            result = json.loads(raw)
            payload = result.get("data", result)
            detail = payload if path in ("/api/db/compact", "/api/db/flush", "/api/db/native-status") else None
            self.remember("request", path=path, http_status=response.status,
                          request_sha256=hashlib.sha256((body or path).encode()).hexdigest(),
                          response_sha256=hashlib.sha256(raw).hexdigest(),
                          response_bytes=len(raw), elapsed_nanos=elapsed, detail=detail)
            if path == "/api/mutations/batch":
                self.timings.append(elapsed / 1e6)
            if response.status != 200:
                raise RuntimeError(f"{path}: HTTP {response.status}: {raw[:1000]!r}")
            return payload
        finally:
            connection.close()

    def start(self):
        environment = {name: value for name, value in os.environ.items()
                       if not name.startswith(("LASTDB", "FOLD", "SENTRY"))}
        environment.update(LASTDB_ISOLATED_COPY="1", LASTDB_SUPPRESS_RECOVERY_PHRASE="1",
                           LASTDB_HOME=str(self.home), FOLDDB_HOME=str(self.home),
                           LASTDB_AUTO_MAINTENANCE="0", RUST_MIN_STACK="16777216")
        self.log = (self.output / "node.log").open("a")
        self.process = subprocess.Popen([str(self.binary), "--data-dir", str(self.home),
            "--socket-path", str(self.socket), "--full-socket-path", str(self.full_socket)],
            env=environment, stdin=subprocess.DEVNULL, stdout=self.log, stderr=self.log)
        self.remember("owned-node-start", pid=self.process.pid, home=str(self.home))
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise RuntimeError("The owned node exited before health")
            if self.socket.exists():
                try:
                    health = self.call("/health")
                    if health.get("ok") or health.get("status") == "ok":
                        return
                except (OSError, http.client.HTTPException):
                    pass
            time.sleep(0.05)
        raise RuntimeError("The owned node missed its health deadline")

    def verify(self, phase):
        actual = {}
        for first in range(0, self.records, 64):
            result = self.call("/api/query", {"schema_name": base.SCHEMA, "fields": base.FIELDS,
                "filter": {"HashRangeKeys": [[base.HASH, base.key(number)]
                    for number in range(first, min(first + 64, self.records))]}, "limit": 64})
            actual.update(self.rows(result))
        for key in self.long_keys:
            result = self.call("/api/query", {"schema_name": base.SCHEMA, "fields": base.FIELDS,
                "filter": {"HashRangeKey": {"hash": base.HASH, "range": key}}, "limit": 1})
            actual.update(self.rows(result))
        if actual != self.expected:
            raise RuntimeError(f"{phase}: exact keys differ from accepted writes")
        cursor, listed = None, set()
        for _ in range(self.records + 1):
            query = {"schema": base.SCHEMA, "hash": base.HASH, "limit": 1000}
            if cursor:
                query["cursor"] = cursor
            result = self.call("/api/list?" + urlencode(query))["list"]
            keys = result.get("keys", result.get("records", []))
            current = {item["range"] for item in keys}
            if current.intersection(listed):
                raise RuntimeError("The list continuation repeated a key")
            listed.update(current)
            if not result.get("has_more"):
                break
            next_cursor = result.get("next_cursor")
            if not next_cursor or next_cursor == cursor:
                raise RuntimeError("The list continuation did not advance")
            cursor = next_cursor
        else:
            raise RuntimeError("The list exceeded its finite page bound")
        if listed != set(self.expected):
            raise RuntimeError(f"{phase}: list membership differs")
        digest = hashlib.sha256(json.dumps(actual, sort_keys=True).encode()).hexdigest()
        self.remember("known-key-readback", phase=phase, live_records=len(actual), sha256=digest)

    def compact(self):
        report = self.call("/api/db/compact", {"dry_run": False, "grace_ms": 0})
        self.compactions.append(report["compact"]["native_disk"])
        return report

    def write_long(self, number, revision):
        key = f"long-{number}-" + "x" * 1536
        fields = base.fields(number, revision)
        fields["id"] = key
        operation = "update" if key in self.expected else "create"
        self.call("/api/mutations/batch", {"mutations": [{"type": "mutation", "schema": base.SCHEMA,
            "mutation_type": operation, "durability": "durable",
            "key_value": {"hash": base.HASH, "range": key}, "fields_and_values": fields}]})
        self.expected[key] = fields
        if key not in self.long_keys:
            self.long_keys.append(key)

    def interrupt_copy(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            request = pool.submit(self.compact)
            observed = None
            for _ in range(500):
                state = self.call("/api/db/native-status")["native_disk"]["parts"]
                if state.get("compaction_phase") == "copy":
                    observed = state
                    self.stop(force=True)
                    break
                if request.done():
                    break
                time.sleep(0.02)
            try:
                request.result()
            except (OSError, http.client.HTTPException):
                if observed is None:
                    raise
            self.remember("interrupted-compaction", observed_phase=observed,
                          signal="SIGKILL" if observed else None)
        if observed is None:
            raise RuntimeError("The copy phase ended before an interruption receipt")
        self.start()
        self.verify("interrupted-copy-reopen")

    def writer(self, offset):
        for revision in range(12):
            self.mutate("update", range(offset, offset + 16), 100 + revision, durability="durable")

    def run(self):
        self.start()
        self.install()
        for first in range(0, self.records, 64):
            self.mutate("create", range(first, min(first + 64, self.records)), 0, durability="durable")
        self.write_long(0, 0)
        self.flush("seed")
        self.verify("seed")
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            writers = [pool.submit(self.writer, offset) for offset in (0, 32)]
            self.compact()
            for writer in writers:
                writer.result()
        self.flush("concurrent-copy")
        self.verify("concurrent-copy")
        for revision in range(3):
            self.mutate("update", range(64, 80), 200 + revision, durability="durable")
            self.write_long(1, 200 + revision)
            self.compact()
            self.verify(f"recent-tier-{revision}")
        self.mutate("delete", range(80, 88), 0, durability="durable")
        self.compact()
        self.verify("tombstones")
        self.stop()
        self.start()
        self.verify("graceful-reopen")
        self.mutate("update", [self.records - 1], 999, durability="durable")
        self.stop(force=True)
        self.start()
        self.verify("forced-reopen")
        self.interrupt_copy()
        self.compact()
        self.verify("compacted-after-forced-reopen")
        self.flush("final")
        base.save(self.output / "compactions.json", self.compactions)
        if not any(report.get("catch_up_records", 0) > 0 for report in self.compactions):
            raise RuntimeError("No compaction receipt includes a concurrent changed key")
        if not any(report.get("tier_boundary", 0) > 0 for report in self.compactions):
            raise RuntimeError("No compaction receipt includes a recent tier")
        ordered = sorted(self.timings)
        base.save(self.output / "latency.json", {"all_mutation_calls": len(ordered),
            "p50_ms": ordered[len(ordered) // 2], "max_ms": max(ordered),
            "scope": "Mixed generated write phases on a shared host; no paired speed claim"})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scratch", type=Path, required=True)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--records", type=int, default=4096)
    args = parser.parse_args()
    if not 128 <= args.records <= 100000:
        parser.error("records must be between 128 and 100000")
    capture, complete, error = ProductRun(args), False, None
    try:
        capture.run()
        complete = True
    except Exception as failure:
        error = f"{type(failure).__name__}: {failure}"
        capture.remember("failure", error=error)
    finally:
        capture.stop()
        base.save(capture.output / "summary.json", {"complete": complete, "error": error,
            "scratch": str(capture.root), "home": str(capture.home),
            "all_owned_processes_stopped": capture.process is None,
            "expected_live_records": len(capture.expected), "data_retained_for_review": True})
    print(json.dumps({"complete": complete, "error": error, "output": str(capture.output)}))
    return 0 if complete else 1


if __name__ == "__main__":
    raise SystemExit(main())
